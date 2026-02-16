pub mod handlers;
pub mod models;
#[cfg(feature = "python")]
pub mod python;
pub mod security;
pub mod wrapper;

pub use wrapper::StatCanDataFrame;

use crate::models::{
    Cube, CubeListResponse, CubeMetadataResponse, DataPoint, DataResponse, Dimension,
    FullTableResponse, VectorDataResponse,
};
use ::zip::ZipArchive;
use async_trait::async_trait;
use polars::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info};

const BASE_URL: &str = "https://www150.statcan.gc.ca/t1/wds/rest";

#[derive(Error, Debug)]
pub enum StatCanError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("API error: {0}")]
    Api(String),
    #[error("Zip error: {0}")]
    Zip(#[from] ::zip::result::ZipError),
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Table not found")]
    TableNotFound,
}

pub type Result<T> = std::result::Result<T, StatCanError>;

pub(crate) fn pad_coordinate(coord: &str) -> String {
    let c = coord.trim();
    let parts: Vec<&str> = c.split('.').collect();
    let mut padded_string = c.to_string();
    if parts.len() < 10 {
        let needed = 10 - parts.len();
        for _ in 0..needed {
            padded_string.push_str(".0");
        }
    }
    padded_string
}

pub trait StatCanClientTrait: CKANClient + Send + Sync {
    fn get_all_cubes_list_lite(&self) -> impl Future<Output = Result<CubeListResponse>> + Send;
    fn get_cube_metadata(
        &self,
        pid: &str,
    ) -> impl Future<Output = Result<CubeMetadataResponse>> + Send;
    fn find_cubes_by_dimension(
        &self,
        dim_query: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(String, String, String)>>> + Send;
    fn get_data_from_vectors(
        &self,
        vectors: Vec<String>,
        periods: i32,
    ) -> impl Future<Output = Result<DataResponse>> + Send;
    fn get_data_from_coords(
        &self,
        pid: &str,
        coords: Vec<String>,
        periods: i32,
    ) -> impl Future<Output = Result<DataResponse>> + Send;
    fn fetch_fast_snippet(
        &self,
        pid: &str,
    ) -> impl Future<Output = Result<StatCanDataFrame>> + Send;
    fn fetch_full_table(&self, pid: &str) -> impl Future<Output = Result<StatCanDataFrame>> + Send;
}

impl StatCanClientTrait for StatCanDriver {
    async fn get_all_cubes_list_lite(&self) -> Result<CubeListResponse> {
        self.get_all_cubes_list_lite().await
    }
    async fn get_cube_metadata(&self, pid: &str) -> Result<CubeMetadataResponse> {
        self.get_cube_metadata(pid).await
    }
    async fn find_cubes_by_dimension(
        &self,
        dim_query: &str,
        limit: usize,
    ) -> Result<Vec<(String, String, String)>> {
        self.find_cubes_by_dimension(dim_query, limit).await
    }
    async fn get_data_from_vectors(
        &self,
        vectors: Vec<String>,
        periods: i32,
    ) -> Result<DataResponse> {
        self.get_data_from_vectors(vectors, periods).await
    }
    async fn get_data_from_coords(
        &self,
        pid: &str,
        coords: Vec<String>,
        periods: i32,
    ) -> Result<DataResponse> {
        self.get_data_from_coords(pid, coords, periods).await
    }
    async fn fetch_fast_snippet(&self, pid: &str) -> Result<StatCanDataFrame> {
        self.fetch_fast_snippet(pid).await
    }
    async fn fetch_full_table(&self, pid: &str) -> Result<StatCanDataFrame> {
        self.fetch_full_table(pid).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageMetadata {
    pub id: String,
    pub title: String,
    pub notes: Option<String>,
    pub url: Option<String>,
    pub resources: Vec<ResourceMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub id: String,
    pub name: String,
    pub format: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone)]
pub enum DataHandler {
    /// Resource ID and optional download URL
    DatastoreQuery(String, Option<String>),
    BlobDownload(String), // URL
}

#[async_trait]
pub trait CKANClient: Send + Sync {
    async fn ping(&self) -> Result<String>;
    async fn search_packages(&self, query: &str, limit: usize) -> Result<Vec<PackageMetadata>>;
    async fn get_package_metadata(&self, id: &str) -> Result<PackageMetadata>;
    async fn get_resource_handler(&self, resource_id: &str) -> Result<DataHandler>;
    async fn query_datastore(&self, sql: &str) -> Result<Vec<serde_json::Value>>;
    async fn get_resource_schema(&self, resource_id: &str) -> Result<Vec<(String, String)>>;
}

pub type StatCanClient = StatCanDriver;

pub struct StatCanDriver {
    client: Client,
    cubes_cache: Arc<RwLock<Option<Vec<Cube>>>>,
}

impl StatCanDriver {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .gzip(true)
            .build()?;
        Ok(Self {
            client,
            cubes_cache: Arc::new(RwLock::new(None)),
        })
    }

    fn extract_data_points(responses: Vec<VectorDataResponse>) -> Vec<DataPoint> {
        let mut all_points = Vec::new();
        for r in responses {
            if r.status == "SUCCESS" {
                if let Some(obj) = r.object {
                    for vp in obj.vector_data_point {
                        all_points.push(DataPoint {
                            vector_id: obj.vector_id,
                            coordinate: obj.coordinate.clone(),
                            ref_date: vp.ref_per,
                            value: vp.value,
                            decimals: vp.decimals,
                            scalar_factor_code: vp.scalar_factor_code,
                            symbol_code: vp.symbol_code,
                            status_code: vp.status_code,
                            security_level_code: vp.security_level_code,
                            release_time: vp.release_time,
                            frequency_code: vp.frequency_code,
                        });
                    }
                }
            }
        }
        all_points
    }

    fn validate_pid(pid: &str) -> Result<()> {
        if pid.is_empty() {
            return Err(StatCanError::Api("PID cannot be empty".to_string()));
        }
        if !pid
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(StatCanError::Api("Invalid PID format".to_string()));
        }
        Ok(())
    }

    /// Helper to safely parse API response, handling plain text or HTML errors
    async fn parse_statcan_response(&self, resp: reqwest::Response) -> Result<serde_json::Value> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        Self::parse_response_body(status, &text)
    }

    /// Pure function to parse response body, exposed for testing logic
    fn parse_response_body(status: reqwest::StatusCode, text: &str) -> Result<serde_json::Value> {
        // 1. Try to parse as JSON
        match serde_json::from_str::<serde_json::Value>(text) {
            Ok(json) => Ok(json),
            Err(_) => {
                // 2. Parsing failed. Inspect raw text.
                let text_lower = text.trim().to_lowercase();
                if text_lower.contains("not found")
                    || text_lower.contains("database")
                    || text_lower.contains("unavailable")
                {
                    // Likely "Data not found" or "Database not available"
                    return Err(StatCanError::Api(format!(
                        "StatCan API Error: {}",
                        text.trim()
                    )));
                }
                if text.to_lowercase().contains("<html") || text.to_lowercase().contains("<body") {
                    return Err(StatCanError::Api(format!(
                        "StatCan Gateway Error (HTML received): Status {}",
                        status
                    )));
                }

                // Generic fallback
                Err(StatCanError::Api(format!(
                    "Invalid JSON response: {:.100}",
                    text
                )))
            }
        }
    }

    pub async fn get_all_cubes_list_lite(&self) -> Result<CubeListResponse> {
        // 1. Check cache (Read lock)
        {
            let cache = self.cubes_cache.read().await;
            if let Some(cubes) = &*cache {
                debug!("Cache HIT for getAllCubesListLite");
                return Ok(CubeListResponse {
                    object: Some(cubes.clone()),
                    status: "SUCCESS".to_string(),
                });
            } else {
                debug!("Cache MISS for getAllCubesListLite");
            }
        }

        let url = format!("{}/getAllCubesListLite", BASE_URL);
        let resp = self.client.get(&url).send().await?;

        let body = self.parse_statcan_response(resp).await?;
        let data: Vec<Cube> = serde_json::from_value(body)
            .map_err(|e| StatCanError::Api(format!("Failed to parse cube list: {}", e)))?;

        // 2. Update cache (Write lock)
        {
            let mut cache = self.cubes_cache.write().await;
            *cache = Some(data.clone());
        }

        Ok(CubeListResponse {
            object: Some(data),
            status: "SUCCESS".to_string(),
        })
    }

    pub async fn get_cube_metadata(&self, pid: &str) -> Result<CubeMetadataResponse> {
        Self::validate_pid(pid)?;
        let url = format!("{}/getCubeMetadata", BASE_URL);
        let body_req = json!([{ "productId": pid }]);
        let resp = self.client.post(&url).json(&body_req).send().await?;

        let body_resp = self.parse_statcan_response(resp).await?;
        let mut data: Vec<CubeMetadataResponse> = serde_json::from_value(body_resp)
            .map_err(|e| StatCanError::Api(format!("Failed to parse metadata: {}", e)))?;

        if let Some(item) = data.pop() {
            if item.status != "SUCCESS" {
                return Err(StatCanError::Api(format!("Status: {}", item.status)));
            }
            Ok(item)
        } else {
            Err(StatCanError::Api("Empty response".to_string()))
        }
    }

    pub async fn get_cubes_metadata_batch(
        &self,
        pids: Vec<String>,
    ) -> Result<Vec<CubeMetadataResponse>> {
        let url = format!("{}/getCubeMetadata", BASE_URL);
        let body_req: Vec<_> = pids.iter().map(|pid| json!({ "productId": pid })).collect();
        let resp = self.client.post(&url).json(&body_req).send().await?;

        let body_resp = self.parse_statcan_response(resp).await?;
        let data: Vec<CubeMetadataResponse> = serde_json::from_value(body_resp)
            .map_err(|e| StatCanError::Api(format!("Failed to parse metadata: {}", e)))?;

        Ok(data)
    }

    /// Find cubes that contain a specific dimension name (case-insensitive substring)
    /// Optimizes by searching titles first, then checking metadata of top matches.
    pub async fn find_cubes_by_dimension(
        &self,
        dim_query: &str,
        limit: usize,
    ) -> Result<Vec<(String, String, String)>> {
        // 1. Get all cubes (lite) - this is cached by the OS usually or fast enough (array of structs)
        let all_cubes = self.get_all_cubes_list_lite().await?;
        let cubes = all_cubes.object.unwrap_or_default();

        let query_lower = dim_query.to_lowercase();

        // 2. Filter by Title Match (Heuristic: Title usually contains dimension keywords)
        // We take top 50 title matches to inspect deeply
        let candidates: Vec<&Cube> = cubes
            .iter()
            .filter(|c| c.cube_title_en.to_lowercase().contains(&query_lower))
            .take(50)
            .collect();

        let mut results = Vec::new();

        let pids: Vec<String> = candidates.iter().map(|c| c.product_id.clone()).collect();
        if pids.is_empty() {
            return Ok(results);
        }

        // 3. Inspect Metadata for candidates (Batched)
        // Fetch metadata in one go instead of sequential requests
        if let Ok(metadata_list) = self.get_cubes_metadata_batch(pids).await {
            for meta_resp in metadata_list {
                // Respect user limit on RESULTS
                if results.len() >= limit {
                    break;
                }

                if meta_resp.status == "SUCCESS" {
                    if let Some(obj) = meta_resp.object {
                        // Check if any dimension matches query strictly
                        let has_dim = obj
                            .dimension
                            .iter()
                            .any(|d| d.dimension_name_en.to_lowercase().contains(&query_lower));

                        if has_dim {
                            results.push((
                                obj.product_id.clone(),
                                obj.cube_title_en.clone(),
                                // Return the specifically matching dimension name(s) joined?
                                obj.dimension
                                    .iter()
                                    .filter(|d| {
                                        d.dimension_name_en.to_lowercase().contains(&query_lower)
                                    })
                                    .map(|d| d.dimension_name_en.clone())
                                    .collect::<Vec<_>>()
                                    .join(", "),
                            ));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub async fn get_data_from_coords(
        &self,
        pid: &str,
        coords: Vec<String>,
        periods: i32,
    ) -> Result<DataResponse> {
        Self::validate_pid(pid)?;
        let url = format!("{}/getDataFromCubePidCoordAndLatestNPeriods", BASE_URL);

        let payload: Vec<_> = coords
            .into_iter()
            .map(|c_owned| {
                let padded_string = pad_coordinate(&c_owned);

                info!(
                    "Fetching coord: original='{}', padded='{}', periods={}",
                    c_owned.trim(),
                    padded_string,
                    periods
                );

                let pid_val = if let Ok(n) = pid.parse::<i64>() {
                    json!(n)
                } else {
                    json!(pid)
                };

                json!({
                    "productId": pid_val,
                    "coordinate": padded_string,
                    "latestN": periods
                })
            })
            .collect();

        let resp = self.client.post(&url).json(&payload).send().await?;

        // Use robust parsing
        let body = self.parse_statcan_response(resp).await?;
        let responses: Vec<VectorDataResponse> = serde_json::from_value(body).map_err(|e| {
            StatCanError::Api(format!("Failed to deserialize coords response: {}", e))
        })?;

        // Flatten and map to DataResponse
        let all_points = Self::extract_data_points(responses);

        Ok(DataResponse {
            status: "SUCCESS".to_string(),
            object: Some(all_points),
        })
    }

    pub async fn get_data_from_vectors(
        &self,
        vectors: Vec<String>,
        periods: i32,
    ) -> Result<DataResponse> {
        let url = format!("{}/getDataFromVectorsAndLatestNPeriods", BASE_URL);
        let payload: Vec<_> = vectors
            .iter()
            .map(|v| {
                let v_clean = v.to_lowercase().replace("v", "");
                // Parse to int if possible, else generic string
                let id_val = if let Ok(n) = v_clean.parse::<i64>() {
                    json!(n)
                } else {
                    json!(v_clean)
                };

                json!({
                    "vectorId": id_val,
                    "latestN": periods
                })
            })
            .collect();

        let resp = self.client.post(&url).json(&payload).send().await?;

        // Use robust parsing
        let body = self.parse_statcan_response(resp).await?;
        debug!("Vector response body: {:?}", body);

        // Check if it's an error object (StatCan sometimes returns object instead of array on failure)
        if body.is_object() {
            // Direct inspection to avoid cloning and deserialization overhead
            let mut is_error = false;
            let mut status_msg = "FAILED".to_string();

            if let Some(s) = body.get("status").and_then(|v| v.as_str()) {
                if s != "SUCCESS" {
                    is_error = true;
                    status_msg = s.to_string();
                }
            } else if let Some(msg) = body.get("message").and_then(|v| v.as_str()) {
                is_error = true;
                status_msg = msg.to_string();
            }

            if is_error {
                info!(
                    "API returned error for vectors: {:?} -> {}",
                    vectors, status_msg
                );
                return Ok(DataResponse {
                    status: status_msg,
                    object: Some(Vec::new()),
                });
            }
        }

        // If it's an array, or success object (if that happens?), try standard deserialization
        let responses: Vec<VectorDataResponse> =
            serde_json::from_value(body.clone()).map_err(|e| {
                info!("Failed JSON body: {}", body);
                StatCanError::Api(format!("Failed to deserialize vector response: {}", e))
            })?;

        // Flatten and map
        let all_points = Self::extract_data_points(responses);

        Ok(DataResponse {
            status: "SUCCESS".to_string(),
            object: Some(all_points),
        })
    }

    pub async fn fetch_fast_snippet(&self, pid: &str) -> Result<StatCanDataFrame> {
        Self::validate_pid(pid)?;
        // 1. Get Metadata to find dimensions and valid members
        let metadata = self.get_cube_metadata(pid).await?;
        let meta_obj = metadata
            .object
            .ok_or(StatCanError::Api("No metadata found".to_string()))?;

        // 2. Construct coordinate and prepare dimension columns
        let mut coords_parts = Vec::new();
        let mut check_added_columns = Vec::new();

        for dim in meta_obj.dimension {
            if let Some(first_member) = dim.member.first() {
                coords_parts.push(first_member.member_id.to_string());
                check_added_columns
                    .push((dim.dimension_name_en, first_member.member_name_en.clone()));
            } else {
                // Dimension has no members? Use "1" as fallback but this is risky
                coords_parts.push("1".to_string());
                check_added_columns.push((dim.dimension_name_en, "Unknown".to_string()));
            }
        }

        if coords_parts.is_empty() {
            return Err(StatCanError::Api("Cube has no dimensions".to_string()));
        }

        // Join to standard coordinate: "1.1.1..."
        let coord_str = coords_parts.join(".");

        // 3. Fetch data for this coordinate (recent 1 period)
        let data_resp = self.get_data_from_coords(pid, vec![coord_str], 1).await?;

        // 4. Convert to DataFrame
        let points = data_resp.object.unwrap_or_default();
        if points.is_empty() {
            return Ok(StatCanDataFrame::new(DataFrame::default()));
        }

        let df_wrapper = StatCanDataFrame::from_data_points(points)?;
        let df = df_wrapper.into_polars();

        // 5. Enrich with Dimension Columns (e.g. "Geography" = "Canada")
        // We use lazy execution to add literal columns
        let mut lazy = df.lazy();
        for (col_name, col_val) in check_added_columns {
            lazy = lazy.with_column(lit(col_val).alias(&col_name));
        }

        let enriched = lazy.collect()?;
        Ok(StatCanDataFrame::new(enriched))
    }

    pub async fn get_full_cube_from_cube_pid(&self, pid: &str) -> Result<FullTableResponse> {
        Self::validate_pid(pid)?;
        let url = format!("{}/getFullTableDownloadCSV/{}/en", BASE_URL, pid);
        let resp = self.client.get(&url).send().await?;
        // Note: Full table download returns metadata JSON, not data. Data is in the URL inside.
        // We can parse safely too.
        let body = self.parse_statcan_response(resp).await?;
        let data: FullTableResponse = serde_json::from_value(body).map_err(|e| {
            StatCanError::Api(format!("Failed to parse full table response: {}", e))
        })?;

        if data.status != "SUCCESS" {
            return Err(StatCanError::Api(format!("Status: {}", data.status)));
        }
        Ok(data)
    }

    async fn get_cache_path(&self, pid: &str) -> Result<std::path::PathBuf> {
        Self::validate_pid(pid)?;
        let mut path = std::env::temp_dir();
        path.push("statcan");
        tokio::fs::create_dir_all(&path).await.unwrap_or(()); // Ensure dir exists
        path.push(format!("{}.csv", pid));
        Ok(path)
    }

    async fn fetch_file_with_cache(&self, pid: &str) -> Result<std::path::PathBuf> {
        let csv_path = self.get_cache_path(pid).await?;
        if tokio::fs::try_exists(&csv_path).await.unwrap_or(false) {
            info!("Cache hit for PID: {}", pid);
            return Ok(csv_path);
        }

        info!("Cache miss for PID: {}. Downloading...", pid);
        // 1. Get the URL
        let metadata = self.get_full_cube_from_cube_pid(pid).await?;
        let download_url = metadata.object.ok_or(StatCanError::TableNotFound)?;

        // 2. Download the ZIP (Streaming)
        let mut zip_resp = self.client.get(&download_url).send().await?;
        let zip_path = std::env::temp_dir().join(format!("statcan/{}.zip", pid));

        let mut zip_file = tokio::fs::File::create(&zip_path).await?;
        while let Some(chunk) = zip_resp.chunk().await? {
            use tokio::io::AsyncWriteExt;
            zip_file.write_all(&chunk).await?;
        }
        zip_file.sync_all().await?;
        drop(zip_file); // Close file

        // 3. Unzip (Blocking)
        let zip_path_clone = zip_path.clone();
        let csv_path_clone = csv_path.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let file = std::fs::File::open(&zip_path_clone)?;
            let mut archive = ZipArchive::new(file)?;
            // Usually the CSV file inside has the same name as the PID or similar.
            // We'll just take the first file.
            let mut csv_file = archive.by_index(0)?;
            let mut out_file = std::fs::File::create(&csv_path_clone)?;
            std::io::copy(&mut csv_file, &mut out_file)?;
            Ok(())
        })
        .await
        .map_err(|e| StatCanError::Io(std::io::Error::other(e)))??;

        // Cleanup ZIP
        let _ = tokio::fs::remove_file(zip_path).await;

        Ok(csv_path)
    }

    pub async fn fetch_full_table(&self, pid: &str) -> Result<StatCanDataFrame> {
        Self::validate_pid(pid)?;
        let csv_path = self.fetch_file_with_cache(pid).await?;

        // 4. Parse with Polars (Blocking to avoid stalling async runtime)
        let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
            let df = CsvReader::from_path(csv_path)?
                .infer_schema(Some(100))
                .has_header(true)
                .finish()?;
            Ok(df)
        })
        .await
        .map_err(|e| StatCanError::Io(std::io::Error::other(e)))??;

        Ok(StatCanDataFrame::new(df))
    }
}

#[async_trait]
impl CKANClient for StatCanDriver {
    async fn ping(&self) -> Result<String> {
        // Simple health check
        let url = format!("{}/getAllCubesListLite", BASE_URL);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            Ok("StatCan WDS OK".to_string())
        } else {
            Err(StatCanError::Api(format!("Ping failed: {}", resp.status())))
        }
    }

    async fn search_packages(&self, query: &str, limit: usize) -> Result<Vec<PackageMetadata>> {
        let mut packages = Vec::new();

        // 1. Try to find cubes by dimension name if query looks like a dimension.
        let results = self
            .find_cubes_by_dimension(query, limit)
            .await
            .unwrap_or_default();
        for (pid, title, _) in results {
            packages.push(PackageMetadata {
                id: pid.clone(),
                title: title,
                notes: None,
                url: None,
                resources: vec![],
            });
        }

        // If no results and query looks like PID, try fetching metadata directly
        if packages.is_empty() && regex::Regex::new(r"^\d+$").unwrap().is_match(query) {
            if let Ok(meta) = self.get_cube_metadata(query).await {
                if let Some(obj) = meta.object {
                    packages.push(PackageMetadata {
                        id: obj.product_id.clone(),
                        title: obj.cube_title_en,
                        notes: None,
                        url: None,
                        resources: vec![],
                    });
                }
            }
        }

        Ok(packages)
    }

    async fn get_package_metadata(&self, id: &str) -> Result<PackageMetadata> {
        let meta = self.get_cube_metadata(id).await?;
        let obj = meta.object.ok_or(StatCanError::TableNotFound)?;

        // Construct a virtual CSV resource
        let csv_resource = ResourceMetadata {
            id: format!("{}-csv", obj.product_id),
            name: format!("{} (CSV)", obj.cube_title_en),
            format: Some("CSV".to_string()),
            url: None,
        };

        Ok(PackageMetadata {
            id: obj.product_id,
            title: obj.cube_title_en,
            notes: None,
            url: None,
            resources: vec![csv_resource],
        })
    }

    async fn get_resource_handler(&self, resource_id: &str) -> Result<DataHandler> {
        let pid = resource_id.trim_end_matches("-csv");
        let metadata = self.get_full_cube_from_cube_pid(pid).await?;
        let download_url = metadata.object.ok_or(StatCanError::TableNotFound)?;
        Ok(DataHandler::BlobDownload(download_url))
    }

    async fn query_datastore(&self, _sql: &str) -> Result<Vec<serde_json::Value>> {
        Err(StatCanError::Api(
            "Datastore SQL queries not supported by StatCan WDS".to_string(),
        ))
    }

    async fn get_resource_schema(&self, resource_id: &str) -> Result<Vec<(String, String)>> {
        // For StatCan, we can infer schema from the full table if downloaded
        let df = self.fetch_full_table(resource_id).await?;
        let polars_df = df.into_polars();
        let schema = polars_df.schema();
        let mut result = Vec::new();
        for field in schema.iter_fields() {
            result.push((field.name().to_string(), format!("{:?}", field.dtype)));
        }
        Ok(result)
    }
}

#[derive(Clone)]
pub struct GenericCKANDriver {
    client: Client,
    base_url: String,
}

impl GenericCKANDriver {
    pub fn new(base_url: &str) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .gzip(true)
            .build()?;
        // Ensure base_url doesn't end with slash to make joining easier
        let base_url = base_url.trim_end_matches('/').to_string();
        Ok(Self { client, base_url })
    }
}

#[async_trait]
impl CKANClient for GenericCKANDriver {
    async fn ping(&self) -> Result<String> {
        // Simple check: site_read or just root API
        let url = format!("{}/api/3/action/site_read", self.base_url);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            Ok("CKAN API OK".to_string())
        } else {
            // Fallback: Try package_search with limit 0
            let fallback_url = format!("{}/api/3/action/package_search?rows=0", self.base_url);
            let fallback_resp = self.client.get(&fallback_url).send().await?;
            if fallback_resp.status().is_success() {
                Ok("CKAN API OK (Fallback)".to_string())
            } else {
                Err(StatCanError::Api(format!("Ping failed: {}", resp.status())))
            }
        }
    }

    async fn search_packages(&self, query: &str, limit: usize) -> Result<Vec<PackageMetadata>> {
        let url = format!("{}/api/3/action/package_search", self.base_url);
        let resp = self
            .client
            .get(&url)
            .query(&[("q", query), ("rows", &limit.to_string())])
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(StatCanError::Api(format!(
                "Search failed: {}",
                resp.status()
            )));
        }

        let body: serde_json::Value = resp.json().await?;

        if let Some(false) = body["success"].as_bool() {
            return Err(StatCanError::Api(
                "CKAN API Error: search unsuccessful".to_string(),
            ));
        }

        let results = body["result"]["results"]
            .as_array()
            .ok_or(StatCanError::Api(
                "Invalid response structure: results missing".to_string(),
            ))?;

        let mut packages = Vec::new();
        for pkg in results {
            let id = pkg["id"].as_str().unwrap_or_default().to_string();
            let title = pkg["title"].as_str().unwrap_or_default().to_string();
            let notes = pkg["notes"].as_str().map(|s| s.to_string());
            let url = pkg["url"].as_str().map(|s| s.to_string());

            let mut resources = Vec::new();
            if let Some(res_list) = pkg["resources"].as_array() {
                for res in res_list {
                    resources.push(ResourceMetadata {
                        id: res["id"].as_str().unwrap_or_default().to_string(),
                        name: res["name"].as_str().unwrap_or_default().to_string(),
                        format: res["format"].as_str().map(|s| s.to_string()),
                        url: res["url"].as_str().map(|s| s.to_string()),
                    });
                }
            }

            packages.push(PackageMetadata {
                id,
                title,
                notes,
                url,
                resources,
            });
        }

        Ok(packages)
    }

    async fn get_package_metadata(&self, id: &str) -> Result<PackageMetadata> {
        let url = format!("{}/api/3/action/package_show", self.base_url);
        let resp = self.client.get(&url).query(&[("id", id)]).send().await?;

        if !resp.status().is_success() {
            return Err(StatCanError::Api(format!(
                "Get package failed: {}",
                resp.status()
            )));
        }

        let body: serde_json::Value = resp.json().await?;
        if let Some(false) = body["success"].as_bool() {
            return Err(StatCanError::Api(
                "CKAN API Error: package_show unsuccessful".to_string(),
            ));
        }

        let pkg = &body["result"];
        let pkg_id = pkg["id"].as_str().unwrap_or_default().to_string();
        let title = pkg["title"].as_str().unwrap_or_default().to_string();
        let notes = pkg["notes"].as_str().map(|s| s.to_string());
        let pkg_url = pkg["url"].as_str().map(|s| s.to_string());

        let mut resources = Vec::new();
        if let Some(res_list) = pkg["resources"].as_array() {
            for res in res_list {
                resources.push(ResourceMetadata {
                    id: res["id"].as_str().unwrap_or_default().to_string(),
                    name: res["name"].as_str().unwrap_or_default().to_string(),
                    format: res["format"].as_str().map(|s| s.to_string()),
                    url: res["url"].as_str().map(|s| s.to_string()),
                });
            }
        }

        Ok(PackageMetadata {
            id: pkg_id,
            title,
            notes,
            url: pkg_url,
            resources,
        })
    }

    async fn get_resource_handler(&self, resource_id: &str) -> Result<DataHandler> {
        // We need to fetch resource details to see if datastore is active
        // Typically, we use resource_show
        let url = format!("{}/api/3/action/resource_show", self.base_url);
        let resp = self
            .client
            .get(&url)
            .query(&[("id", resource_id)])
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(StatCanError::Api(format!(
                "Get resource failed: {}",
                resp.status()
            )));
        }

        let body: serde_json::Value = resp.json().await?;
        if let Some(false) = body["success"].as_bool() {
            return Err(StatCanError::Api(
                "CKAN API Error: resource_show unsuccessful".to_string(),
            ));
        }

        let res = &body["result"];
        let datastore_active = res["datastore_active"].as_bool().unwrap_or(false);
        let download_url = res["url"]
            .as_str()
            .ok_or(StatCanError::Api("Resource has no URL".to_string()))?
            .to_string();

        if datastore_active {
            Ok(DataHandler::DatastoreQuery(
                resource_id.to_string(),
                Some(download_url),
            ))
        } else {
            Ok(DataHandler::BlobDownload(download_url))
        }
    }

    async fn query_datastore(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let url = format!("{}/api/3/action/datastore_search_sql", self.base_url);

        // The Open Data Canada API expects the SQL query in the 'sql' parameter.
        // It seems the previous error "Action name not known: datastore_search_sql"
        // might indicate that this specific action is restricted or not enabled on this CKAN instance,
        // OR that the URL construction was slightly off.
        // However, standard CKAN uses /datastore_search_sql.
        // Let's try to be robust and also check if we need to encode it differently.

        let resp = self.client.get(&url).query(&[("sql", sql)]).send().await?;

        if !resp.status().is_success() {
            // Try to parse error message
            let status = resp.status();
            let error_text = resp.text().await.unwrap_or_default();
            // Pass back the raw error for debugging if parsing fails
            return Err(StatCanError::Api(format!(
                "SQL query failed ({}): {}",
                status, error_text
            )));
        }

        let body: serde_json::Value = resp.json().await?;

        if let Some(false) = body["success"].as_bool() {
            let error_msg = body["error"]
                .as_str()
                .or_else(|| body["error"]["message"].as_str())
                .unwrap_or("Unknown error");
            // Check specifically for the "Action name not known" error
            if error_msg.contains("Action name not known") {
                return Err(StatCanError::Api(
                    "Datastore SQL search is not enabled on this server.".to_string(),
                ));
            }
            return Err(StatCanError::Api(format!("CKAN API Error: {}", error_msg)));
        }

        // success is true, but we need to check if 'result' exists and has 'records'
        let records = body["result"]["records"]
            .as_array()
            .ok_or(StatCanError::Api(
                "Invalid response: records missing".to_string(),
            ))?;

        Ok(records.clone())
    }

    async fn get_resource_schema(&self, resource_id: &str) -> Result<Vec<(String, String)>> {
        let df = self.fetch_full_table(resource_id).await?;
        let polars_df = df.into_polars();
        let schema = polars_df.schema();
        let mut result = Vec::new();
        for field in schema.iter_fields() {
            result.push((field.name().to_string(), format!("{:?}", field.dtype)));
        }
        Ok(result)
    }
}

pub async fn download_and_extract_file(
    client: &Client,
    url: &str,
    pid: &str,
) -> Result<std::path::PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        let cache_dir = std::path::PathBuf::from(home).join(".cache/statcan-rs/resources");
        let cached_path = cache_dir.join(format!("{}.csv", pid));
        if tokio::fs::try_exists(&cached_path).await.unwrap_or(false) {
            return Ok(cached_path);
        }
    }

    let mut path = std::env::temp_dir();
    path.push("statcan");
    tokio::fs::create_dir_all(&path).await.unwrap_or(());
    let csv_path = path.join(format!("{}.csv", pid));

    if tokio::fs::try_exists(&csv_path).await.unwrap_or(false) {
        return Ok(csv_path);
    }

    let mut resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(StatCanError::Api(format!(
            "Download failed: {}",
            resp.status()
        )));
    }

    // Determine if it's a zip based on Content-Type or URL extension
    let is_zip = {
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        url.to_lowercase().ends_with(".zip") || content_type.contains("zip")
    };

    if is_zip {
        let zip_path = std::env::temp_dir().join(format!("statcan/{}.zip", pid));
        let mut zip_file = tokio::fs::File::create(&zip_path).await?;
        while let Some(chunk) = resp.chunk().await? {
            use tokio::io::AsyncWriteExt;
            zip_file.write_all(&chunk).await?;
        }
        zip_file.sync_all().await?;
        drop(zip_file); // Close file

        let zip_path_clone = zip_path.clone();
        let csv_path_clone = csv_path.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let file = std::fs::File::open(&zip_path_clone)?;
            let mut archive = ZipArchive::new(file)?;
            // Usually the CSV file inside has the same name as the PID or similar.
            // We'll just take the first file.
            let mut csv_file = archive.by_index(0)?;
            let mut out_file = std::fs::File::create(&csv_path_clone)?;
            std::io::copy(&mut csv_file, &mut out_file)?;
            Ok(())
        })
        .await
        .map_err(|e| StatCanError::Io(std::io::Error::other(e)))??;

        let _ = tokio::fs::remove_file(zip_path).await;
    } else {
        // Assume direct CSV download
        let mut out_file = tokio::fs::File::create(&csv_path).await?;
        while let Some(chunk) = resp.chunk().await? {
            use tokio::io::AsyncWriteExt;
            out_file.write_all(&chunk).await?;
        }
        out_file.sync_all().await?;
    }

    // Copy to persistent cache if possible
    if let Ok(home) = std::env::var("HOME") {
        let cache_dir = std::path::PathBuf::from(home).join(".cache/statcan-rs/resources");
        let _ = tokio::fs::create_dir_all(&cache_dir).await;
        let cached_path = cache_dir.join(format!("{}.csv", pid));
        let _ = tokio::fs::copy(&csv_path, &cached_path).await;
    }

    Ok(csv_path)
}

impl StatCanClientTrait for GenericCKANDriver {
    async fn get_all_cubes_list_lite(&self) -> Result<CubeListResponse> {
        let packages = self.search_packages("*", 100).await?;
        let cubes: Vec<Cube> = packages
            .into_iter()
            .map(|p| Cube {
                product_id: p.id.clone(),
                cube_title_en: p.title,
                cube_pid: Some(p.id),
            })
            .collect();
        Ok(CubeListResponse {
            status: "SUCCESS".to_string(),
            object: Some(cubes),
        })
    }

    async fn get_cube_metadata(&self, pid: &str) -> Result<CubeMetadataResponse> {
        let meta = self.get_package_metadata(pid).await?;
        // Map PackageMetadata to minimal CubeMetadataResponse
        // We only populate what we can
        let dim = vec![Dimension {
            dimension_name_en: "Columns".to_string(),
            position_id: 1,
            member: vec![], // No members known upfront
        }];

        Ok(CubeMetadataResponse {
            status: "SUCCESS".to_string(),
            object: Some(crate::models::CubeMetadata {
                product_id: meta.id,
                cube_title_en: meta.title,
                dimension: dim,
            }),
        })
    }

    async fn find_cubes_by_dimension(
        &self,
        dim_query: &str,
        limit: usize,
    ) -> Result<Vec<(String, String, String)>> {
        let packages = self.search_packages(dim_query, limit).await?;
        Ok(packages
            .into_iter()
            .map(|p| (p.id, p.title, dim_query.to_string()))
            .collect())
    }

    async fn get_data_from_vectors(
        &self,
        _vectors: Vec<String>,
        _periods: i32,
    ) -> Result<DataResponse> {
        Err(StatCanError::Api(
            "Vector data not supported by generic CKAN driver".to_string(),
        ))
    }

    async fn get_data_from_coords(
        &self,
        _pid: &str,
        _coords: Vec<String>,
        _periods: i32,
    ) -> Result<DataResponse> {
        Err(StatCanError::Api(
            "Coordinate data not supported by generic CKAN driver".to_string(),
        ))
    }

    async fn fetch_fast_snippet(&self, pid: &str) -> Result<StatCanDataFrame> {
        // Fallback to fetching full table and taking head
        let df = self.fetch_full_table(pid).await?;
        Ok(df) // Not technically a snippet but works
    }

    async fn fetch_full_table(&self, pid: &str) -> Result<StatCanDataFrame> {
        // 1. Get Resource Handler
        let handler = self.get_resource_handler(pid).await?;

        let url = match handler {
            DataHandler::BlobDownload(u) => u,
            DataHandler::DatastoreQuery(_, Some(u)) => u,
            DataHandler::DatastoreQuery(_, None) => {
                return Err(StatCanError::Api(
                    "Cannot fetch full table from Datastore query: no download URL available"
                        .to_string(),
                ))
            }
        };

        // 2. Download
        let csv_path = download_and_extract_file(&self.client, &url, pid).await?;

        // 3. Load to Polars
        let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
            let df = CsvReader::from_path(csv_path)?
                .infer_schema(Some(100))
                .has_header(true)
                .finish()?;
            Ok(df)
        })
        .await
        .map_err(|e| StatCanError::Io(std::io::Error::other(e)))??;

        Ok(StatCanDataFrame::new(df))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pad_coordinate_basic() {
        assert_eq!(pad_coordinate("1.1.1"), "1.1.1.0.0.0.0.0.0.0");
    }

    #[test]
    fn test_pad_coordinate_no_padding_needed() {
        let full_coord = "1.1.1.1.1.1.1.1.1.1";
        assert_eq!(pad_coordinate(full_coord), full_coord);
    }

    #[test]
    fn test_pad_coordinate_trimming() {
        assert_eq!(pad_coordinate("  1.2.3  "), "1.2.3.0.0.0.0.0.0.0");
    }

    #[test]
    fn test_pad_coordinate_single_part() {
        assert_eq!(pad_coordinate("1"), "1.0.0.0.0.0.0.0.0.0");
    }

    #[test]
    fn test_pad_coordinate_empty() {
        // Current logic: "" -> [""] -> len 1 -> needs 9 -> ".0.0.0.0.0.0.0.0.0"
        // This is arguably a bug but we are testing current behavior after refactor.
        assert_eq!(pad_coordinate(""), ".0.0.0.0.0.0.0.0.0");
    }

    #[test]
    fn test_pad_coordinate_already_long() {
        let long_coord = "1.2.3.4.5.6.7.8.9.10.11.12";
        assert_eq!(pad_coordinate(long_coord), long_coord);
    }

    #[test]
    fn test_parse_statcan_response_logic() {
        // 1. Valid JSON
        let valid_json = r#"{"status": "SUCCESS", "object": []}"#;
        let res = StatCanClient::parse_response_body(reqwest::StatusCode::OK, valid_json);
        assert!(res.is_ok());
        let val = res.unwrap();
        assert_eq!(val["status"], "SUCCESS");

        // 2. Data not found (case insensitive, contains "not found")
        let not_found_text = "Data not found for this cube";
        let res =
            StatCanClient::parse_response_body(reqwest::StatusCode::NOT_FOUND, not_found_text);
        assert!(res.is_err());
        match res.unwrap_err() {
            StatCanError::Api(msg) => assert!(msg.contains("StatCan API Error: Data not found")),
            _ => panic!("Expected Api error"),
        }

        // 3. HTML Error
        let html_error = "<html><body>Error</body></html>";
        let res = StatCanClient::parse_response_body(reqwest::StatusCode::BAD_GATEWAY, html_error);
        assert!(res.is_err());
        match res.unwrap_err() {
            StatCanError::Api(msg) => {
                assert!(msg.contains("StatCan Gateway Error"));
                assert!(msg.contains("502")); // BAD_GATEWAY is 502
            }
            _ => panic!("Expected Api error"),
        }

        // 4. Generic fallback (Invalid JSON, not special case)
        let garbage = "This is not JSON and not special error";
        let res = StatCanClient::parse_response_body(reqwest::StatusCode::OK, garbage);
        assert!(res.is_err());
        match res.unwrap_err() {
            StatCanError::Api(msg) => assert!(msg.contains("Invalid JSON response")),
            _ => panic!("Expected Api error"),
        }

        // 5. Data not found (does not start with D)
        let not_found_text_2 = "Error: Data not found";
        let res =
            StatCanClient::parse_response_body(reqwest::StatusCode::NOT_FOUND, not_found_text_2);
        assert!(res.is_err());
        match res.unwrap_err() {
            StatCanError::Api(msg) => {
                assert!(msg.contains("StatCan API Error: Error: Data not found"))
            }
            _ => panic!("Expected Api error"),
        }
    }

    #[tokio::test]
    async fn test_get_cache_path_security() {
        let client = StatCanClient::new().unwrap();
        // Should fail because of validation
        let res = client.get_cache_path("../bad_path").await;
        assert!(res.is_err());
        match res.unwrap_err() {
            StatCanError::Api(msg) => assert_eq!(msg, "Invalid PID format"),
            _ => panic!("Expected Api error"),
        }

        // Should succeed for valid PID
        let res_ok = client.get_cache_path("12345678").await;
        assert!(res_ok.is_ok());
    }
}
