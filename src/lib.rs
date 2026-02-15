pub mod models;
#[cfg(feature = "python")]
pub mod python;
pub mod wrapper;

pub use wrapper::StatCanDataFrame;

use crate::models::{
    Cube, CubeListResponse, CubeMetadataResponse, DataPoint, DataResponse, FullTableResponse,
    VectorDataResponse,
};
use ::zip::ZipArchive;
use polars::prelude::*;
use reqwest::Client;
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

pub trait StatCanClientTrait: Send + Sync {
    fn get_all_cubes_list_lite(
        &self,
    ) -> impl Future<Output = Result<CubeListResponse>> + Send;
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
    fn fetch_full_table(
        &self,
        pid: &str,
    ) -> impl Future<Output = Result<StatCanDataFrame>> + Send;
}

impl StatCanClientTrait for StatCanClient {
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

pub struct StatCanClient {
    client: Client,
    cubes_cache: Arc<RwLock<Option<Vec<Cube>>>>,
}

impl StatCanClient {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
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
                if text.trim().starts_with("D") && text.contains("not found") {
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
        let body_req: Vec<_> = pids
            .iter()
            .map(|pid| json!({ "productId": pid }))
            .collect();
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
            // If it has "status" != "SUCCESS" or just looks like an error
            // We can try to deserialize as StatCanErrorResponse
            if let Ok(err_resp) =
                serde_json::from_value::<crate::models::StatCanErrorResponse>(body.clone())
            {
                let mut is_error = false;
                let mut status_msg = "FAILED".to_string();

                if let Some(s) = &err_resp.status {
                    if s != "SUCCESS" {
                        is_error = true;
                        status_msg = s.clone();
                    }
                } else if let Some(msg) = &err_resp.message {
                    is_error = true;
                    status_msg = msg.clone();
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

    fn get_cache_path(&self, pid: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push("statcan");
        std::fs::create_dir_all(&path).unwrap_or(()); // Ensure dir exists
        path.push(format!("{}.csv", pid));
        path
    }

    async fn fetch_file_with_cache(&self, pid: &str) -> Result<std::path::PathBuf> {
        let csv_path = self.get_cache_path(pid);
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

        // 2. Data not found (starts with D, contains "not found")
        let not_found_text = "Data not found for this cube";
        let res = StatCanClient::parse_response_body(reqwest::StatusCode::NOT_FOUND, not_found_text);
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
    }
}
