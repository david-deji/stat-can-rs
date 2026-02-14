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
use std::time::Duration;
use thiserror::Error;
use tracing::info;

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

pub struct StatCanClient {
    client: Client,
}

impl StatCanClient {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .gzip(true)
            .build()?;
        Ok(Self { client })
    }

    pub async fn get_all_cubes_list_lite(&self) -> Result<CubeListResponse> {
        let url = format!("{}/getAllCubesListLite", BASE_URL);
        let resp = self.client.get(&url).send().await?;
        let data: Vec<Cube> = resp.json().await?;
        Ok(CubeListResponse {
            object: Some(data),
            status: "SUCCESS".to_string(), // Implicit success if we got a list
        })
    }

    pub async fn get_cube_metadata(&self, pid: &str) -> Result<CubeMetadataResponse> {
        let url = format!("{}/getCubeMetadata", BASE_URL);
        let body = json!([{ "productId": pid }]);
        let resp = self.client.post(&url).json(&body).send().await?;
        // The API returns a list of results, we asked for one.
        let mut data: Vec<CubeMetadataResponse> = resp.json().await?;

        if let Some(item) = data.pop() {
            if item.status != "SUCCESS" {
                return Err(StatCanError::Api(format!("Status: {}", item.status)));
            }
            Ok(item)
        } else {
            Err(StatCanError::Api("Empty response".to_string()))
        }
    }

    pub async fn get_data_from_coords(
        &self,
        pid: &str,
        coords: Vec<String>,
    ) -> Result<DataResponse> {
        let url = format!("{}/getDataFromCubePidCoordAndLatestNPeriods", BASE_URL);

        let payload: Vec<_> = coords
            .into_iter() // Changed from .iter() to consume `coords`
            .map(|c_owned| {
                let c = c_owned.trim(); // Trim whitespace
                                        // Pad coordinate to 10 components if needed
                let parts: Vec<&str> = c.split('.').collect();
                let mut padded_string = c.to_string(); // Use c.to_string() as c is now &str
                if parts.len() < 10 {
                    let needed = 10 - parts.len();
                    for _ in 0..needed {
                        padded_string.push_str(".0");
                    }
                }

                info!(
                    "Fetching coord: original='{}', padded='{}'",
                    c, padded_string
                );

                let pid_val = if let Ok(n) = pid.parse::<i64>() {
                    json!(n)
                } else {
                    json!(pid)
                };

                json!({
                    "productId": pid_val,
                    "coordinate": padded_string,
                    "latestN": 1
                })
            })
            .collect();

        let resp = self.client.post(&url).json(&payload).send().await?;

        if !resp.status().is_success() {
            return Err(StatCanError::Api(format!(
                "API Error {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        let responses: Vec<VectorDataResponse> = resp.json().await?;

        // Flatten and map to DataResponse
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

        Ok(DataResponse {
            status: "SUCCESS".to_string(),
            object: Some(all_points),
        })
    }

    pub async fn get_data_from_vectors(&self, vectors: Vec<String>) -> Result<DataResponse> {
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
                    "latestN": 1
                })
            })
            .collect();

        let resp = self.client.post(&url).json(&payload).send().await?;
        let responses: Vec<VectorDataResponse> = resp.json().await?;

        // Flatten and map
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

        Ok(DataResponse {
            status: "SUCCESS".to_string(),
            object: Some(all_points),
        })
    }

    pub async fn get_full_cube_from_cube_pid(&self, pid: &str) -> Result<FullTableResponse> {
        let url = format!("{}/getFullTableDownloadCSV/{}/en", BASE_URL, pid);
        let resp = self.client.get(&url).send().await?;
        let data: FullTableResponse = resp.json().await?;

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
        if csv_path.exists() {
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
        .map_err(|e| StatCanError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;

        // Cleanup ZIP
        let _ = tokio::fs::remove_file(zip_path).await;

        Ok(csv_path)
    }

    pub async fn fetch_full_table(&self, pid: &str) -> Result<StatCanDataFrame> {
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
        .map_err(|e| StatCanError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;

        Ok(StatCanDataFrame::new(df))
    }
}
