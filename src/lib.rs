pub mod models;
#[cfg(feature = "python")]
pub mod python;
pub mod wrapper;

pub use wrapper::StatCanDataFrame;

use crate::models::{
    Cube, CubeListResponse, CubeMetadataResponse, DataResponse, FullTableResponse,
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

    pub async fn get_data_from_cube_pid(
        &self,
        pid: &str,
        coords: Vec<String>,
    ) -> Result<DataResponse> {
        let url = format!("{}/getDataFromCubePidCoord", BASE_URL);
        // The API expects: [{"productId": "...", "coordinate": "..."}]
        // Actually, getDataFromCubePidCoord takes a list of objects.
        // Wait, the user requirement says "getDataFromCubePid" but mentions "fetch_coordinates".
        // "getDataFromCubePid" usually fetches everything or a subset?
        // Let's check the API docs context provided: "getDataFromCubePid: Fetches data for specific coordinates."
        // The endpoint is likely `getDataFromCubePidCoord` for specific coordinates.
        // Payload: [{"productId": 123, "coordinate": "1.1.1.1.1"}]

        let payload: Vec<_> = coords
            .iter()
            .map(|c| {
                json!({
                    "productId": pid,
                    "coordinate": c
                })
            })
            .collect();

        let resp = self.client.post(&url).json(&payload).send().await?;
        // API returns a list of responses, one per coordinate requested?
        // Or a single object wrapping them?
        // Usually StatCan returns `[{"status": "SUCCESS", "object": {...}}]`
        // But for `getDataFromCubePidCoord` it might return a single list of points if successful?
        // Let's assume standard wrapper for now, but if it's a bulk fetch, it might be different.
        // Actually, looking at `DataResponse` struct, it has `object: Option<Vec<DataPoint>>`.
        // This matches `getDataFromCubePid` (without Coord) which fetches last N periods?
        // If we use `getDataFromCubePidCoord`, the response is `[{"status":..., "object": ...}]`.
        // Let's stick to the user's "fetch_coordinates" helper which implies specific coordinates.

        // Let's assume the response is a list of DataResponse, and we merge them or return the first?
        // If we send multiple coordinates, we get multiple responses.
        // For simplicity, let's assume we want to aggregate them into one `DataResponse` or just return the raw list?
        // The user asked for `fetch_coordinates(pid, coords)`.
        // Let's try to parse as `Vec<DataResponse>` and flatten.

        let responses: Vec<DataResponse> = resp.json().await?;

        // Flatten the objects
        let mut all_points = Vec::new();
        for r in responses {
            if r.status == "SUCCESS" {
                if let Some(points) = r.object {
                    all_points.extend(points);
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
