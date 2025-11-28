pub mod models;
pub mod wrapper;
#[cfg(feature = "python")]
pub mod python;

pub use wrapper::StatCanDataFrame;

use crate::models::{
    Cube, CubeListResponse, CubeMetadataResponse, DataResponse, FullTableResponse,
};
use ::zip::ZipArchive;
use polars::prelude::*;
use reqwest::Client;
use serde_json::json;
use std::io::{Cursor, Read};
use std::time::Duration;
use thiserror::Error;

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
        let url =format!("{}/getAllCubesListLite", BASE_URL);
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

    pub async fn get_data_from_cube_pid(&self, pid: &str, coords: Vec<String>) -> Result<DataResponse> {
        let url = format!("{}/getDataFromCubePidCoord", BASE_URL);
        // The API expects: [{"productId": "...", "coordinate": "..."}]
        // Actually, getDataFromCubePidCoord takes a list of objects.
        // Wait, the user requirement says "getDataFromCubePid" but mentions "fetch_coordinates".
        // "getDataFromCubePid" usually fetches everything or a subset?
        // Let's check the API docs context provided: "getDataFromCubePid: Fetches data for specific coordinates."
        // The endpoint is likely `getDataFromCubePidCoord` for specific coordinates.
        // Payload: [{"productId": 123, "coordinate": "1.1.1.1.1"}]
        
        let payload: Vec<_> = coords.iter().map(|c| {
            json!({
                "productId": pid,
                "coordinate": c
            })
        }).collect();

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

    pub async fn fetch_full_table(&self, pid: &str) -> Result<StatCanDataFrame> {
        // 1. Get the URL
        let metadata = self.get_full_cube_from_cube_pid(pid).await?;
        let download_url = metadata.object.ok_or(StatCanError::TableNotFound)?;

        // 2. Download the ZIP
        let zip_resp = self.client.get(&download_url).send().await?;
        let zip_bytes = zip_resp.bytes().await?;
        let cursor = Cursor::new(zip_bytes);

        // 3. Unzip
        let mut archive = ZipArchive::new(cursor)?;
        // Usually the CSV file inside has the same name as the PID or similar.
        // We'll just take the first file.
        let mut csv_file = archive.by_index(0)?;
        
        // Read into buffer
        let mut buffer = Vec::new();
        csv_file.read_to_end(&mut buffer)?;
        
        // 4. Parse with Polars
        let cursor = Cursor::new(buffer);
        let df = CsvReader::new(cursor)
            .infer_schema(Some(100))
            .has_header(true)
            .finish()?;
            
        Ok(StatCanDataFrame::new(df))
    }
}
