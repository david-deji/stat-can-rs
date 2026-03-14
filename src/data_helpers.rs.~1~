use crate::CKANClient;
use encoding_rs::{UTF_16BE, UTF_16LE};
use polars::prelude::*;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

/// Ensure file is valid UTF-8, transcoding from UTF-16 if necessary.
/// Returns the path to the UTF-8 file (which may be a new temp file or the original).
pub async fn ensure_utf8_encoding(path: &PathBuf) -> Result<PathBuf, String> {
    // 1. Read first chunk to detect BOM/Encoding
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let mut buffer = [0u8; 4096];
    let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
    let slice = &buffer[..n];

    // Check BOMs
    let has_utf16le_bom = slice.starts_with(&[0xFF, 0xFE]);
    let has_utf16be_bom = slice.starts_with(&[0xFE, 0xFF]);

    // Heuristic: check for null bytes pattern
    // In UTF-16 (English text), every other byte is likely 0.
    // BE: 00 'H' 00 'e' ... -> Even indices are 0
    // LE: 'H' 00 'e' 00 ... -> Odd indices are 0

    let even_nulls = slice.iter().step_by(2).filter(|&&b| b == 0).count();
    let odd_nulls = slice.iter().skip(1).step_by(2).filter(|&&b| b == 0).count();
    let total_chars = n / 2;

    // Strong signal: if > 80% of even bytes are null, it's likely BE.
    let heuristic_be = n > 10 && (even_nulls > (total_chars * 4 / 5));
    // Strong signal: if > 80% of odd bytes are null, it's likely LE.
    let heuristic_le = n > 10 && (odd_nulls > (total_chars * 4 / 5));

    if has_utf16le_bom || has_utf16be_bom || heuristic_be || heuristic_le {
        let is_be = has_utf16be_bom || heuristic_be;

        info!(
            "Detected UTF-16 encoding (BOM_LE={}, BOM_BE={}, Heuristic_LE={}, Heuristic_BE={}), transcoding to UTF-8...",
            has_utf16le_bom, has_utf16be_bom, heuristic_le, heuristic_be
        );

        // Read entire file
        let content = std::fs::read(path).map_err(|e| e.to_string())?;

        // Decode
        let (cow, _, had_errors) = if is_be {
            UTF_16BE.decode(&content)
        } else {
            UTF_16LE.decode(&content)
        };

        if had_errors {
            warn!("UTF-16 decoding had errors, output may be corrupted");
        }

        // Write to new temp file
        let new_path = path.with_extension("utf8.csv");
        let mut out = File::create(&new_path).map_err(|e| e.to_string())?;

        // Write standard UTF-8 BOM just in case (optional, but helps some readers)
        // out.write_all(&[0xEF, 0xBB, 0xBF]).map_err(|e| e.to_string())?;

        out.write_all(cow.as_bytes()).map_err(|e| e.to_string())?;

        return Ok(new_path);
    }

    Ok(path.clone())
}

/// Helper to download, transcode, and load a DataFrame from a resource ID.
/// Returns the DataFrame and the path to the temp file (so caller can delete it).
pub async fn fetch_resource_as_df<C: CKANClient>(
    client: Arc<C>,
    resource_id: &str,
) -> Result<(DataFrame, PathBuf), String> {
    // 1. Get Handler
    let handler = client
        .get_resource_handler(resource_id)
        .await
        .map_err(|e| e.to_string())?;

    let download_url = match handler {
        crate::DataHandler::BlobDownload(url) => url,
        crate::DataHandler::DatastoreQuery(_, Some(url)) => url,
        crate::DataHandler::DatastoreQuery(_, None) => {
            return Err("Resource has no download URL available".to_string());
        }
    };

    info!("Downloading Open Data CSV from: {}", download_url);

    // 2. HTTP Client
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .gzip(true)
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {}", e))?;

    // 3. Download
    let temp_path = crate::download_and_extract_file(&http_client, &download_url, resource_id)
        .await
        .map_err(|e| format!("Download failed: {}", e))?;

    // 4. Transcode
    let temp_path = ensure_utf8_encoding(&temp_path)
        .await
        .map_err(|e| format!("Encoding check failed: {}", e))?;

    // 5. Detect Separator
    let separator = {
        let mut file = File::open(&temp_path).map_err(|e| e.to_string())?;
        let mut buffer = [0u8; 4096];
        let n = file.read(&mut buffer).unwrap_or(0);
        let slice = &buffer[..n];

        let commas = slice.iter().filter(|&&c| c == b',').count();
        let tabs = slice.iter().filter(|&&c| c == b'\t').count();
        if tabs > commas {
            b'\t'
        } else {
            b','
        }
    };

    // 6. Load DataFrame
    let temp_path_clone = temp_path.clone();
    let df = tokio::task::spawn_blocking(move || -> Result<DataFrame, String> {
        CsvReader::from_path(&temp_path_clone)
            .map_err(|e| format!("Failed to open CSV: {}", e))?
            .infer_schema(Some(100))
            .has_header(true)
            .with_separator(separator)
            .with_ignore_errors(true)
            .truncate_ragged_lines(true)
            .finish()
            .map_err(|e| format!("Polars parse error: {}", e))
    })
    .await
    .map_err(|e| e.to_string())??;

    Ok((df, temp_path))
}
