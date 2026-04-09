use crate::CKANClient;
use encoding_rs::{UTF_16BE, UTF_16LE};
use polars::prelude::*;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn};

/// Ensure file is valid UTF-8, transcoding from UTF-16 if necessary.
/// Returns the path to the UTF-8 file (which may be a new temp file or the original).
pub async fn ensure_utf8_encoding(path: &PathBuf) -> Result<PathBuf, String> {
    // 1. Read first chunk to detect BOM/Encoding
    let mut file = tokio::fs::File::open(path).await.map_err(|e| e.to_string())?;
    let mut buffer = [0u8; 4096];
    let n = file.read(&mut buffer).await.map_err(|e| e.to_string())?;
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
        let content = tokio::fs::read(path).await.map_err(|e| e.to_string())?;

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
        let mut out = tokio::fs::File::create(&new_path)
            .await
            .map_err(|e| e.to_string())?;

        // Write standard UTF-8 BOM just in case (optional, but helps some readers)
        // out.write_all(&[0xEF, 0xBB, 0xBF]).await.map_err(|e| e.to_string())?;

        out.write_all(cow.as_bytes())
            .await
            .map_err(|e| e.to_string())?;

        return Ok(new_path);
    }

    Ok(path.clone())
}

use crate::ResourceMetadata;

/// Selects the best resource from a list of resources based on heuristics.
pub fn select_best_resource(resources: &[ResourceMetadata]) -> Option<&ResourceMetadata> {
    if resources.is_empty() {
        return None;
    }

    resources.iter().max_by(|a, b| {
        let score_a = score_resource(a);
        let score_b = score_resource(b);
        score_a.cmp(&score_b)
    })
}

fn score_resource(resource: &ResourceMetadata) -> i32 {
    let mut score = 0;

    // Rank formats
    if let Some(ref format) = resource.format {
        let format_upper = format.to_uppercase();
        if format_upper == "CSV" || format_upper == "PARQUET" {
            score += 100;
        } else if format_upper == "JSON" {
            score += 50;
        }
    }

    // Heuristics based on name
    let name_lower = resource.name.to_lowercase();
    if name_lower.contains("data") || name_lower.contains("table") {
        score += 20;
    }

    // Check datastore_active
    if let Some(true) = resource.datastore_active {
        score += 30;
    }

    score
}

/// Helper function to perform fuzzy matching on cube titles.
/// It returns a score. Exact matches score highest, then all-terms matches, then fuzzy matches.
pub fn score_cube_title_match(title: &str, query: &str) -> f64 {
    let title_lower = title.to_lowercase();
    let query_lower = query.to_lowercase();

    let is_exact = title_lower.contains(&query_lower);

    let terms: Vec<String> = query_lower
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    let has_all_terms = terms.iter().all(|term| title_lower.contains(term));

    let similarity = strsim::jaro_winkler(&title_lower, &query_lower);

    let mut score = similarity;
    if is_exact {
        score += 2.0;
    } else if has_all_terms {
        score += 1.0;
    }

    score
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
        let mut file = tokio::fs::File::open(&temp_path)
            .await
            .map_err(|e| e.to_string())?;
        let mut buffer = [0u8; 4096];
        let n = file.read(&mut buffer).await.map_err(|e| e.to_string())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CKANClient;
    use crate::DataHandler;
    use std::io::Write;

    // Returns a TempPath to ensure cleanup when it goes out of scope,
    // and the PathBuf for calling the function.
    fn create_temp_encoded_file(content: &[u8], extension: &str) -> (tempfile::TempPath, PathBuf) {
        let mut temp_file = tempfile::Builder::new()
            .suffix(extension)
            .tempfile()
            .expect("Failed to create temp file");
        temp_file
            .write_all(content)
            .expect("Failed to write to temp file");
        let temp_path = temp_file.into_temp_path();
        let path_buf = temp_path.to_path_buf();
        (temp_path, path_buf)
    }
    use crate::PackageMetadata;
    use crate::ResourceMetadata;
    use async_trait::async_trait;

    struct MockNoUrlClient;

    #[async_trait]
    impl CKANClient for MockNoUrlClient {
        async fn ping(&self) -> crate::Result<String> {
            Ok("pong".to_string())
        }
        async fn search_packages(
            &self,
            _query: &str,
            _limit: usize,
        ) -> crate::Result<Vec<PackageMetadata>> {
            Ok(vec![])
        }
        async fn get_package_metadata(&self, id: &str) -> crate::Result<PackageMetadata> {
            Ok(PackageMetadata {
                id: id.to_string(),
                title: "Test Package".to_string(),
                notes: None,
                url: None,
                resources: vec![],
            })
        }
        async fn get_resource_handler(&self, resource_id: &str) -> crate::Result<DataHandler> {
            Ok(DataHandler::DatastoreQuery(resource_id.to_string(), None))
        }
        async fn query_datastore(&self, _sql: &str) -> crate::Result<Vec<serde_json::Value>> {
            Ok(vec![])
        }
        async fn get_resource_schema(
            &self,
            _resource_id: &str,
        ) -> crate::Result<Vec<(String, String)>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_ensure_utf8_encoding_utf16_le_bom() {
        let utf8_text = "id,name\n1,Test\n";
        let mut content = vec![0xFF, 0xFE]; // UTF-16LE BOM
                                            // encode utf8_text as UTF-16LE
        for codepoint in utf8_text.encode_utf16() {
            content.push((codepoint & 0xFF) as u8);
            content.push((codepoint >> 8) as u8);
        }
        let (_temp_guard, path) = create_temp_encoded_file(&content, ".csv");

        let result = ensure_utf8_encoding(&path).await.unwrap();

        assert_ne!(result, path);
        assert_eq!(result.extension().unwrap(), "csv");
        let read_content = std::fs::read(&result).unwrap();
        assert_eq!(String::from_utf8(read_content).unwrap(), utf8_text);

        let _ = std::fs::remove_file(result);
    }

    #[tokio::test]
    async fn test_ensure_utf8_encoding_utf16_be_bom() {
        let utf8_text = "id,name\n1,Test\n";
        let mut content = vec![0xFE, 0xFF]; // UTF-16BE BOM
                                            // encode utf8_text as UTF-16BE
        for codepoint in utf8_text.encode_utf16() {
            content.push((codepoint >> 8) as u8);
            content.push((codepoint & 0xFF) as u8);
        }
        let (_temp_guard, path) = create_temp_encoded_file(&content, ".csv");

        let result = ensure_utf8_encoding(&path).await.unwrap();

        assert_ne!(result, path);
        let read_content = std::fs::read(&result).unwrap();
        assert_eq!(String::from_utf8(read_content).unwrap(), utf8_text);

        let _ = std::fs::remove_file(result);
    }

    #[tokio::test]
    async fn test_ensure_utf8_encoding_utf16_le_heuristic() {
        // Need string long enough to trigger the > 10 bytes and > 80% of odd bytes being nulls heuristic
        let utf8_text = "id,name\n1,Test\n2,Another\n3,Third\n";
        let mut content = vec![];
        // encode utf8_text as UTF-16LE without BOM
        for codepoint in utf8_text.encode_utf16() {
            content.push((codepoint & 0xFF) as u8);
            content.push((codepoint >> 8) as u8);
        }
        let (_temp_guard, path) = create_temp_encoded_file(&content, ".csv");

        let result = ensure_utf8_encoding(&path).await.unwrap();

        assert_ne!(result, path);
        let read_content = std::fs::read(&result).unwrap();
        assert_eq!(String::from_utf8(read_content).unwrap(), utf8_text);

        let _ = std::fs::remove_file(result);
    }

    #[tokio::test]
    async fn test_ensure_utf8_encoding_utf16_be_heuristic() {
        // Need string long enough to trigger the > 10 bytes and > 80% of even bytes being nulls heuristic
        let utf8_text = "id,name\n1,Test\n2,Another\n3,Third\n";
        let mut content = vec![];
        // encode utf8_text as UTF-16BE without BOM
        for codepoint in utf8_text.encode_utf16() {
            content.push((codepoint >> 8) as u8);
            content.push((codepoint & 0xFF) as u8);
        }
        let (_temp_guard, path) = create_temp_encoded_file(&content, ".csv");

        let result = ensure_utf8_encoding(&path).await.unwrap();

        assert_ne!(result, path);
        let read_content = std::fs::read(&result).unwrap();
        assert_eq!(String::from_utf8(read_content).unwrap(), utf8_text);

        let _ = std::fs::remove_file(result);
    }

    #[tokio::test]
    async fn test_ensure_utf8_encoding_utf8_no_bom() {
        let content = b"id,name\n1,Test\n";
        let (_temp_guard, path) = create_temp_encoded_file(content, ".csv");

        let result = ensure_utf8_encoding(&path).await.unwrap();

        assert_eq!(result, path);
        let read_content = std::fs::read(&result).unwrap();
        assert_eq!(read_content, content);
    }

    #[test]
    fn test_select_best_resource_empty() {
        let resources: Vec<ResourceMetadata> = vec![];
        assert!(select_best_resource(&resources).is_none());
    }

    #[test]
    fn test_select_best_resource_format_ranking() {
        let resources = vec![
            ResourceMetadata {
                id: "1".to_string(),
                name: "JSON Data".to_string(),
                format: Some("JSON".to_string()),
                url: None,
                datastore_active: None,
            },
            ResourceMetadata {
                id: "2".to_string(),
                name: "CSV Data".to_string(),
                format: Some("CSV".to_string()),
                url: None,
                datastore_active: None,
            },
            ResourceMetadata {
                id: "3".to_string(),
                name: "TXT Data".to_string(),
                format: Some("TXT".to_string()),
                url: None,
                datastore_active: None,
            },
        ];

        let best = select_best_resource(&resources).unwrap();
        assert_eq!(best.id, "2"); // CSV preferred over JSON and TXT
    }

    #[test]
    fn test_select_best_resource_name_heuristic() {
        let resources = vec![
            ResourceMetadata {
                id: "1".to_string(),
                name: "Something Else CSV".to_string(),
                format: Some("CSV".to_string()),
                url: None,
                datastore_active: None,
            },
            ResourceMetadata {
                id: "2".to_string(),
                name: "Important Table CSV".to_string(),
                format: Some("CSV".to_string()),
                url: None,
                datastore_active: None,
            },
        ];

        let best = select_best_resource(&resources).unwrap();
        assert_eq!(best.id, "2"); // "Table" gets a boost
    }

    #[test]
    fn test_select_best_resource_datastore_active() {
        let resources = vec![
            ResourceMetadata {
                id: "1".to_string(),
                name: "Data CSV".to_string(),
                format: Some("CSV".to_string()),
                url: None,
                datastore_active: Some(false),
            },
            ResourceMetadata {
                id: "2".to_string(),
                name: "Data CSV".to_string(),
                format: Some("CSV".to_string()),
                url: None,
                datastore_active: Some(true),
            },
        ];

        let best = select_best_resource(&resources).unwrap();
        assert_eq!(best.id, "2"); // datastore_active=true gets a boost
    }

    #[test]
    fn test_score_cube_title_match() {
        // Exact substring match + jaro winkler
        let exact_score =
            score_cube_title_match("Labour force characteristics by province", "Labour force");
        assert!(exact_score > 2.0); // 2.0 (exact) + similarity

        // All terms match + jaro winkler
        let terms_score =
            score_cube_title_match("Labour force characteristics by province", "force labour");
        assert!(terms_score > 1.0); // 1.0 (all terms) + similarity
        assert!(terms_score < 2.0);

        // Fuzzy match
        let fuzzy_score =
            score_cube_title_match("Labour force characteristics by province", "Labor forc");
        assert!(fuzzy_score > 0.0);
        assert!(fuzzy_score < 1.0);
    }

    #[tokio::test]
    async fn test_fetch_resource_as_df_no_url() {
        let client = std::sync::Arc::new(MockNoUrlClient);
        let result = fetch_resource_as_df(client, "test-id").await;
        assert_eq!(
            result.unwrap_err(),
            "Resource has no download URL available"
        );
    }
}
