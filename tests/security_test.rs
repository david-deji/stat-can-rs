use statcan_rs::{download_and_extract_file, StatCanClient, StatCanError};
use reqwest::Client;
use std::fs::File;
use std::io::Write;

#[tokio::test]
async fn test_fetch_full_table_traversal_prevention() {
    let client = StatCanClient::new().unwrap();
    let pid = "../../../../../tmp/pwned";
    let result = client.fetch_full_table(pid).await;

    match result {
        Ok(_) => panic!("Should not succeed"),
        Err(StatCanError::Api(msg)) => {
            if !msg.contains("Invalid PID format") {
                panic!("Expected 'Invalid PID format' error, but got: {}", msg);
            }
        }
        Err(e) => panic!("Unexpected error type: {}", e),
    }
}

#[tokio::test]
async fn test_download_and_extract_file_traversal_prevention() {
    // 1. Create a dummy file outside the expected cache directory
    let temp_file_path = std::env::temp_dir().join("traversal_target.csv");
    let mut file = File::create(&temp_file_path).expect("Failed to create temp file");
    writeln!(file, "dummy,data").expect("Failed to write to temp file");

    // 2. Construct a malicious PID that traverses out of cache to the temp file
    // /tmp/statcan/../../tmp/traversal_target.csv -> /tmp/traversal_target.csv
    let malicious_pid = "../traversal_target";

    // Mock client (won't be used if validation works)
    let client = Client::new();

    // 3. Attempt to "download"
    let result = download_and_extract_file(&client, "http://example.com/ignored", malicious_pid).await;

    // Cleanup
    let _ = std::fs::remove_file(temp_file_path);

    // 4. Assert fix: Returns Error
    match result {
        Ok(path) => {
            panic!("Vulnerability NOT fixed! Access allowed to: {:?}", path);
        },
        Err(e) => {
            // println!("Got expected error: {:?}", e);
            match e {
                StatCanError::Api(msg) => assert_eq!(msg, "Invalid PID format"),
                _ => panic!("Unexpected error type: {:?}", e),
            }
        }
    }
}
