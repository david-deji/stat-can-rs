use statcan_rs::StatCanClient;

#[tokio::test]
async fn test_path_traversal_prevention() {
    let client = StatCanClient::new().unwrap();
    let pid = "../../../../../tmp/pwned";
    let result = client.fetch_full_table(pid).await;

    match result {
        Ok(_) => panic!("Should not succeed"),
        Err(statcan_rs::StatCanError::Api(msg)) => {
            if !msg.contains("Invalid PID format") {
                panic!("Expected 'Invalid PID format' error, but got: {}", msg);
            }
        }
        Err(e) => panic!("Unexpected error type: {}", e),
    }
}

#[tokio::test]
async fn test_download_and_extract_file_path_traversal() {
    // This test verifies that `download_and_extract_file` validates the PID/Resource ID
    // before attempting any network or file operations, preventing path traversal attacks.
    let client = reqwest::Client::new();
    let url = "https://example.com/malicious.zip";
    let pid = "../../../../../tmp/pwned";

    let result = statcan_rs::download_and_extract_file(&client, url, pid).await;

    match result {
        Ok(_) => panic!("Should not succeed - path traversal must be blocked"),
        Err(statcan_rs::StatCanError::Api(msg)) => {
            if !msg.contains("Invalid PID format") {
                panic!("Expected 'Invalid PID format' error, but got: {}", msg);
            }
        }
        Err(e) => {
             panic!("Expected 'Invalid PID format' error, but got other error: {}", e);
        }
    }
}
