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
