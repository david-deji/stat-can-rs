use statcan_rs::StatCanClient;
use tokio;

#[tokio::test]
#[ignore]
async fn test_get_all_cubes_list_lite() {
    let client = StatCanClient::new().unwrap();
    let result = client.get_all_cubes_list_lite().await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status, "SUCCESS");
    assert!(response.object.unwrap().len() > 0);
}

#[tokio::test]
#[ignore]
async fn test_get_cube_metadata() {
    let client = StatCanClient::new().unwrap();
    // CPI PID: 18100004
    let result = client.get_cube_metadata("18100004").await;
    if let Err(e) = &result {
        println!("Error fetching metadata: {:?}", e);
    }
    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert_eq!(metadata.status, "SUCCESS");
    assert_eq!(metadata.object.unwrap().product_id, "18100004");
}

#[tokio::test]
#[ignore]
async fn test_fetch_full_table() {
    let client = StatCanClient::new().unwrap();
    // Use a smaller table if possible, but CPI is reliable.
    let result = client.fetch_full_table("18100004").await;
    assert!(result.is_ok());
    let df_wrapper = result.unwrap();
    let df = df_wrapper.as_polars();
    assert!(df.height() > 0);
}
