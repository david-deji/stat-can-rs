use statcan_rs::StatCanClient;

#[tokio::test]
#[ignore]
async fn test_get_all_cubes_list_lite() {
    let client = StatCanClient::new().unwrap();
    let result = client.get_all_cubes_list_lite().await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status, "SUCCESS");
    assert!(!response.object.unwrap().is_empty());
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

#[tokio::test]
async fn test_fetch_data_by_vector_periods() {
    let client = StatCanClient::new().unwrap();
    // CPI Vector v41690973, get last 2 periods
    let result = client
        .get_data_from_vectors(vec!["v41690973".to_string()], 2)
        .await;

    assert!(result.is_ok(), "Failed to fetch data: {:?}", result.err());
    let response = result.unwrap();
    assert_eq!(response.status, "SUCCESS");

    let points = response.object.unwrap();
    println!("Fetched {} points", points.len());

    // Should get 2 points (one for each period requested)
    // Note: StatCan API behaviour might return more points if multiple vectors, but for 1 vector over 2 periods, we expect 2 points.
    // If the latestN periods span over missing months, it might be fewer, but for CPI it should be there.
    assert!(
        points.len() >= 2,
        "Expected at least 2 points, got {}",
        points.len()
    );

    // Verify they are distinct dates
    let dates: std::collections::HashSet<_> = points.iter().map(|p| &p.ref_date).collect();
    assert_eq!(dates.len(), 2, "Expected 2 distinct dates");
}

#[tokio::test]
#[ignore]
async fn test_find_cubes_by_dimension() {
    let client = StatCanClient::new().unwrap();
    // Use a common dimension like "Geography"
    let result = client.find_cubes_by_dimension("Geography", 5).await;

    assert!(result.is_ok(), "Failed to search cubes by dimension: {:?}", result.err());
    let results = result.unwrap();

    // Check that we got at least some results (and no more than requested)
    assert!(results.len() > 0, "Expected to find at least one cube with the 'Geography' dimension");
    assert!(results.len() <= 5, "Expected no more than 5 results");

    // Check structure
    let (pid, title, matching_dims) = &results[0];
    assert!(!pid.is_empty(), "Product ID should not be empty");
    assert!(!title.is_empty(), "Title should not be empty");
    assert!(matching_dims.to_lowercase().contains("geography"), "Matching dimensions should contain our query");
}
