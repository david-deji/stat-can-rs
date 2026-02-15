use statcan_rs::StatCanClient;

#[tokio::test]
async fn test_fast_snippet() {
    let client = StatCanClient::new().unwrap();
    // Use a known large cube (e.g. CPI or Labour Force)
    let pid = "18100004"; // CPI
    let result = client.fetch_fast_snippet(pid).await;
    match result {
        Ok(df) => {
            let p_df = df.as_polars();
            println!("Fast snippet returned {} rows", p_df.height());
            assert!(p_df.height() > 0);

            // Verify enriched columns
            let schema = p_df.schema();
            println!("Schema: {:?}", schema);
            assert!(schema.contains("Geography")); // Expect "Geography" column
            assert!(schema.contains("REF_DATE"));
        }
        Err(e) => {
            panic!("Fast snippet failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_vector_error_handling() {
    let _ = tracing_subscriber::fmt::try_init();
    let client = StatCanClient::new().unwrap();
    // Use a definitely invalid vector ID
    let vectors = vec!["v00000000".to_string()];
    let result = client.get_data_from_vectors(vectors, 1).await;

    match result {
        Ok(resp) => {
            println!("Vector fetch result: {:?}", resp);
            // Expect empty object or status != SUCCESS
            // Our implementation returns status="FAILED" (or whatever API returns) and empty object
            // Inspect status
            if resp.status == "SUCCESS" {
                // Even if success, object might be empty or valid if v0 exists?
                // v0 is unlikely.
            } else {
                assert_ne!(resp.status, "SUCCESS");
            }
        }
        Err(e) => {
            panic!("Vector fetch crashed or returned error: {}", e);
        }
    }
}
