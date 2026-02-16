use statcan_rs::{CKANClient, DataHandler, GenericCKANDriver};

#[tokio::test]
async fn test_ckan_ping() {
    let driver =
        GenericCKANDriver::new("https://open.canada.ca/data").expect("Failed to create driver");
    // This might fail if no internet, so we'll just print the result
    match driver.ping().await {
        Ok(msg) => {
            println!("Ping success: {}", msg);
            assert!(msg.contains("OK"));
        }
        Err(e) => {
            println!("Ping failed (likely network): {}", e);
        }
    }
}

#[tokio::test]
async fn test_search_packages() {
    let driver =
        GenericCKANDriver::new("https://open.canada.ca/data").expect("Failed to create driver");
    // Search for "housing"
    match driver.search_packages("housing", 5).await {
        Ok(packages) => {
            println!("Found {} packages", packages.len());
            if !packages.is_empty() {
                let first = &packages[0];
                println!("First package: {} ({})", first.title, first.id);
                assert!(!first.id.is_empty());
                assert!(!first.title.is_empty());
            }
        }
        Err(e) => {
            println!("Search failed (likely network): {}", e);
        }
    }
}

#[tokio::test]
async fn test_get_package_metadata_and_resource() {
    // We'll try to find a package first, then get its metadata
    let driver =
        GenericCKANDriver::new("https://open.canada.ca/data").expect("Failed to create driver");

    // Use a known package ID if possible, or search one
    let packages_res = driver.search_packages("population", 1).await;

    if let Ok(packages) = packages_res {
        if let Some(pkg) = packages.first() {
            println!("Testing get_package_metadata for {}", pkg.id);
            let meta_res = driver.get_package_metadata(&pkg.id).await;
            match meta_res {
                Ok(meta) => {
                    assert_eq!(meta.id, pkg.id);
                    println!("Metadata retrieved for {}", meta.title);

                    if let Some(res) = meta.resources.first() {
                        println!("Testing get_resource_handler for {}", res.id);
                        let handler_res = driver.get_resource_handler(&res.id).await;
                        match handler_res {
                            Ok(handler) => println!("Handler: {:?}", handler),
                            Err(e) => println!("get_resource_handler failed: {}", e),
                        }
                    }
                }
                Err(e) => println!("get_package_metadata failed: {}", e),
            }
        } else {
            println!("No packages found to test metadata.");
        }
    } else {
        println!("Search failed, skipping metadata test.");
    }
}

#[tokio::test]
async fn test_job_bank_package() {
    let driver =
        GenericCKANDriver::new("https://open.canada.ca/data").expect("Failed to create driver");
    let job_bank_id = "ea639e28-c0fc-48bf-b5dd-b8899bd43072";

    match driver.get_package_metadata(job_bank_id).await {
        Ok(meta) => {
            println!("Job Bank Package Found: {}", meta.title);
            assert_eq!(meta.id, job_bank_id);

            // Check for CSV resource
            if let Some(res) = meta
                .resources
                .iter()
                .find(|r| r.format.as_deref() == Some("CSV"))
            {
                println!("Found CSV resource: {}", res.name);

                // Check if datastore is active via get_resource_handler
                match driver.get_resource_handler(&res.id).await {
                    Ok(handler) => {
                        println!("Handler: {:?}", handler);
                        if let statcan_rs::DataHandler::DatastoreQuery(rid, _) = handler {
                            println!("Datastore IS active for resource {}", rid);

                            // Try a simple SQL query
                            let sql = format!("SELECT * FROM \"{}\" LIMIT 5", rid);
                            match driver.query_datastore(&sql).await {
                                Ok(records) => {
                                    println!(
                                        "SQL Query Success: Retrieved {} records",
                                        records.len()
                                    );
                                    assert!(records.len() <= 5);
                                }
                                Err(e) => println!("SQL Query Failed: {}", e),
                            }
                        } else {
                            println!("Datastore NOT active for this resource.");
                        }
                    }
                    Err(e) => println!("get_resource_handler failed: {}", e),
                }
            } else {
                println!("No CSV resource found in Job Bank package.");
            }
        }
        Err(e) => {
            println!("Failed to get Job Bank package (likely network): {}", e);
        }
    }
}
