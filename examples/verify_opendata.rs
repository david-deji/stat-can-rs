use statcan_rs::{CKANClient, GenericCKANDriver};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = GenericCKANDriver::new("https://open.canada.ca/data/en")?;
    println!("Pinging Open Data Canada...");
    match client.ping().await {
        Ok(msg) => println!("Ping result: {}", msg),
        Err(e) => println!("Ping failed: {}", e),
    }

    println!("\nSearching for 'unemployment'...");
    match client.search_packages("unemployment", 5).await {
        Ok(packages) => {
            for p in packages {
                println!("- {} (ID: {})", p.title, p.id);
                for r in p.resources {
                    println!("  * Resource: {} (Format: {:?})", r.name, r.format);
                }
            }
        }
        Err(e) => println!("Search failed: {}", e),
    }

    Ok(())
}
