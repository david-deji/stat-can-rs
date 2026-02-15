use chrono::{Datelike, Utc};
use polars::prelude::*;
use statcan_rs::StatCanClient;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Initializing StatCan Client...");
    let client = StatCanClient::new()?;

    // 1. Fetch and Parse List of Cubes
    println!("\n--- 1. Available Cubes (First 5) ---");
    let cubes_resp = client.get_all_cubes_list_lite().await?;
    if let Some(cubes) = cubes_resp.object {
        println!("Total tables available: {}", cubes.len());
        for cube in cubes.iter().take(5) {
            println!("PID: {}, Title: {}", cube.product_id, cube.cube_title_en);
        }
    }

    // 2. CPI by Province (Last 5 Years)
    println!("\n--- 2. CPI by Province (Last 5 Years) ---");
    analyze_cpi(&client).await?;

    // 3. Unemployment by Province (Last 5 Years)
    println!("\n--- 3. Unemployment Rate by Province (Last 5 Years) ---");
    analyze_unemployment(&client).await?;

    Ok(())
}

async fn analyze_cpi(client: &StatCanClient) -> Result<(), Box<dyn Error>> {
    let pid = "18100004"; // Consumer Price Index
    println!("Fetching CPI table ({}) ...", pid);
    let df_wrapper = client.fetch_full_table(pid).await?;

    // Filter for last 5 years
    let current_year = Utc::now().year();
    let start_year = current_year - 5;
    let end_year = current_year;

    // Use ergonomic wrapper methods
    let df_filtered = df_wrapper
        .filter_date_range(start_year, end_year)?
        .filter_column("Products and product groups", "All-items")?
        //.filter_geo("Province")? // "Province" might be case sensitive or not present in all rows as expected
        .into_polars();

    // Further filtering
    let df_final = df_filtered
        .lazy()
        .filter(col("GEO").neq(lit("Canada")))
        .filter(col("GEO").str().contains_literal(lit("Province")).not()) // Actually we want to KEEP provinces?
        // Wait, the original logic was: exclude Canada AND exclude "Province of..." if it's redundant?
        // Or maybe we want to keep rows where GEO is a province name.
        // Let's look at unique GEOs: "Newfoundland and Labrador", "Prince Edward Island", etc.
        // They don't have "Province" in the name usually.
        // The original code had `.and(col("GEO").str().contains(lit("Province"), false).not())`
        // This implies we wanted to REMOVE things with "Province" in the name?
        // Let's just filter out "Canada" and keep everything else for now to see what we get.
        .select([col("parsed_date").alias("date"), col("GEO"), col("VALUE")])
        .collect()?;

    println!("CPI Data (Head):");
    println!("{:?}", df_final.head(Some(5)));

    // Group by GEO and calculate average CPI over the period
    let avg_cpi = df_final
        .lazy()
        .group_by([col("GEO")])
        .agg([col("VALUE").mean().alias("avg_cpi_5y")])
        .sort("avg_cpi_5y", SortOptions::default())
        .collect()?;

    println!("Average CPI by Province (Last 5 Years):");
    println!("{:?}", avg_cpi);

    Ok(())
}

async fn analyze_unemployment(client: &StatCanClient) -> Result<(), Box<dyn Error>> {
    let pid = "14100287"; // Labour force characteristics
    println!("Fetching Unemployment table ({}) ...", pid);
    let df_wrapper = client.fetch_full_table(pid).await?;

    // Inspect columns easily
    // df_wrapper.inspect_column("Labour force characteristics")?;

    let current_year = Utc::now().year();
    let start_year = current_year - 5;
    let end_year = current_year;

    let df_filtered = df_wrapper
        .filter_date_range(start_year, end_year)?
        .filter_column("Labour force characteristics", "Unemployment rate")?
        .filter_column("Age group", "15 years and over")?
        .filter_column("Statistics", "Estimate")?
        // Use the custom filter for Gender which requires partial match
        //.filter_geo("Canada")? // We want to EXCLUDE Canada, but let's filter broadly first or just use Polars for specific exclusion
        .into_polars();

    // Specific filtering that wrapper might not cover perfectly yet (like "NOT Canada" or partial match on Gender)
    let df_final = df_filtered
        .lazy()
        .filter(col("GEO").neq(lit("Canada")))
        .filter(col("Gender").str().contains_literal(lit("Total"))) // "Total - Gender"
        .select([
            col("parsed_date").alias("date"),
            col("GEO"),
            col("VALUE").cast(DataType::Float64),
        ])
        .collect()?;

    println!("Unemployment Data (Head):");
    println!("{:?}", df_final.head(Some(5)));

    // Show latest unemployment rate by province
    let latest_unemployment = df_final
        .lazy()
        .sort_by_exprs(vec![col("date")], vec![true], true, false) // Sort by date descending
        .group_by([col("GEO")])
        .agg([
            col("VALUE").first().alias("latest_rate"),
            col("date").first().alias("ref_date"),
        ])
        .sort("latest_rate", SortOptions::default())
        .collect()?;

    println!("Latest Unemployment Rate by Province:");
    println!("{:?}", latest_unemployment);

    Ok(())
}
