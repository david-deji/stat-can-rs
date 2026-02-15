use polars::prelude::*;
use statcan_rs::{StatCanClient, StatCanDataFrame};

#[tokio::test]
async fn test_fuzzy_filtering() {
    // Create a mock DataFrame simulating a StatCan table
    let s0 = Series::new("REF_DATE", &["2023-01", "2023-01", "2023-02"]);
    let s1 = Series::new("Geography", &["Canada", "Quebec", "Ontario"]);
    let s2 = Series::new(
        "Labour force characteristics",
        &["Population", "Population", "Unemployment"],
    );
    let s3 = Series::new("VALUE", &[100, 50, 5]);

    let df = DataFrame::new(vec![s0, s1, s2, s3]).unwrap();
    let wrapper = StatCanDataFrame::new(df);

    // Test 1: Geo Filter with Case Insensitivity
    let filtered_geo = wrapper.clone().filter_geo("canada").unwrap();
    assert_eq!(filtered_geo.as_polars().height(), 1);
    assert_eq!(
        filtered_geo
            .as_polars()
            .column("Geography")
            .unwrap()
            .get(0)
            .unwrap()
            .to_string()
            .replace("\"", ""),
        "Canada"
    );

    // Test 2: Geo Filter with Partial Match (Mock StatCanDataFrame implementation of filter_geo uses exact match logic but maybe we should verify if I changed it to fuzzy?
    // Wait, filter_geo in wrapper.rs:
    // .filter(col("Geography").str().to_lowercase().eq(lit(value.to_lowercase())))
    // So it is case-insensitive exact match.
    // Let's verify that "canada" matches "Canada".

    let filtered_geo_upper = wrapper.clone().filter_geo("CANADA").unwrap();
    assert_eq!(filtered_geo_upper.as_polars().height(), 1);

    // Test 3: Column Fuzziness
    // "labour" should match "Labour force characteristics" if substring matching is enabled
    // wrapper.rs: resolve_column_name checks: exact, then case-insensitive, then substring.
    // So "labour" -> "Labour force characteristics"

    // Filter value "pop" -> "Population" (No, value filtering is strict equality usually, let's check wrapper.rs)
    // wrapper.rs: filter_column matches value with to_lowercase().eq(lit(value_lower))
    // So value matching is Case-Insensitive Exact.

    let filtered_col = wrapper
        .clone()
        .filter_column("labour", "population")
        .unwrap();
    assert_eq!(filtered_col.as_polars().height(), 2);

    // Test 4: Column partial name match
    // "char" -> "Labour force characteristics"?
    let filtered_col_partial = wrapper
        .clone()
        .filter_column("char", "Unemployment")
        .unwrap();
    assert_eq!(filtered_col_partial.as_polars().height(), 1);
}

#[tokio::test]
async fn test_dimension_search() {
    let client = StatCanClient::new().unwrap();
    // Search for a common dimension like "NAICS" or "Geography"
    // This is an integration test hitting the API

    let limit = 5;
    let result = client.find_cubes_by_dimension("Geography", limit).await;

    match result {
        Ok(cubes) => {
            println!("Found {} cubes with 'Geography' dimension", cubes.len());
            for (pid, title, matching_dims) in &cubes {
                println!("- [{}]: {} (Dims: {})", pid, title, matching_dims);
                assert!(matching_dims.to_lowercase().contains("geography"));
            }
            assert!(cubes.len() > 0);
            assert!(cubes.len() <= limit);
        }
        Err(e) => {
            panic!("Dimension search failed: {}", e);
        }
    }

    // Search for something obscure or specific to ensure it filters
    // "cannabis" might be in title but maybe as a dimension?
    // Let's try "prices" or "index"
}
