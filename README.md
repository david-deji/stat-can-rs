# StatCan RS

A Rust client and **Model Context Protocol (MCP) Server** for the Statistics Canada API.

## 🤖 MCP Server (AI Integration)
This project includes a fully featured MCP server that allows AI agents to search, filter, and retrieve Statistics Canada data.

**Key Features:**
- **Smart Filtering**: Filter by Geography and Date.
- **High Performance**: Streaming downloads and local caching.
- **Dual Mode**: Support for Stdio and HTTP/SSE.

👉 **[Read the MCP User Guide](USER_GUIDE.md)** for setup and usage instructions.

## Library Features
A high-performance, async Rust client for the [Statistics Canada Web Data Service (WDS)](https://www.statcan.gc.ca/eng/developers/wds).

Designed for data engineers and analysts, `statcan-rs` provides a strongly-typed interface to search for data cubes, fetch metadata, and—most importantly—download full datasets directly into [Polars](https://pola.rs/) DataFrames for immediate analysis.

## Features

- 🚀 **Async & Fast**: Built on `tokio` and `reqwest`.
- 🐻 **Polars Integration**: "Killer Feature" - Download full tables (ZIP/CSV) directly into a `Polars DataFrame` in memory. No intermediate files required.
- 📦 **Strongly Typed**: Comprehensive structs for Cube metadata, Dimensions, and Members.
- 🛠️ **Easy API**: Simple methods for `getAllCubesListLite`, `getCubeMetadata`, and `getDataFromCubePid`.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
statcan-rs = { path = "." } # Or git url
tokio = { version = "1.0", features = ["full"] }
polars = { version = "0.36", features = ["lazy", "csv", "strings", "temporal", "dtype-date"] }
```

## Usage

### Basic Metadata Fetching

```rust
use statcan_rs::StatCanClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = StatCanClient::new()?;

    // List all available tables
    let cubes = client.get_all_cubes_list_lite().await?;
    println!("Found {} tables", cubes.object.unwrap().len());

    // Get metadata for a specific table (e.g., CPI: 18100004)
    let metadata = client.get_cube_metadata("18100004").await?;
    println!("Title: {}", metadata.object.unwrap().cube_title_en);

    Ok(())
}
```

### ⚡ Bulk Download to Polars DataFrame

Download an entire dataset (potentially millions of rows) and load it into Polars in one go.

```rust
use statcan_rs::StatCanClient;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = StatCanClient::new()?;

    // Fetch Consumer Price Index (18100004)
    // This downloads the ZIP, extracts the CSV in RAM, and parses it.
    let df = client.fetch_full_table("18100004").await?;

    println!("{:?}", df.head(Some(5)));
    
    // Perform analysis immediately
    let df_filtered = df.lazy()
        .filter(col("GEO").eq(lit("Canada")))
        .collect()?;
        
    println!("{:?}", df_filtered);

    Ok(())
}
```

## API Coverage

| Method | Description |
|--------|-------------|
| `get_all_cubes_list_lite` | Lists all available data cubes. |
| `get_cube_metadata` | Details about dimensions, members, and frequency. |
| `get_data_from_cube_pid` | Fetch specific data points by coordinate. |
| `get_full_cube_from_cube_pid` | Get the download URL for the full table (CSV). |
| `fetch_full_table` | **Helper**: Downloads and parses full table to DataFrame. |

## License

MIT
