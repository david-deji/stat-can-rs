use crate::{CKANClient, StatCanClientTrait, StatCanError};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

// --- Protocol Types ---

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(rename = "jsonrpc")]
    pub _jsonrpc: String,
    pub method: String,
    pub params: Option<Value>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

impl JsonRpcResponse {
    pub fn from_result(result: Result<Value, JsonRpcError>, id: Option<Value>) -> Self {
        match result {
            Ok(res) => Self {
                jsonrpc: "2.0".to_string(),
                result: Some(res),
                error: None,
                id,
            },
            Err(err) => Self {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(err),
                id,
            },
        }
    }
}

impl JsonRpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<StatCanError> for JsonRpcError {
    fn from(e: StatCanError) -> Self {
        match e {
            StatCanError::TableNotFound => JsonRpcError::new(-32000, "Table not found"),
            StatCanError::Network(err) => {
                error!("Network error: {}", err);
                JsonRpcError::new(-32000, format!("Network/Timeout error: {}", err))
            }
            StatCanError::Api(ref msg)
                if msg == "Invalid PID format" || msg == "PID cannot be empty" =>
            {
                JsonRpcError::new(-32602, msg.clone())
            }
            StatCanError::Api(msg) => {
                error!("StatCan API error: {}", msg);
                JsonRpcError::new(-32000, msg)
            }
            e => {
                error!("Internal error: {:?}", e);
                JsonRpcError::new(-32000, "Internal server error")
            }
        }
    }
}

// --- Core Logic ---

pub fn list_tools() -> Result<Value, JsonRpcError> {
    Ok(json!({
        "tools": [
            {
                "name": "list_cubes",
                "description": "List all available data cubes (summary)",
                "inputSchema": { "type": "object", "properties": {} }
            },
            {
                "name": "get_metadata",
                "description": "Get metadata for a specific cube",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "pid": { "type": "string", "description": "The Product ID of the cube (e.g. 18100004)" }
                    },
                    "required": ["pid"]
                }
            },
            {
                "name": "get_cube_dimensions",
                "description": "Get valid dimensions and members for a cube. Use this to find what 'Geography' or 'Products' filters are available.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "pid": { "type": "string", "description": "The Product ID" },
                        "member_query": { "type": "string", "description": "Optional text to filter member names" }
                    },
                    "required": ["pid"]
                }
            },
            {
                "name": "search_cubes",
                "description": "Search for cubes by title (supports multi-word queries)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "Search query (e.g. 'labour ontario')" }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "fetch_data_by_vector",
                "description": "Fetch specific data points by Vector ID (e.g. v123456). Most precise method.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "vectors": { "type": "array", "items": { "type": "string" }, "description": "List of Vector IDs" },
                        "recent_periods": { "type": "integer", "description": "Number of recent periods to fetch (default: 1)" }
                    },
                    "required": ["vectors"]
                }
            },
            {
                "name": "fetch_data_by_coords",
                "description": "Fetch specific data points by Coordinate string (e.g. '1.1.1.1.1').",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "pid": { "type": "string", "description": "The Product ID" },
                        "coords": { "type": "array", "items": { "type": "string" }, "description": "List of Coordinate strings" },
                        "recent_periods": { "type": "integer", "description": "Number of recent periods to fetch (default: 1)" }
                    },
                    "required": ["pid", "coords"]
                }
            },
            {
                "name": "search_cubes_by_dimension",
                "description": "Find cubes that contain a specific dimension name (e.g. 'Geography', 'NAICS'). Useful for finding relevant data sets when you know the dimension you need.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dimension_name": { "type": "string", "description": "Dimension name to search for (case-insensitive substring)" },
                        "limit": { "type": "integer", "description": "Max number of cubes to return (default 10)" }
                    },
                    "required": ["dimension_name"]
                }
            },
            {
                "name": "fetch_data_snippet",
                "description": "Fetch data from a cube. Supports filtering by Geography and getting the most recent N periods. Results are sorted most-recent-first by default. Filters use exact match first, falling back to substring.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "pid": { "type": "string", "description": "The Product ID" },
                        "rows": { "type": "integer", "description": "Number of rows to return (default 5). Results are always sorted most-recent-first." },
                        "geo": { "type": "string", "description": "Filter by Geography (e.g. 'Canada', 'British Columbia')" },
                        "recent_months": { "type": "integer", "description": "Get ALL rows for the last N time periods. Returns every row (all geographies, industries, etc.) matching those periods." },
                        "filters": { "type": "object", "properties": {}, "additionalProperties": { "type": "string" }, "description": "Key-value pairs for column filtering. Uses exact match first, then substring fallback (e.g. {'Products and product groups': 'Energy'} matches 'Energy' exactly, not 'All-items excluding energy')" }
                    },
                    "required": ["pid"]
                }
            },
            {
                "name": "search_open_data",
                "description": "Search the Canadian Open Government portal for datasets.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "Search query (keywords)" },
                        "limit": { "type": "integer", "description": "Max results (default 10)" }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "get_open_data_metadata",
                "description": "Get detailed metadata for a specific dataset from the Open Government portal.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string", "description": "The Dataset (Package) ID" }
                    },
                    "required": ["id"]
                }
            },
            {
                "name": "query_open_data_datastore",
                "description": "Execute an SQL query against the Canadian Open Government Datastore (for datasets with 'datastore_active'=true).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string", "description": "The SQL query to execute (e.g. 'SELECT * FROM \"resource_id\" LIMIT 5'). Note: Table names must be the Resource ID in double quotes." }
                    },
                    "required": ["sql"]
                }
            },
            {
                "name": "fetch_open_data_resource_snippet",
                "description": "Fetch a small snippet of data from an Open Government resource (CSV) for previewing. Supports SQL queries and column selection.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "resource_id": { "type": "string", "description": "The Resource ID to fetch" },
                        "rows": { "type": "integer", "description": "Number of rows to return (default 5)" },
                        "filters": { "type": "object", "properties": {}, "additionalProperties": { "type": "string" }, "description": "Key-value pairs to filter by column (case-insensitive substring match). Use column names from the CSV header." },
                        "columns": { "type": "array", "items": { "type": "string" }, "description": "List of columns to return. If omitted, all columns are returned." },
                        "sql": { "type": "string", "description": "Optional SQL query to run against the data. Use 'data' as the table name. Example: SELECT * FROM data WHERE \"Salary Minimum\" > 25" }
                    },
                    "required": ["resource_id"]
                }
            },
            {
                "name": "get_open_data_resource_schema",
                "description": "Get the schema (column names and types) for an Open Government resource.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "resource_id": { "type": "string", "description": "The Resource ID to fetch schema for" }
                    },
                    "required": ["resource_id"]
                }
            }
        ]
    }))
}

pub async fn handle_list_cubes<C: StatCanClientTrait>(
    client: Arc<C>,
    _args: &Value,
) -> Result<Value, JsonRpcError> {
    let resp = client.get_all_cubes_list_lite().await?;
    let count = resp.object.as_ref().map(|v| v.len()).unwrap_or(0);
    if count > 100 {
        let mut cubes = resp.object.unwrap();
        cubes.truncate(50);
        Ok(
            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&cubes).unwrap() }] }),
        )
    } else {
        Ok(
            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp).unwrap() }] }),
        )
    }
}

pub async fn handle_get_metadata<C: StatCanClientTrait>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let pid = args["pid"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing pid"))?;
    let resp = client.get_cube_metadata(pid).await?;
    Ok(
        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
    )
}

pub async fn handle_get_cube_dimensions<C: StatCanClientTrait>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let pid = args["pid"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing pid"))?;
    let resp = client.get_cube_metadata(pid).await?;

    let metadata = resp
        .object
        .ok_or(JsonRpcError::new(-32000, "Table not found"))?;

    let member_query_lower = args["member_query"].as_str().map(|s| s.to_lowercase());

    // Simplify output: Map Dimension Name -> List of Member Names
    let simplified: std::collections::HashMap<String, Vec<String>> = metadata
        .dimension
        .into_iter()
        .map(|d| {
            let members: Vec<String> = d
                .member
                .into_iter()
                .map(|m| format!("{} (ID: {})", m.member_name_en, m.member_id))
                .filter(|name| {
                    member_query_lower
                        .as_ref()
                        .map(|q| name.to_lowercase().contains(q))
                        .unwrap_or(true)
                })
                .collect();
            (d.dimension_name_en, members)
        })
        .collect();

    Ok(
        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&simplified).unwrap() }] }),
    )
}

pub async fn handle_search_cubes<C: StatCanClientTrait>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let query = args["query"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing query"))?;
    let resp = client.get_all_cubes_list_lite().await?;

    let all_cubes = resp.object.unwrap_or_default();
    let terms: Vec<String> = query.split_whitespace().map(|s| s.to_lowercase()).collect();

    let results: Vec<&crate::models::Cube> = all_cubes
        .iter()
        .filter(|c| {
            let title_lower = c.cube_title_en.to_lowercase();
            // ALL terms must be present (AND logic)
            terms.iter().all(|term| title_lower.contains(term))
        })
        .take(100)
        .collect();

    if results.is_empty() {
        Ok(json!({ "content": [{ "type": "text", "text": "No cubes found matching query." }] }))
    } else {
        Ok(
            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&results).unwrap() }] }),
        )
    }
}

pub async fn handle_fetch_data_by_vector<C: StatCanClientTrait>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let vectors_val = args["vectors"]
        .as_array()
        .ok_or(JsonRpcError::new(-32602, "Missing vectors array"))?;
    let vectors: Vec<String> = vectors_val
        .iter()
        .map(|v| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                v.to_string()
            }
        })
        .collect();

    let periods = args["recent_periods"].as_i64().unwrap_or(1) as i32;

    let resp = client.get_data_from_vectors(vectors, periods).await?;
    if resp.status != "SUCCESS" {
        Ok(
            json!({ "content": [{ "type": "text", "text": format!("Error from StatCan API: {}", resp.status) }] }),
        )
    } else if resp.object.is_none() || resp.object.as_ref().map(|v| v.is_empty()).unwrap_or(true) {
        Ok(
            json!({ "content": [{ "type": "text", "text": "No data found for the requested vector(s). Please verify the ID." }] }),
        )
    } else {
        Ok(
            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
        )
    }
}

pub async fn handle_fetch_data_by_coords<C: StatCanClientTrait>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let pid = args["pid"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing pid"))?;
    let coords_val = args["coords"]
        .as_array()
        .ok_or(JsonRpcError::new(-32602, "Missing coords array"))?;
    let coords: Vec<String> = coords_val
        .iter()
        .map(|v| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                v.to_string()
            }
        })
        .collect();

    let periods = args["recent_periods"].as_i64().unwrap_or(1) as i32;

    let resp = client.get_data_from_coords(pid, coords, periods).await?;
    if resp.status != "SUCCESS" {
        Ok(
            json!({ "content": [{ "type": "text", "text": format!("Error from StatCan API: {}", resp.status) }] }),
        )
    } else if resp.object.is_none() || resp.object.as_ref().map(|v| v.is_empty()).unwrap_or(true) {
        Ok(
            json!({ "content": [{ "type": "text", "text": "No data found for the requested coordinate(s)." }] }),
        )
    } else {
        Ok(
            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
        )
    }
}

pub async fn handle_search_cubes_by_dimension<C: StatCanClientTrait>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let dim_name = args["dimension_name"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing dimension_name"))?;
    let limit = args["limit"].as_u64().unwrap_or(10) as usize;

    let results = client.find_cubes_by_dimension(dim_name, limit).await?;

    // Format results nicely
    // Result: Vec<(pid, title, matching_dims)>
    let output_json = json!(results
        .iter()
        .map(|(pid, title, dims)| {
            json!({
                "productId": pid,
                "title": title,
                "matching_dimensions": dims
            })
        })
        .collect::<Vec<_>>());

    Ok(
        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&output_json).unwrap() }] }),
    )
}

pub async fn handle_fetch_data_snippet<C: StatCanClientTrait>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let pid = args["pid"]
        .as_str()
        .or_else(|| args["resource_id"].as_str())
        .ok_or(JsonRpcError::new(-32602, "Missing pid or resource_id"))?;
    let rows = args["rows"].as_u64().unwrap_or(5) as usize;
    let geo = args["geo"].as_str();
    let recent_months = args["recent_months"].as_u64(); // Option<u64>
    let filters = args["filters"].as_object(); // Option<&Map>

    // OPTIMIZATION: If no filters and no geo, try fast snippet first
    if geo.is_none() && filters.is_none() && recent_months.unwrap_or(1) <= 1 && rows <= 5 {
        if let Ok(df) = client.fetch_fast_snippet(pid).await {
            if df.as_polars().height() > 0 {
                let mut polars_df = df.into_polars();
                let mut buf = Vec::new();
                polars::prelude::JsonWriter::new(&mut buf)
                    .with_json_format(polars::prelude::JsonFormat::Json)
                    .finish(&mut polars_df)
                    .map_err(|e| {
                        error!("Serialization error: {}", e);
                        JsonRpcError::new(-32000, "Internal server error")
                    })?;

                let output = String::from_utf8(buf).map_err(|e| {
                    error!("UTF-8 error: {}", e);
                    JsonRpcError::new(-32000, "Internal server error")
                })?;
                return Ok(json!({ "content": [{ "type": "text", "text": output }] }));
            }
        }
        info!(
            "Fast snippet failed or empty for {}, falling back to full download.",
            pid
        );
    }

    let mut df_wrapper = client.fetch_full_table(pid).await?;

    // Filter by Geography if provided (Fuzzy Match enabled in wrapper)
    if let Some(g) = geo {
        df_wrapper = df_wrapper.filter_geo(g)?;
    }

    // Apply generic filters (Fuzzy Match enabled in wrapper)
    if let Some(f) = filters {
        for (col, val) in f {
            if let Some(v_str) = val.as_str() {
                df_wrapper = df_wrapper.filter_column(col, v_str)?;
            }
        }
    }

    // Get recent periods (returns ALL rows for N most recent dates)
    if let Some(n) = recent_months {
        df_wrapper = df_wrapper.take_recent_periods(n as usize)?;
        // Sort descending so most recent data appears first
        df_wrapper = df_wrapper.sort_date(true)?;
    } else {
        // Default: sort descending (most recent first) then take N rows
        df_wrapper = df_wrapper.sort_date(true)?;
        df_wrapper = df_wrapper.take_n(rows)?;
    }

    // Format output as JSON
    let mut df = df_wrapper.into_polars();
    let mut buf = Vec::new();
    polars::prelude::JsonWriter::new(&mut buf)
        .with_json_format(polars::prelude::JsonFormat::Json)
        .finish(&mut df)
        .map_err(|e| {
            error!("Serialization error: {}", e);
            JsonRpcError::new(-32000, "Internal server error")
        })?;

    let output = String::from_utf8(buf).map_err(|e| {
        error!("UTF-8 error: {}", e);
        JsonRpcError::new(-32000, "Internal server error")
    })?;
    Ok(json!({ "content": [{ "type": "text", "text": output }] }))
}

pub async fn handle_search_open_data<C: CKANClient>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let query = args["query"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing query"))?;
    let limit = args["limit"].as_u64().unwrap_or(10) as usize;

    let packages = client.search_packages(query, limit).await.map_err(|e| {
        error!("Open Data search failed: {}", e);
        JsonRpcError::new(-32000, format!("Open Data search failed: {}", e))
    })?;

    if packages.is_empty() {
        Ok(json!({ "content": [{ "type": "text", "text": "No datasets found matching query." }] }))
    } else {
        Ok(
            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&packages).unwrap() }] }),
        )
    }
}

pub async fn handle_get_open_data_metadata<C: CKANClient>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let id = args["id"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing id"))?;

    let meta = client.get_package_metadata(id).await.map_err(|e| {
        error!("Get metadata failed: {}", e);
        JsonRpcError::new(-32000, format!("Get metadata failed: {}", e))
    })?;

    Ok(
        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&meta).unwrap() }] }),
    )
}

pub async fn handle_query_open_data_datastore<C: CKANClient>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let sql = args["sql"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing sql"))?;

    let records = client.query_datastore(sql).await.map_err(|e| {
        error!("Datastore query failed: {}", e);
        JsonRpcError::new(-32000, format!("Datastore query failed: {}", e))
    })?;

    // Format output (limit size if needed, but client limits via SQL usually)
    Ok(
        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&records).unwrap() }] }),
    )
}

pub async fn handle_fetch_open_data_resource_snippet<C: CKANClient>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let resource_id = args["resource_id"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing resource_id"))?;

    let rows = args["rows"].as_u64().unwrap_or(5) as usize;

    // 1. Get the resource handler to find the download URL
    info!("Looking up resource: {}", resource_id);
    let handler = client
        .get_resource_handler(resource_id)
        .await
        .map_err(|e| {
            error!("Failed to get resource handler for {}: {}", resource_id, e);
            JsonRpcError::new(-32000, format!("Failed to look up resource: {}", e))
        })?;

    let download_url = match handler {
        crate::DataHandler::BlobDownload(url) => url,
        crate::DataHandler::DatastoreQuery(_, Some(url)) => url,
        crate::DataHandler::DatastoreQuery(_, None) => {
            return Err(JsonRpcError::new(
                -32000,
                "Resource has no download URL available",
            ));
        }
    };

    info!("Downloading Open Data CSV from: {}", download_url);

    // 2. Download the CSV with a longer timeout (these files can be large)
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .gzip(true)
        .build()
        .map_err(|e| {
            error!("Failed to build HTTP client: {}", e);
            JsonRpcError::new(-32000, format!("HTTP client error: {}", e))
        })?;

    // 2. Download/Extract using library helper (supports caching and ZIP)
    let temp_path = crate::download_and_extract_file(&http_client, &download_url, resource_id)
        .await
        .map_err(|e| {
            error!("Failed to download/extract resource {}: {}", resource_id, e);
            JsonRpcError::new(-32000, format!("Download failed: {}", e))
        })?;

    // 4. Parse with Polars, apply filters, take N rows
    let rows_limit = rows;
    let filters_owned: Option<Vec<(String, String)>> =
        args.get("filters").and_then(|v| v.as_object()).map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        });

    let columns_owned: Option<Vec<String>> = args["columns"].as_array().map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    });

    let sql_query = args["sql"].as_str().map(|s| s.to_string());

    // Detect delimiter
    let separator = {
        use std::io::Read;
        let mut file = std::fs::File::open(&temp_path).map_err(|e| {
            error!("Failed to open temp file for inspection: {}", e);
            JsonRpcError::new(-32000, "Internal server error")
        })?;
        let mut buffer = [0u8; 4096];
        let n = file.read(&mut buffer).unwrap_or(0);
        let slice = &buffer[..n];

        let commas = slice.iter().filter(|&&c| c == b',').count();
        let tabs = slice.iter().filter(|&&c| c == b'\t').count();

        if tabs > commas {
            b'\t'
        } else {
            b','
        }
    };
    info!("Detected separator: '{}'", separator as char);

    let temp_path_clone = temp_path.clone();
    let df_result = tokio::task::spawn_blocking(
        move || -> std::result::Result<polars::prelude::DataFrame, String> {
            let reader = polars::prelude::CsvReader::from_path(&temp_path_clone)
                .map_err(|e| format!("Failed to open CSV: {}", e))?;

            let df = reader
                .infer_schema(Some(100))
                .has_header(true)
                .with_separator(separator)
                .with_ignore_errors(true) // Skip rows with encoding/parsing errors
                .truncate_ragged_lines(true)
                .finish()
                .map_err(|e| format!("CSV parse error: {}", e))?;

            info!("Parsed CSV: {} rows x {} cols", df.height(), df.width());

            let mut result = df;

            // Apply filters if provided
            if let Some(ref filter_pairs) = filters_owned {
                for (col_name, col_val) in filter_pairs {
                    let col_lower = col_name.to_lowercase();
                    let val_lower = col_val.to_lowercase();
                    let actual_col = result
                        .get_column_names()
                        .iter()
                        .find(|c| c.to_lowercase() == col_lower)
                        .map(|c| c.to_string());

                    if let Some(col) = actual_col {
                        let series = result
                            .column(&col)
                            .map_err(|e| format!("Column error: {}", e))?;
                        let str_series = series
                            .cast(&polars::prelude::DataType::String)
                            .map_err(|e| format!("Cast error: {}", e))?;
                        let ca = str_series.str().map_err(|e| format!("Str error: {}", e))?;

                        let mask = ca
                            .into_iter()
                            .map(|opt_val| {
                                opt_val.map_or(false, |v| v.to_lowercase().contains(&val_lower))
                            })
                            .collect::<polars::prelude::BooleanChunked>();
                        result = result
                            .filter(&mask)
                            .map_err(|e| format!("Filter error: {}", e))?;
                    }
                }
            }

            // Apply SQL if provided
            if let Some(sql) = sql_query {
                let mut ctx = polars::sql::SQLContext::new();
                ctx.register("data", result.clone().lazy());
                let sql_df = ctx.execute(&sql).map_err(|e| format!("SQL error: {}", e))?;
                result = sql_df
                    .collect()
                    .map_err(|e| format!("SQL collection error: {}", e))?;
            }

            // Select columns if provided
            if let Some(cols) = columns_owned {
                result = result
                    .select(&cols)
                    .map_err(|e| format!("Selection error: {}", e))?;
            }

            let take = std::cmp::min(rows_limit, result.height());
            Ok(result.head(Some(take)))
        },
    )
    .await
    .map_err(|e| {
        error!("Task join error: {}", e);
        JsonRpcError::new(-32000, format!("Processing error: {}", e))
    })?
    .map_err(|e| {
        error!("CSV processing failed: {}", e);
        JsonRpcError::new(-32000, e)
    })?;

    // Clean up temp file
    let _ = tokio::fs::remove_file(&temp_path).await;

    // 5. Serialize to JSON
    let mut df = df_result;
    let mut buf = Vec::new();
    polars::prelude::JsonWriter::new(&mut buf)
        .with_json_format(polars::prelude::JsonFormat::Json)
        .finish(&mut df)
        .map_err(|e| {
            error!("Serialization error: {}", e);
            JsonRpcError::new(-32000, format!("Serialization error: {}", e))
        })?;

    let output = String::from_utf8(buf).map_err(|e| {
        error!("UTF-8 error: {}", e);
        JsonRpcError::new(-32000, format!("Encoding error: {}", e))
    })?;

    Ok(json!({ "content": [{ "type": "text", "text": output }] }))
}

pub async fn handle_get_open_data_resource_schema<C: CKANClient>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let resource_id = args["resource_id"]
        .as_str()
        .ok_or_else(|| JsonRpcError::new(-32602, "resource_id is required"))?;

    let schema = client.get_resource_schema(resource_id).await.map_err(|e| {
        error!("Failed to get schema for {}: {}", resource_id, e);
        JsonRpcError::new(-32000, format!("Failed to get schema: {}", e))
    })?;

    Ok(json!({
        "resource_id": resource_id,
        "schema": schema.into_iter().map(|(n, t)| json!({"name": n, "type": t})).collect::<Vec<Value>>()
    }))
}

pub async fn handle_request<C: StatCanClientTrait, O: CKANClient>(
    client: Arc<C>,
    od_client: Arc<O>,
    method: &str,
    params: Option<Value>,
) -> Result<Value, JsonRpcError> {
    match method {
        "tools/list" => list_tools(),
        "tools/call" => {
            let params = params.ok_or(JsonRpcError::new(-32602, "Missing params"))?;
            let name = params["name"]
                .as_str()
                .ok_or(JsonRpcError::new(-32602, "Missing tool name"))?;
            let args = &params["arguments"];

            match name {
                "list_cubes" => handle_list_cubes(client, args).await,
                "get_metadata" => handle_get_metadata(client, args).await,
                "get_cube_dimensions" => handle_get_cube_dimensions(client, args).await,
                "search_cubes" => handle_search_cubes(client, args).await,
                "fetch_data_by_vector" => handle_fetch_data_by_vector(client, args).await,
                "fetch_data_by_coords" => handle_fetch_data_by_coords(client, args).await,
                "search_cubes_by_dimension" => handle_search_cubes_by_dimension(client, args).await,
                "fetch_data_snippet" => handle_fetch_data_snippet(client, args).await,
                "search_open_data" => handle_search_open_data(od_client, args).await,
                "get_open_data_metadata" => handle_get_open_data_metadata(od_client, args).await,
                "query_open_data_datastore" => {
                    handle_query_open_data_datastore(od_client, args).await
                }
                "fetch_open_data_resource_snippet" => {
                    handle_fetch_open_data_resource_snippet(od_client, args).await
                }
                "get_open_data_resource_schema" => {
                    handle_get_open_data_resource_schema(od_client, args).await
                }
                _ => Err(JsonRpcError::new(-32601, "Tool not found")),
            }
        }
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "serverInfo": {
                "name": "stat-can-rs",
                "version": "0.1.0"
            },
            "capabilities": {
                "tools": {}
            }
        })),
        "notifications/initialized" => Ok(Value::Null),
        "ping" => Ok(json!({})),
        _ => Err(JsonRpcError::new(-32601, "Method not found")),
    }
}
