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
                "description": "List all available data cubes (summary). Tip: For discovering specific datasets, consider using 'search_all' or 'search_cubes' instead.",
                "inputSchema": { "type": "object", "properties": {} }
            },
            {
                "name": "get_metadata",
                "description": "Get metadata for a specific cube. For dimension details, consider using 'get_cube_dimensions' for easier discovery.",
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
                "description": "Get valid dimensions and members for a cube. Use this to find what 'Geography' or 'Products' filters are available. Highly recommended before querying full tables.",
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
                "description": "Search for cubes by title (supports multi-word queries). Ranked by fuzzy search. To explore a found cube's contents, use 'get_cube_dimensions'.",
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
                        "filters": { "type": "object", "properties": {}, "additionalProperties": { "type": "string" }, "description": "Key-value pairs for column filtering. Uses exact match first, then substring fallback (e.g. {'Products and product groups': 'Energy'} matches 'Energy' exactly, not 'All-items excluding energy')" },
                        "format": { "type": "string", "enum": ["json", "csv"], "description": "The output format. Default is json." }
                    },
                    "required": ["pid"]
                }
            },
            {
                "name": "search_all",
                "description": "Unified search across both StatCan cubes and the Canadian Open Government portal.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "Search query (e.g. 'labour ontario')" },
                        "limit": { "type": "integer", "description": "Max results per source (default 10)" }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "search_open_data",
                "description": "Search the Canadian Open Government portal for datasets. For detailed metadata, follow up with 'get_open_data_metadata'.",
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
                "description": "Get detailed metadata for a specific dataset from the Open Government portal. Output will identify a 'suggested_best_resource_id' for subsequent data fetching.",
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
                        "sql": { "type": "string", "description": "The SQL query to execute (e.g. 'SELECT * FROM \"resource_id\" LIMIT 5'). Note: Table names must be the Resource ID in double quotes." },
                        "format": { "type": "string", "enum": ["json", "csv"], "description": "The output format. Default is json." }
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
                        "columns": { "type": "array", "items": { "type": "string" }, "description": "List of columns to return. If omitted, all columns are returned." },
                        "sql": { "type": "string", "description": "Optional SQL query to run against the data. Use 'data' as the table name. Example: SELECT * FROM data WHERE \"Salary Minimum\" > 25" },
                        "format": { "type": "string", "enum": ["json", "csv"], "description": "The output format. Default is json." }
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
    let mut resp = client.get_all_cubes_list_lite().await?;
    let count = resp.object.as_ref().map(|v| v.len()).unwrap_or(0);
    if count > 100 {
        if let Some(ref mut cubes) = resp.object {
            cubes.truncate(50);
            let json_str = serde_json::to_string_pretty(&cubes)
                .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
            Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
        } else {
            Ok(json!({ "content": [{ "type": "text", "text": "[]" }] }))
        }
    } else {
        let json_str = serde_json::to_string_pretty(&resp)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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
    let json_str = serde_json::to_string_pretty(&resp.object)
        .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
    Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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

    let json_str = serde_json::to_string_pretty(&simplified)
        .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
    Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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

    let mut scored_cubes: Vec<(&crate::models::Cube, f64)> = all_cubes
        .iter()
        .filter_map(|c| {
            let score = crate::data_helpers::score_cube_title_match(&c.cube_title_en, query);

            // Only keep results with a reasonable score threshold
            if score > 0.6 {
                Some((c, score))
            } else {
                None
            }
        })
        .collect();

    // Sort by score descending
    scored_cubes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    // Take top 100 results and strip the scores for output
    let results: Vec<&crate::models::Cube> =
        scored_cubes.into_iter().take(100).map(|(c, _)| c).collect();

    if results.is_empty() {
        Ok(json!({ "content": [{ "type": "text", "text": "No cubes found matching query." }] }))
    } else {
        let json_str = serde_json::to_string_pretty(&results)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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
        let json_str = serde_json::to_string_pretty(&resp.object)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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
        let json_str = serde_json::to_string_pretty(&resp.object)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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

    let json_str = serde_json::to_string_pretty(&output_json)
        .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
    Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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
    let filters = args["filters"].as_object();
    let format = args["format"].as_str().unwrap_or("json").to_lowercase(); // Option<&Map>

    // OPTIMIZATION: If no filters and no geo, try fast snippet first
    if geo.is_none() && filters.is_none() && recent_months.unwrap_or(1) <= 1 && rows <= 5 {
        if let Ok(df) = client.fetch_fast_snippet(pid).await {
            if df.as_polars().height() > 0 {
                let mut polars_df = df.into_polars();
                let output = if format == "csv" {
                    let mut buf = Vec::new();
                    polars::prelude::CsvWriter::new(&mut buf)
                        .include_header(true)
                        .finish(&mut polars_df)
                        .map_err(|e| {
                            error!("CSV serialization error: {}", e);
                            JsonRpcError::new(-32000, "Internal server error")
                        })?;
                    String::from_utf8(buf).map_err(|e| {
                        error!("UTF-8 error: {}", e);
                        JsonRpcError::new(-32000, "Internal server error")
                    })?
                } else {
                    let mut buf = Vec::new();
                    polars::prelude::JsonWriter::new(&mut buf)
                        .with_json_format(polars::prelude::JsonFormat::Json)
                        .finish(&mut polars_df)
                        .map_err(|e| {
                            error!("Serialization error: {}", e);
                            JsonRpcError::new(-32000, "Internal server error")
                        })?;
                    String::from_utf8(buf).map_err(|e| {
                        error!("UTF-8 error: {}", e);
                        JsonRpcError::new(-32000, "Internal server error")
                    })?
                };
                return Ok(json!({ "content": [{ "type": "text", "text": output }] }));
            }
        }
        info!(
            "Fast snippet failed or empty for {}, falling back to full download.",
            pid
        );
    }

    let mut df_wrapper = client.fetch_full_table_scan(pid).await?;

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

    // Format output
    let mut df = df_wrapper.collect()?.into_polars();
    let output = if format == "csv" {
        let mut buf = Vec::new();
        polars::prelude::CsvWriter::new(&mut buf)
            .include_header(true)
            .finish(&mut df)
            .map_err(|e| {
                error!("CSV serialization error: {}", e);
                JsonRpcError::new(-32000, "Internal server error")
            })?;
        String::from_utf8(buf).map_err(|e| {
            error!("UTF-8 error: {}", e);
            JsonRpcError::new(-32000, "Internal server error")
        })?
    } else {
        let mut buf = Vec::new();
        polars::prelude::JsonWriter::new(&mut buf)
            .with_json_format(polars::prelude::JsonFormat::Json)
            .finish(&mut df)
            .map_err(|e| {
                error!("Serialization error: {}", e);
                JsonRpcError::new(-32000, "Internal server error")
            })?;
        String::from_utf8(buf).map_err(|e| {
            error!("UTF-8 error: {}", e);
            JsonRpcError::new(-32000, "Internal server error")
        })?
    };
    Ok(json!({ "content": [{ "type": "text", "text": output }] }))
}

pub async fn handle_search_all<C: StatCanClientTrait, O: CKANClient>(
    client: Arc<C>,
    od_client: Arc<O>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let query = args["query"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing query"))?;
    let limit = args["limit"].as_u64().unwrap_or(10) as usize;

    let query_lower = query.to_lowercase();

    // Spawn both requests concurrently
    let statcan_future = client.get_all_cubes_list_lite();
    let od_future = od_client.search_packages(query, limit);

    let (statcan_res, od_res) = tokio::join!(statcan_future, od_future);

    let mut unified_results = Vec::new();

    // Process StatCan results
    if let Ok(resp) = statcan_res {
        let all_cubes = resp.object.unwrap_or_default();
        let mut scored_cubes: Vec<(&crate::models::Cube, f64)> = all_cubes
            .iter()
            .filter_map(|c| {
                let score = crate::data_helpers::score_cube_title_match(&c.cube_title_en, query);

                if score > 0.6 {
                    Some((c, score))
                } else {
                    None
                }
            })
            .collect();

        scored_cubes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        for (cube, score) in scored_cubes.into_iter().take(limit) {
            unified_results.push(json!({
                "source": "StatCan",
                "id": cube.product_id,
                "title": cube.cube_title_en,
                "score": score
            }));
        }
    }

    // Process Open Data results
    if let Ok(packages) = od_res {
        for pkg in packages.into_iter().take(limit) {
            unified_results.push(json!({
                "source": "OpenData",
                "id": pkg.id,
                "title": pkg.title,
                // Open Data doesn't give us a score directly, we'll assign a default or calculate one
                "score": strsim::jaro_winkler(&pkg.title.to_lowercase(), &query_lower) + 1.0 // Add 1.0 because CKAN returned it (implies some relevance)
            }));
        }
    }

    // Sort combined results
    unified_results.sort_by(|a, b| {
        let score_a = a["score"].as_f64().unwrap_or(0.0);
        let score_b = b["score"].as_f64().unwrap_or(0.0);
        score_b
            .partial_cmp(&score_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if unified_results.is_empty() {
        Ok(
            json!({ "content": [{ "type": "text", "text": "No datasets found matching query in either source." }] }),
        )
    } else {
        let json_str = serde_json::to_string_pretty(&unified_results)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
    }
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
        let json_str = serde_json::to_string_pretty(&packages)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
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

    // Optional: add a field or indicator for the "best" resource
    if let Some(best) = crate::data_helpers::select_best_resource(&meta.resources) {
        let best_id = best.id.clone();
        // We could just add a small note or return it alongside
        let mut meta_json = serde_json::to_value(&meta)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        if let Some(obj) = meta_json.as_object_mut() {
            obj.insert("suggested_best_resource_id".to_string(), json!(best_id));
        }
        let json_str = serde_json::to_string_pretty(&meta_json)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
        return Ok(json!({ "content": [{ "type": "text", "text": json_str }] }));
    }

    let json_str = serde_json::to_string_pretty(&meta)
        .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?;
    Ok(json!({ "content": [{ "type": "text", "text": json_str }] }))
}

pub async fn handle_query_open_data_datastore<C: CKANClient>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let sql = args["sql"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing sql"))?;
    let format = args["format"].as_str().unwrap_or("json").to_lowercase();

    let records = client.query_datastore(sql).await.map_err(|e| {
        error!("Datastore query failed: {}", e);
        JsonRpcError::new(-32000, format!("Datastore query failed: {}", e))
    })?;

    let output = if format == "csv" {
        if records.is_empty() {
            String::new()
        } else {
            // Convert Vec<Value> into a CSV string
            let mut wtr = csv::Writer::from_writer(Vec::new());
            let headers = if let Some(first) = records.first() {
                first
                    .as_object()
                    .map(|obj| obj.keys().cloned().collect::<Vec<String>>())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            if !headers.is_empty() {
                wtr.write_record(&headers).map_err(|e| {
                    error!("CSV header error: {}", e);
                    JsonRpcError::new(-32000, "Internal server error")
                })?;

                for row in &records {
                    if let Some(obj) = row.as_object() {
                        let vals: Vec<String> = headers
                            .iter()
                            .map(|k| {
                                obj.get(k)
                                    .map(|v| {
                                        if v.is_string() {
                                            v.as_str().unwrap().to_string()
                                        } else {
                                            v.to_string()
                                        }
                                    })
                                    .unwrap_or_default()
                            })
                            .collect();
                        wtr.write_record(&vals).map_err(|e| {
                            error!("CSV row error: {}", e);
                            JsonRpcError::new(-32000, "Internal server error")
                        })?;
                    }
                }
            }
            let buf = wtr.into_inner().map_err(|e| {
                error!("CSV finalize error: {}", e);
                JsonRpcError::new(-32000, "Internal server error")
            })?;
            String::from_utf8(buf).map_err(|e| {
                error!("UTF-8 error: {}", e);
                JsonRpcError::new(-32000, "Internal server error")
            })?
        }
    } else {
        serde_json::to_string_pretty(&records)
            .map_err(|e| JsonRpcError::new(-32000, format!("Serialization error: {}", e)))?
    };

    Ok(json!({ "content": [{ "type": "text", "text": output }] }))
}

pub async fn handle_fetch_open_data_resource_snippet<C: CKANClient>(
    client: Arc<C>,
    args: &Value,
) -> Result<Value, JsonRpcError> {
    let resource_id = args["resource_id"]
        .as_str()
        .ok_or(JsonRpcError::new(-32602, "Missing resource_id"))?;

    let rows = args["rows"].as_u64().unwrap_or(5) as usize;
    let format = args["format"].as_str().unwrap_or("json").to_lowercase();

    // 1. Fetch Resource as DataFrame using helper
    let (df, temp_path) = crate::data_helpers::fetch_resource_as_df(client.clone(), resource_id)
        .await
        .map_err(|e| {
            error!("Fetch failed: {}", e);
            JsonRpcError::new(-32000, e)
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

    let df_result = tokio::task::spawn_blocking(
        move || -> std::result::Result<polars::prelude::DataFrame, String> {
            let mut result = df;

            // Apply filters if provided
            if let Some(ref filter_pairs) = filters_owned {
                let col_map: std::collections::HashMap<String, String> = result
                    .get_column_names()
                    .iter()
                    .map(|c| (c.to_lowercase(), c.to_string()))
                    .collect();

                for (col_name, col_val) in filter_pairs {
                    let col_lower = col_name.to_lowercase();
                    let val_lower = col_val.to_lowercase();
                    let actual_col = col_map.get(&col_lower).cloned();

                    if let Some(col_name) = actual_col {
                        result = result
                            .lazy()
                            .filter(
                                polars::prelude::col(&col_name)
                                    .cast(polars::prelude::DataType::String)
                                    .str()
                                    .to_lowercase()
                                    .str()
                                    .contains_literal(polars::prelude::lit(val_lower)),
                            )
                            .collect()
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

    // 5. Serialize output
    let mut df = df_result;
    let output = if format == "csv" {
        let mut buf = Vec::new();
        polars::prelude::CsvWriter::new(&mut buf)
            .include_header(true)
            .finish(&mut df)
            .map_err(|e| {
                error!("CSV serialization error: {}", e);
                JsonRpcError::new(-32000, format!("CSV serialization error: {}", e))
            })?;
        String::from_utf8(buf).map_err(|e| {
            error!("UTF-8 error: {}", e);
            JsonRpcError::new(-32000, format!("Encoding error: {}", e))
        })?
    } else {
        let mut buf = Vec::new();
        polars::prelude::JsonWriter::new(&mut buf)
            .with_json_format(polars::prelude::JsonFormat::Json)
            .finish(&mut df)
            .map_err(|e| {
                error!("Serialization error: {}", e);
                JsonRpcError::new(-32000, format!("Serialization error: {}", e))
            })?;
        String::from_utf8(buf).map_err(|e| {
            error!("UTF-8 error: {}", e);
            JsonRpcError::new(-32000, format!("Encoding error: {}", e))
        })?
    };

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
                "search_all" => handle_search_all(client, od_client, args).await,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_tools() {
        let result = list_tools();
        assert!(result.is_ok(), "list_tools should return Ok");

        let value = result.unwrap();

        let tools_array = value.get("tools").expect("Response should contain a 'tools' key");
        assert!(tools_array.is_array(), "'tools' should be an array");

        let tools = tools_array.as_array().unwrap();
        assert!(!tools.is_empty(), "The tools array should not be empty");

        for tool in tools {
            assert!(tool.get("name").is_some(), "Each tool must have a 'name' property");
            assert!(tool.get("description").is_some(), "Each tool must have a 'description' property");
            assert!(tool.get("inputSchema").is_some(), "Each tool must have an 'inputSchema' property");

            let name = tool.get("name").unwrap();
            let description = tool.get("description").unwrap();
            let schema = tool.get("inputSchema").unwrap();

            assert!(name.is_string(), "'name' should be a string");
            assert!(description.is_string(), "'description' should be a string");
            assert!(schema.is_object(), "'inputSchema' should be an object");
        }

        // Assert that we have 14 tools exactly to be robust and precise as seen in the code.
        assert_eq!(tools.len(), 14, "There should be exactly 14 tools defined");
    }
}
