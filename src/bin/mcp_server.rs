use axum::{
    extract::{Json, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Router,
};
use clap::Parser;
use constant_time_eq::constant_time_eq;
use futures::stream::{self, StreamExt};
use futures::Stream;
use polars::prelude::SerWriter;
use postgrest::Postgrest;
use rand::{thread_rng, Rng};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use statcan_rs::{StatCanClient, StatCanClientTrait, StatCanError};
use std::{
    convert::Infallible,
    io::BufRead,
    sync::{Arc, OnceLock},
};

use tower_governor::{
    governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor, GovernorLayer,
};
use tower_http::trace::TraceLayer;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on (if not set, runs in stdio mode)
    #[arg(short, long, env = "MCP_PORT")]
    port: Option<u16>,

    /// API Key for HTTP authentication
    #[arg(long, env = "MCP_API_KEY")]
    api_key: Option<String>,

    /// Rate limit (requests per minute) for stdio mode. Default: 60.
    #[arg(long, default_value = "60")]
    rate_limit: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "text".to_string());
    if log_format.to_lowercase() == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_writer(std::io::stderr)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
    }
    let args = Args::parse();
    let client = Arc::new(StatCanClient::new()?);

    if let Some(port) = args.port {
        info!("Starting MCP server in HTTP/SSE mode on port {}", port);
        http_mode(port, args.api_key, client).await?;
    } else {
        info!(
            "Starting MCP server in Stdio mode (Rate Limit: {}/min)",
            args.rate_limit
        );
        stdio_mode(client, args.rate_limit).await?;
    }

    Ok(())
}

// --- Protocol Types ---

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[serde(rename = "jsonrpc")]
    _jsonrpc: String,
    method: String,
    params: Option<Value>,
    id: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    result: Option<Value>,
    error: Option<JsonRpcError>,
    id: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

impl JsonRpcResponse {
    fn from_result(result: Result<Value, JsonRpcError>, id: Option<Value>) -> Self {
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
    fn new(code: i32, message: impl Into<String>) -> Self {
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
            StatCanError::Api(ref msg) if msg == "Invalid PID format" || msg == "PID cannot be empty" => {
                JsonRpcError::new(-32602, msg.clone())
            }
            e => {
                error!("Internal error: {:?}", e);
                JsonRpcError::new(-32000, "Internal server error")
            }
        }
    }
}

// --- Core Logic ---


async fn handle_request<C: StatCanClientTrait>(
    client: Arc<C>,
    method: &str,
    params: Option<Value>,
) -> Result<Value, JsonRpcError> {
    match method {
        "tools/list" => Ok(json!({
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
                }
            ]
        })),
        "tools/call" => {
            let params = params.ok_or(JsonRpcError::new(-32602, "Missing params"))?;
            let name = params["name"]
                .as_str()
                .ok_or(JsonRpcError::new(-32602, "Missing tool name"))?;
            let args = &params["arguments"];

            match name {
                "list_cubes" => {
                    let resp = client
                        .get_all_cubes_list_lite()
                        .await?;
                    // Truncate for brevity in LLM context? No, user wants list.
                    // But the list is HUGE (thousands). We should probably warn or truncate.
                    // For now, let's verify size.
                    let count = resp.object.as_ref().map(|v| v.len()).unwrap_or(0);
                    if count > 100 {
                        // Return summary or first 100?
                        // Let's return first 50 to avoid context overflow.
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
                "get_metadata" => {
                    let pid = args["pid"]
                        .as_str()
                        .ok_or(JsonRpcError::new(-32602, "Missing pid"))?;
                    let resp = client
                        .get_cube_metadata(pid)
                        .await?;
                    Ok(
                        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
                    )
                }
                "get_cube_dimensions" => {
                    let pid = args["pid"]
                        .as_str()
                        .ok_or(JsonRpcError::new(-32602, "Missing pid"))?;
                    let resp = client
                        .get_cube_metadata(pid)
                        .await?;

                    let metadata = resp
                        .object
                        .ok_or(JsonRpcError::new(-32000, "Table not found"))?;

                    let member_query_lower =
                        args["member_query"].as_str().map(|s| s.to_lowercase());

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
                "search_cubes" => {
                    let query = args["query"]
                        .as_str()
                        .ok_or(JsonRpcError::new(-32602, "Missing query"))?;
                    let resp = client
                        .get_all_cubes_list_lite()
                        .await?;

                    let all_cubes = resp.object.unwrap_or_default();
                    let terms: Vec<String> =
                        query.split_whitespace().map(|s| s.to_lowercase()).collect();

                    let results: Vec<&statcan_rs::models::Cube> = all_cubes
                        .iter()
                        .filter(|c| {
                            let title_lower = c.cube_title_en.to_lowercase();
                            // ALL terms must be present (AND logic)
                            terms.iter().all(|term| title_lower.contains(term))
                        })
                        .take(100)
                        .collect();

                    if results.is_empty() {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": "No cubes found matching query." }] }),
                        )
                    } else {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&results).unwrap() }] }),
                        )
                    }
                }
                "fetch_data_by_vector" => {
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

                    let resp = client
                        .get_data_from_vectors(vectors, periods)
                        .await?;
                    if resp.status != "SUCCESS" {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": format!("Error from StatCan API: {}", resp.status) }] }),
                        )
                    } else if resp.object.is_none()
                        || resp.object.as_ref().map(|v| v.is_empty()).unwrap_or(true)
                    {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": "No data found for the requested vector(s). Please verify the ID." }] }),
                        )
                    } else {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
                        )
                    }
                }
                "fetch_data_by_coords" => {
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

                    let resp = client
                        .get_data_from_coords(pid, coords, periods)
                        .await?;
                    if resp.status != "SUCCESS" {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": format!("Error from StatCan API: {}", resp.status) }] }),
                        )
                    } else if resp.object.is_none()
                        || resp.object.as_ref().map(|v| v.is_empty()).unwrap_or(true)
                    {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": "No data found for the requested coordinate(s)." }] }),
                        )
                    } else {
                        Ok(
                            json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
                        )
                    }
                }
                "search_cubes_by_dimension" => {
                    let dim_name = args["dimension_name"]
                        .as_str()
                        .ok_or(JsonRpcError::new(-32602, "Missing dimension_name"))?;
                    let limit = args["limit"].as_u64().unwrap_or(10) as usize;

                    let results = client
                        .find_cubes_by_dimension(dim_name, limit)
                        .await?;

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
                "fetch_data_snippet" => {
                    let pid = args["pid"]
                        .as_str()
                        .ok_or(JsonRpcError::new(-32602, "Missing pid"))?;
                    let rows = args["rows"].as_u64().unwrap_or(5) as usize;
                    let geo = args["geo"].as_str();
                    let recent_months = args["recent_months"].as_u64(); // Option<u64>
                    let filters = args["filters"].as_object(); // Option<&Map>

                    // OPTIMIZATION: If no filters and no geo, try fast snippet first
                    if geo.is_none()
                        && filters.is_none()
                        && recent_months.unwrap_or(1) <= 1
                        && rows <= 5
                    {
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
                                return Ok(
                                    json!({ "content": [{ "type": "text", "text": output }] }),
                                );
                            }
                        }
                        info!(
                            "Fast snippet failed or empty for {}, falling back to full download.",
                            pid
                        );
                    }

                    let mut df_wrapper = client
                        .fetch_full_table(pid)
                        .await?;

                    // Filter by Geography if provided (Fuzzy Match enabled in wrapper)
                    if let Some(g) = geo {
                        df_wrapper = df_wrapper.filter_geo(g)?;
                    }

                    // Apply generic filters (Fuzzy Match enabled in wrapper)
                    if let Some(f) = filters {
                        for (col, val) in f {
                            if let Some(v_str) = val.as_str() {
                                df_wrapper = df_wrapper
                                    .filter_column(col, v_str)?;
                            }
                        }
                    }

                    // Get recent periods (returns ALL rows for N most recent dates)
                    if let Some(n) = recent_months {
                        df_wrapper = df_wrapper
                            .take_recent_periods(n as usize)?;
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
                _ => Err(JsonRpcError::new(-32601, "Method not found")),
            }
        }
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "server": {
                "name": "stat-can-rs",
                "version": "0.1.0"
            },
            "capabilities": {
                "tools": {}
            }
        })),
        "notifications/initialized" => Ok(json!({})),
        "ping" => Ok(json!({})),
        _ => Err(JsonRpcError::new(-32601, "Method not found")),
    }
}

// --- Stdio Mode ---

use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

async fn stdio_mode(client: Arc<StatCanClient>, rate_limit_per_min: u32) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut handle = stdin.lock();
    let mut line = String::new();

    // Configure Rate Limiter
    let quota = Quota::per_minute(
        NonZeroU32::new(rate_limit_per_min).unwrap_or(NonZeroU32::new(60).unwrap()),
    );
    let limiter = RateLimiter::direct(quota);

    while handle.read_line(&mut line)? > 0 {
        if line.trim().is_empty() {
            line.clear();
            continue;
        }

        // Enforce Rate Limit
        if limiter.check().is_err() {
            // If rate limited, we should probably return an error JSON-RPC response
            // asking the client to back off, or just block.
            // Blocking is better for stdio as it acts as backpressure.
            limiter.until_ready().await;
        }

        let req: Result<JsonRpcRequest, _> = serde_json::from_str(&line);
        match req {
            Ok(req) => {
                let is_notification = req.id.is_none();
                let result = handle_request(client.clone(), &req.method, req.params).await;

                if !is_notification {
                    let resp = JsonRpcResponse::from_result(result, req.id);
                    match serde_json::to_string(&resp) {
                        Ok(json_str) => println!("{}", json_str),
                        Err(e) => error!("Failed to serialize response: {}", e),
                    }
                }
            }
            Err(e) => {
                error!("Failed to parse request: {}", e);
                // Send parse error?
            }
        }

        line.clear();
    }
    Ok(())
}

// --- HTTP Mode ---

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

struct AuthCacheEntry {
    expires_at: Instant,
}

struct AuthCache {
    entries: RwLock<HashMap<String, AuthCacheEntry>>,
    ttl: Duration,
}

impl AuthCache {
    fn new(ttl_secs: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    fn is_valid(&self, key_hash: &str) -> bool {
        let map = self.entries.read().unwrap();
        if let Some(entry) = map.get(key_hash) {
            if entry.expires_at > Instant::now() {
                return true;
            }
        }
        false
    }

    fn add(&self, key_hash: String) {
        let mut map = self.entries.write().unwrap();
        map.insert(
            key_hash,
            AuthCacheEntry {
                expires_at: Instant::now() + self.ttl,
            },
        );
    }
}

#[derive(Clone)]
#[allow(dead_code)]
struct AppState {
    client: Arc<StatCanClient>,
    supabase: Option<Arc<Postgrest>>,
    use_supabase: bool,
    legacy_key: Option<String>,
    auth_cache: Arc<AuthCache>,
}

static EMAIL_REGEX: OnceLock<Regex> = OnceLock::new();

fn is_valid_email(email: &str) -> bool {
    let re = EMAIL_REGEX.get_or_init(|| {
        Regex::new(r"^(?i)[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$").unwrap()
    });
    if !re.is_match(email) {
        return false;
    }
    // Additional checks for common pitfalls not easily caught by simple regex
    if email.contains("..")
        || email.starts_with('.')
        || email
            .split('@')
            .next()
            .map(|s| s.ends_with('.'))
            .unwrap_or(false)
    {
        return false;
    }
    true
}

#[derive(serde::Deserialize)]
struct RegisterRequest {
    email: Option<String>,
}

#[derive(serde::Serialize)]
struct RegisterResponse {
    api_key: String,
    message: String,
}

async fn handle_register(
    State(state): State<AppState>,
    Json(payload): Json<RegisterRequest>,
) -> impl IntoResponse {
    if !state.use_supabase {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({"error": "Registration not enabled"})),
        );
    }

    // Validate email format if provided
    if let Some(email) = &payload.email {
        if !is_valid_email(email) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Invalid email format"})),
            );
        }
    }

    // Generate API Key: sk_live_<32_random_chars>
    let random_bytes: [u8; 24] = thread_rng().gen();
    let key_secret = hex::encode(random_bytes);
    let api_key = format!("sk_live_{}", key_secret);

    // Hash it for storage
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    let key_hash = hex::encode(hasher.finalize());

    // Store in Supabase
    let supabase = state.supabase.as_ref().unwrap();
    let body = json!({
        "key_hash": key_hash,
        "email": payload.email,
    });

    let resp = supabase
        .from("api_keys")
        .insert(body.to_string())
        .execute()
        .await;

    match resp {
        Ok(_) => (
            StatusCode::OK,
            Json(json!(RegisterResponse {
                api_key,
                message: "Store this key safely. It will not be shown again.".to_string()
            })),
        ),
        Err(e) => {
            error!("Supabase error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database error"})),
            )
        }
    }
}

async fn http_mode(
    port: u16,
    args_api_key: Option<String>, // Legacy single key
    client: Arc<StatCanClient>,
) -> anyhow::Result<()> {

    // Supabase Config
    let sb_url = std::env::var("SUPABASE_URL").ok();
    let sb_key = std::env::var("SUPABASE_KEY").ok();

    let (supabase, use_supabase) = if let (Some(url), Some(key)) = (sb_url, sb_key) {
        info!("Supabase integration enabled for Key Management");
        (
            Some(Arc::new(Postgrest::new(url).insert_header("apikey", key))),
            true,
        )
    } else {
        (None, false)
    };

    let auth_cache = Arc::new(AuthCache::new(300));

    let state = AppState {
        client,
        supabase,
        use_supabase,
        legacy_key: args_api_key,
        auth_cache,
    };

    // Rate Limiting: 60 r/m default, but stricter for /register
    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .key_extractor(SmartIpKeyExtractor)
            .per_second(2)
            .burst_size(10)
            .finish()
            .unwrap(),
    );

    let app = Router::new()
        .route("/mcp/messages", post(handle_http_message))
        .route("/mcp/sse", get(sse_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .route("/register", post(handle_register)) // Public
        .layer(TraceLayer::new_for_http())
        .layer(GovernorLayer {
            config: governor_conf,
        })
        .with_state(state);

    // ... rest of function ...
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// Auth Middleware
async fn auth_middleware(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));

    let token = match auth_header {
        Some(token) => token,
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    // Check Legacy Key first (fastest)
    if let Some(legacy_key) = &state.legacy_key {
        if constant_time_eq(token.as_bytes(), legacy_key.as_bytes()) {
            return Ok(next.run(request).await);
        }
    }

    // Check Supabase (if enabled)
    if state.use_supabase {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let key_hash = hex::encode(hasher.finalize());

        // Check Cache first
        if state.auth_cache.is_valid(&key_hash) {
            return Ok(next.run(request).await);
        }

        let client = state.supabase.as_ref().unwrap();
        let resp = client
            .from("api_keys")
            .select("id")
            .eq("key_hash", &key_hash)
            .execute()
            .await;

        match resp {
            Ok(r) => {
                let body = r.text().await.unwrap_or_else(|_| "[]".to_string());
                // If body is not empty array "[]", key exists
                if body != "[]" {
                    state.auth_cache.add(key_hash);
                    return Ok(next.run(request).await);
                }
            }
            Err(e) => {
                error!("Auth check failed: {}", e);
            }
        }
    }

    Err(StatusCode::UNAUTHORIZED)
}

async fn handle_http_message(
    State(state): State<AppState>,
    _headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    let result = handle_request(state.client.clone(), &req.method, req.params).await;
    let resp = JsonRpcResponse::from_result(result, req.id);
    Json(resp).into_response()
}

async fn sse_handler(
    State(_state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // 1. Send the endpoint event immediately so the client knows where to POST
    let endpoint_event = Event::default().event("endpoint").data("/mcp/messages");

    // 2. Keep the stream open
    let pending = stream::pending::<Result<Event, Infallible>>();

    let stream = stream::once(async { Ok(endpoint_event) }).chain(pending);

    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    use statcan_rs::{
        models::{
            Cube, CubeListResponse, CubeMetadata, CubeMetadataResponse, DataResponse, Dimension,
            Member,
        },
        Result, StatCanDataFrame, StatCanError,
    };

    struct MockStatCanClient;

    impl StatCanClientTrait for MockStatCanClient {
        async fn get_all_cubes_list_lite(&self) -> Result<CubeListResponse> {
            Ok(CubeListResponse {
                status: "SUCCESS".to_string(),
                object: Some(vec![Cube {
                    product_id: "98765432".to_string(),
                    cube_title_en: "Mocked Cube Title".to_string(),
                    cube_pid: Some("98765432".to_string()),
                }]),
            })
        }

        async fn get_cube_metadata(&self, pid: &str) -> Result<CubeMetadataResponse> {
            if pid == "error" {
                return Err(StatCanError::Api("Mocked API Error".to_string()));
            }
            Ok(CubeMetadataResponse {
                status: "SUCCESS".to_string(),
                object: Some(CubeMetadata {
                    product_id: pid.to_string(),
                    cube_title_en: "Mocked Cube".to_string(),
                    dimension: vec![Dimension {
                        dimension_name_en: "Geography".to_string(),
                        position_id: 1,
                        member: vec![Member {
                            member_id: 1,
                            member_name_en: "Canada".to_string(),
                            classification_code: Some("11124".to_string()),
                        }],
                    }],
                }),
            })
        }

        async fn find_cubes_by_dimension(
            &self,
            dim_query: &str,
            _limit: usize,
        ) -> Result<Vec<(String, String, String)>> {
            Ok(vec![(
                "12345".to_string(),
                "Found Cube".to_string(),
                dim_query.to_string(),
            )])
        }

        async fn get_data_from_vectors(
            &self,
            _vectors: Vec<String>,
            _periods: i32,
        ) -> Result<DataResponse> {
            Ok(DataResponse {
                status: "SUCCESS".to_string(),
                object: Some(vec![]),
            })
        }

        async fn get_data_from_coords(
            &self,
            _pid: &str,
            _coords: Vec<String>,
            _periods: i32,
        ) -> Result<DataResponse> {
            Ok(DataResponse {
                status: "SUCCESS".to_string(),
                object: Some(vec![]),
            })
        }

        async fn fetch_fast_snippet(&self, _pid: &str) -> Result<StatCanDataFrame> {
            Ok(StatCanDataFrame::new(DataFrame::default()))
        }

        async fn fetch_full_table(&self, _pid: &str) -> Result<StatCanDataFrame> {
            Ok(StatCanDataFrame::new(DataFrame::default()))
        }
    }

    #[test]
    fn test_email_validation() {
        // Valid emails
        assert!(is_valid_email("test@example.com"));
        assert!(is_valid_email("user.name@domain.co.uk"));
        assert!(is_valid_email("user+tag@example.com"));
        assert!(is_valid_email("1234567890@example.com"));
        assert!(is_valid_email("email@example-one.com"));

        // Invalid emails
        assert!(!is_valid_email("plainaddress"));
        assert!(!is_valid_email("#@%^%#$@#$@#.com"));
        assert!(!is_valid_email("@example.com"));
        assert!(!is_valid_email("Joe Smith <email@example.com>"));
        assert!(!is_valid_email("email.example.com"));
        assert!(!is_valid_email("email@example@example.com"));
        assert!(!is_valid_email(".email@example.com"));
        assert!(!is_valid_email("email.@example.com"));
        assert!(!is_valid_email("email..email@example.com"));
        assert!(!is_valid_email("あいうえお@example.com"));
        assert!(!is_valid_email("email@example.com (Joe Smith)"));
        assert!(!is_valid_email("email@example"));
    }

    #[tokio::test]
    async fn test_handle_request_tools_list() {
        let client = Arc::new(MockStatCanClient);
        let resp = handle_request(client, "tools/list", None).await;
        assert!(resp.is_ok());
        let val = resp.unwrap();
        let tools = val["tools"].as_array();
        assert!(tools.is_some());
        assert!(!tools.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_request_list_cubes() {
        let client = Arc::new(MockStatCanClient);
        let params = json!({
            "name": "list_cubes",
            "arguments": {}
        });
        let resp = handle_request(client, "tools/call", Some(params)).await;
        assert!(resp.is_ok());
        let val = resp.unwrap();
        let content = val["content"].as_array().unwrap();
        let text = content[0]["text"].as_str().unwrap();
        assert!(text.contains("Mocked Cube Title"));
    }

    #[tokio::test]
    async fn test_handle_request_get_metadata() {
        let client = Arc::new(MockStatCanClient);
        let params = json!({
            "name": "get_metadata",
            "arguments": { "pid": "12345" }
        });
        let resp = handle_request(client, "tools/call", Some(params)).await;
        assert!(resp.is_ok());
        let val = resp.unwrap();
        let content = val["content"].as_array().unwrap();
        let text = content[0]["text"].as_str().unwrap();
        assert!(text.contains("Mocked Cube"));
        assert!(text.contains("Geography"));
    }

    #[tokio::test]
    async fn test_handle_request_error_handling() {
        let client = Arc::new(MockStatCanClient);
        // Test missing params
        let resp = handle_request(client.clone(), "tools/call", None).await;
        assert!(resp.is_err());
        assert_eq!(resp.unwrap_err().code, -32602);

        // Test API error propagation
        let params = json!({
            "name": "get_metadata",
            "arguments": { "pid": "error" }
        });
        let resp = handle_request(client, "tools/call", Some(params)).await;
        assert!(resp.is_err());
        let err = resp.unwrap_err();
        assert_eq!(err.code, -32000);
        assert_eq!(err.message, "Internal server error");
    }

    #[tokio::test]
    async fn test_auth_middleware_legacy_key() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
            middleware,
            routing::get,
            Router,
        };
        use tower::ServiceExt;

        // Use real client since AppState requires concrete StatCanClient
        // This is safe because auth_middleware doesn't use the client.
        let client = Arc::new(StatCanClient::new().expect("Failed to create client"));
        let auth_cache = Arc::new(AuthCache::new(300));

        let state = AppState {
            client,
            supabase: None,
            use_supabase: false,
            legacy_key: Some("secret-key".to_string()),
            auth_cache,
        };

        let app = Router::new()
            .route("/", get(|| async { "OK" }))
            .route_layer(middleware::from_fn_with_state(state, auth_middleware));

        // 1. Valid Key
        let req = Request::builder()
            .uri("/")
            .header("Authorization", "Bearer secret-key")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 2. Invalid Key
        let req = Request::builder()
            .uri("/")
            .header("Authorization", "Bearer wrong-key")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_cache_hit() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
            middleware,
            routing::get,
            Router,
        };
        use tower::ServiceExt;

        // Use real client since AppState requires concrete StatCanClient
        let client = Arc::new(StatCanClient::new().expect("Failed to create client"));

        let auth_cache = Arc::new(AuthCache::new(300));
        let valid_key = "test-key";
        let mut hasher = Sha256::new();
        hasher.update(valid_key.as_bytes());
        let key_hash = hex::encode(hasher.finalize());

        // Populate Cache Manually
        auth_cache.add(key_hash);

        let state = AppState {
            client,
            supabase: None, // Missing client would cause panic if accessed
            use_supabase: true,
            legacy_key: None,
            auth_cache,
        };

        let app = Router::new()
            .route("/", get(|| async { "OK" }))
            .route_layer(middleware::from_fn_with_state(state, auth_middleware));

        // 1. Valid Key (in Cache)
        let req = Request::builder()
            .uri("/")
            .header("Authorization", format!("Bearer {}", valid_key))
            .body(Body::empty())
            .unwrap();

        // This should SUCCEED because cache is hit. If cache missed, it would hit supabase logic and panic on unwrap().
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
