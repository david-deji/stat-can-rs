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
use futures::Stream;
use polars::prelude::SerWriter;
use postgrest::Postgrest;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use statcan_rs::StatCanClient;
use std::{convert::Infallible, io::BufRead, sync::Arc};
use tokio::sync::broadcast;
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
        info!("Starting MCP server in Stdio mode");
        stdio_mode(client).await?;
    }

    Ok(())
}

// --- Protocol Types ---

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JsonRpcRequest {
    jsonrpc: String,
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

// --- Core Logic ---

async fn handle_request(
    client: Arc<StatCanClient>,
    method: &str,
    params: Option<Value>,
) -> Result<Value, JsonRpcError> {
    match method {
        "list_tools" => Ok(json!({
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
                            "pid": { "type": "string", "description": "The Product ID" }
                        },
                        "required": ["pid"]
                    }
                },
                {
                    "name": "search_cubes",
                    "description": "Search for cubes by title",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string", "description": "Search query" }
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
                            "vectors": { "type": "array", "items": { "type": "string" }, "description": "List of Vector IDs" }
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
                            "coords": { "type": "array", "items": { "type": "string" }, "description": "List of Coordinate strings" }
                        },
                        "required": ["pid", "coords"]
                    }
                },
                {
                    "name": "fetch_data_snippet",
                    "description": "Fetch data from a cube. Supports filtering by Geography and getting the most recent N periods.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pid": { "type": "string", "description": "The Product ID" },
                            "rows": { "type": "integer", "description": "Number of rows to return (default 5, ignored if recent_months is set)" },
                            "geo": { "type": "string", "description": "Filter by Geography (e.g. 'Canada', 'British Columbia')" },
                            "recent_months": { "type": "integer", "description": "Get the last N months/periods (sorts by date descending)" }
                        },
                        "required": ["pid"]
                    }
                }
            ]
        })),
        "call_tool" => {
            let params = params.ok_or(JsonRpcError {
                code: -32602,
                message: "Missing params".to_string(),
            })?;
            let name = params["name"].as_str().ok_or(JsonRpcError {
                code: -32602,
                message: "Missing tool name".to_string(),
            })?;
            let args = &params["arguments"];

            match name {
                "list_cubes" => {
                    let resp =
                        client
                            .get_all_cubes_list_lite()
                            .await
                            .map_err(|e| JsonRpcError {
                                code: -32000,
                                message: e.to_string(),
                            })?;
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
                    let pid = args["pid"].as_str().ok_or(JsonRpcError {
                        code: -32602,
                        message: "Missing pid".to_string(),
                    })?;
                    let resp = client
                        .get_cube_metadata(pid)
                        .await
                        .map_err(|e| JsonRpcError {
                            code: -32000,
                            message: e.to_string(),
                        })?;
                    Ok(
                        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
                    )
                }
                "get_cube_dimensions" => {
                    let pid = args["pid"].as_str().ok_or(JsonRpcError {
                        code: -32602,
                        message: "Missing pid".to_string(),
                    })?;
                    let resp = client
                        .get_cube_metadata(pid)
                        .await
                        .map_err(|e| JsonRpcError {
                            code: -32000,
                            message: e.to_string(),
                        })?;

                    let metadata = resp.object.ok_or(JsonRpcError {
                        code: -32000,
                        message: "Table not found".to_string(),
                    })?;

                    // Simplify output: Map Dimension Name -> List of Member Names
                    let simplified: std::collections::HashMap<String, Vec<String>> = metadata
                        .dimension
                        .into_iter()
                        .map(|d| {
                            let members = d.member.into_iter().map(|m| m.member_name_en).collect();
                            (d.dimension_name_en, members)
                        })
                        .collect();

                    Ok(
                        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&simplified).unwrap() }] }),
                    )
                }
                "search_cubes" => {
                    let query = args["query"].as_str().ok_or(JsonRpcError {
                        code: -32602,
                        message: "Missing query".to_string(),
                    })?;
                    let resp =
                        client
                            .get_all_cubes_list_lite()
                            .await
                            .map_err(|e| JsonRpcError {
                                code: -32000,
                                message: e.to_string(),
                            })?;

                    let all_cubes = resp.object.unwrap_or_default();
                    let results: Vec<&statcan_rs::models::Cube> = all_cubes
                        .iter()
                        .filter(|c| {
                            c.cube_title_en
                                .to_lowercase()
                                .contains(&query.to_lowercase())
                        })
                        .collect();
                    Ok(
                        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&results).unwrap() }] }),
                    )
                }
                "fetch_data_by_vector" => {
                    let vectors_val = args["vectors"].as_array().ok_or(JsonRpcError {
                        code: -32602,
                        message: "Missing vectors array".to_string(),
                    })?;
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

                    let resp =
                        client
                            .get_data_from_vectors(vectors)
                            .await
                            .map_err(|e| JsonRpcError {
                                code: -32000,
                                message: e.to_string(),
                            })?;
                    Ok(
                        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
                    )
                }
                "fetch_data_by_coords" => {
                    let pid = args["pid"].as_str().ok_or(JsonRpcError {
                        code: -32602,
                        message: "Missing pid".to_string(),
                    })?;
                    let coords_val = args["coords"].as_array().ok_or(JsonRpcError {
                        code: -32602,
                        message: "Missing coords array".to_string(),
                    })?;
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

                    let resp = client
                        .get_data_from_coords(pid, coords)
                        .await
                        .map_err(|e| JsonRpcError {
                            code: -32000,
                            message: e.to_string(),
                        })?;
                    Ok(
                        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&resp.object).unwrap() }] }),
                    )
                }
                "fetch_data_snippet" => {
                    let pid = args["pid"].as_str().ok_or(JsonRpcError {
                        code: -32602,
                        message: "Missing pid".to_string(),
                    })?;
                    let rows = args["rows"].as_u64().unwrap_or(5) as usize;
                    let geo = args["geo"].as_str();
                    let recent_months = args["recent_months"].as_u64(); // Option<u64>

                    let mut df_wrapper =
                        client
                            .fetch_full_table(pid)
                            .await
                            .map_err(|e| JsonRpcError {
                                code: -32000,
                                message: e.to_string(),
                            })?;

                    // Filter by Geography if provided
                    if let Some(g) = geo {
                        df_wrapper = df_wrapper.filter_geo(g).map_err(|e| JsonRpcError {
                            code: -32000,
                            message: e.to_string(),
                        })?;
                    }

                    // Filter recent months (Sort Descending by Date + Take N)
                    if let Some(n) = recent_months {
                        df_wrapper = df_wrapper.sort_date(true).map_err(|e| JsonRpcError {
                            code: -32000,
                            message: e.to_string(),
                        })?;
                        df_wrapper = df_wrapper.take_n(n as usize).map_err(|e| JsonRpcError {
                            code: -32000,
                            message: e.to_string(),
                        })?;
                    } else {
                        // Default behavior: just take head of whatever order (or use rows arg)
                        df_wrapper = df_wrapper.take_n(rows).map_err(|e| JsonRpcError {
                            code: -32000,
                            message: e.to_string(),
                        })?;
                    }

                    // Format output as JSON
                    let mut df = df_wrapper.into_polars();
                    let mut buf = Vec::new();
                    polars::prelude::JsonWriter::new(&mut buf)
                        .with_json_format(polars::prelude::JsonFormat::Json)
                        .finish(&mut df)
                        .map_err(|e| JsonRpcError {
                            code: -32000,
                            message: format!("Serialization error: {}", e),
                        })?;

                    let output = String::from_utf8(buf).map_err(|e| JsonRpcError {
                        code: -32000,
                        message: format!("UTF-8 error: {}", e),
                    })?;
                    Ok(json!({ "content": [{ "type": "text", "text": output }] }))
                }
                _ => Err(JsonRpcError {
                    code: -32601,
                    message: "Method not found".to_string(),
                }),
            }
        }
        _ => Err(JsonRpcError {
            code: -32601,
            message: "Method not found".to_string(),
        }),
    }
}

// --- Stdio Mode ---

async fn stdio_mode(client: Arc<StatCanClient>) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut handle = stdin.lock();
    let mut line = String::new();

    while handle.read_line(&mut line)? > 0 {
        if line.trim().is_empty() {
            line.clear();
            continue;
        }

        let req: Result<JsonRpcRequest, _> = serde_json::from_str(&line);
        match req {
            Ok(req) => {
                let result = handle_request(client.clone(), &req.method, req.params).await;
                let resp = match result {
                    Ok(res) => JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        result: Some(res),
                        error: None,
                        id: req.id,
                    },
                    Err(err) => JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        result: None,
                        error: Some(err),
                        id: req.id,
                    },
                };
                println!("{}", serde_json::to_string(&resp)?);
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

#[derive(Clone)]
#[allow(dead_code)]
struct AppState {
    client: Arc<StatCanClient>,
    sender: broadcast::Sender<String>,
    supabase: Option<Arc<Postgrest>>,
    use_supabase: bool,
    legacy_key: Option<String>,
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
    let (tx, _rx) = broadcast::channel(100);

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

    let state = AppState {
        client,
        sender: tx,
        supabase,
        use_supabase,
        legacy_key: args_api_key,
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
        if token == legacy_key {
            return Ok(next.run(request).await);
        }
    }

    // Check Supabase (if enabled)
    if state.use_supabase {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let key_hash = hex::encode(hasher.finalize());

        let client = state.supabase.as_ref().unwrap();
        let resp = client
            .from("api_keys")
            .select("id")
            .eq("key_hash", key_hash)
            .execute()
            .await;

        match resp {
            Ok(r) => {
                let body = r.text().await.unwrap_or_else(|_| "[]".to_string());
                // If body is not empty array "[]", key exists
                if body != "[]" {
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

    let resp = match result {
        Ok(res) => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(res),
            error: None,
            id: req.id,
        },
        Err(err) => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(err),
            id: req.id,
        },
    };

    Json(resp).into_response()
}

async fn sse_handler(
    State(_state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Basic SSE implementation - mostly a placeholder as MCP primarily uses request/response
    // But protocol allows server notifications.
    let stream = futures::stream::iter(vec![]); // Empty for now
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}
