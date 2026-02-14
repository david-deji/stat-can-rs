use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures::Stream;
use polars::prelude::SerWriter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use statcan_rs::StatCanClient;
use std::{convert::Infallible, io::BufRead, sync::Arc};
use tokio::sync::broadcast;
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
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
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
                },
                // Add more tools here
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
                    let matches: Vec<_> = all_cubes
                        .into_iter()
                        .filter(|c| {
                            c.cube_title_en
                                .to_lowercase()
                                .contains(&query.to_lowercase())
                        })
                        .take(20) // Limit results
                        .collect();

                    Ok(
                        json!({ "content": [{ "type": "text", "text": serde_json::to_string_pretty(&matches).unwrap() }] }),
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
struct AppState {
    client: Arc<StatCanClient>,
    api_key: Option<String>,
    sender: broadcast::Sender<String>, // For SSE broadcast (if we had server-initiated events)
}

async fn http_mode(
    port: u16,
    api_key: Option<String>,
    client: Arc<StatCanClient>,
) -> anyhow::Result<()> {
    let (tx, _rx) = broadcast::channel(100);
    let state = AppState {
        client,
        api_key,
        sender: tx,
    };

    let app = Router::new()
        .route("/mcp/messages", post(handle_http_message))
        .route("/sse", get(sse_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_http_message(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    // Auth Check
    if let Some(ref key) = state.api_key {
        let auth_header = headers
            .get("Authorization")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        if auth_header != format!("Bearer {}", key) {
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }

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
