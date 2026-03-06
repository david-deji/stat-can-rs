use axum::{
    extract::{Json, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::post,
    Router,
};
use clap::Parser;
use futures::stream::{self, StreamExt};
use governor::{Quota, RateLimiter};
use statcan_rs::handlers::{handle_request, JsonRpcRequest, JsonRpcResponse};
use statcan_rs::StatCanClient;
use std::{
    collections::HashMap,
    convert::Infallible,
    io::BufRead,
    num::NonZeroU32,
    sync::{Arc, RwLock},
    time::Instant,
};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on (if not set, runs in stdio mode)
    #[arg(short, long, env = "PORT")]
    port: Option<u16>,

    /// Transport mode: stdio (default) or sse
    #[arg(long, env = "ONET_TRANSPORT")]
    transport: Option<String>,

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
    let open_data_client = Arc::new(statcan_rs::GenericCKANDriver::new(
        "https://open.canada.ca/data/en",
    )?);

    // Determine mode
    let is_sse = args.transport.as_deref() == Some("sse") || args.port.is_some();

    if is_sse {
        let port = args.port.unwrap_or(3000);
        info!("Starting MCP server in HTTP/SSE mode on port {}", port);
        run_sse_server(port, args.api_key, client, open_data_client).await?;
    } else {
        info!(
            "Starting MCP server in Stdio mode (Rate Limit: {}/min)",
            args.rate_limit
        );
        run_stdio_server(client, open_data_client, args.rate_limit).await?;
    }

    Ok(())
}

// --- Stdio Mode ---

async fn run_stdio_server(
    client: Arc<StatCanClient>,
    od_client: Arc<statcan_rs::GenericCKANDriver>,
    rate_limit_per_min: u32,
) -> anyhow::Result<()> {
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
            limiter.until_ready().await;
        }

        eprintln!("DEBUG: Received line: {:?}", line); // Debugging input
        let req: Result<JsonRpcRequest, _> = serde_json::from_str(&line);
        match req {
            Ok(req) => {
                let is_notification = req.id.is_none();
                let result =
                    handle_request(client.clone(), od_client.clone(), &req.method, req.params)
                        .await;

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
            }
        }

        line.clear();
    }
    Ok(())
}

// --- SSE Mode ---

#[allow(dead_code)]
struct Session {
    id: String,
    created_at: Instant,
}

struct AppState {
    client: Arc<StatCanClient>,
    open_data_client: Arc<statcan_rs::GenericCKANDriver>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    api_key: Option<String>,
}

async fn run_sse_server(
    port: u16,
    api_key: Option<String>,
    client: Arc<StatCanClient>,
    open_data_client: Arc<statcan_rs::GenericCKANDriver>,
) -> anyhow::Result<()> {
    let state = AppState {
        client,
        open_data_client,
        sessions: Arc::new(RwLock::new(HashMap::new())),
        api_key,
    };

    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_headers(tower_http::cors::Any)
        .expose_headers(["Mcp-Session-Id".parse::<axum::http::HeaderName>().unwrap()]);

    let app = Router::new()
        .route(
            "/sse",
            post(handle_sse_post)
                .get(handle_sse_get)
                .delete(handle_sse_delete),
        )
        .route("/messages", post(handle_sse_post)) // Legacy alias?
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(Arc::new(state));

    // Bind to dual-stack
    let addr = format!("[::]:{}", port);
    info!("Listening on {}", addr);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(_) => {
            // Fallback to IPv4 if IPv6 fails
            let addr4 = format!("0.0.0.0:{}", port);
            info!("IPv6 bind failed, falling back to {}", addr4);
            TcpListener::bind(&addr4).await?
        }
    };

    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_sse_post(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    let is_initialize = req.method == "initialize";
    let is_notification = req.id.is_none();

    let session_id = if is_initialize {
        let new_id = Uuid::new_v4().to_string();
        let mut sessions = state.sessions.write().unwrap();
        sessions.insert(
            new_id.clone(),
            Session {
                id: new_id.clone(),
                created_at: Instant::now(),
            },
        );
        Some(new_id)
    } else {
        // Validate session
        if let Some(id_val) = headers.get("mcp-session-id") {
            if let Ok(id) = id_val.to_str() {
                let sessions = state.sessions.read().unwrap();
                if sessions.contains_key(id) {
                    Some(id.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    };

    if !is_initialize && session_id.is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            "Missing or invalid Mcp-Session-Id header",
        )
            .into_response();
    }

    // Auth Check
    if let Some(ref key) = state.api_key {
        let auth_header = headers
            .get("x-api-key")
            .or_else(|| headers.get("authorization"))
            .and_then(|h| h.to_str().ok());

        let authorized = match auth_header {
            Some(h) => {
                let token = h.strip_prefix("Bearer ").unwrap_or(h);
                if token.len() == key.len() {
                    constant_time_eq::constant_time_eq(token.as_bytes(), key.as_bytes())
                } else {
                    false
                }
            }
            None => false,
        };

        if !authorized {
            error!("Auth failed.");
            return (StatusCode::UNAUTHORIZED, "Invalid API Key").into_response();
        }
    }

    let result = handle_request(
        state.client.clone(),
        state.open_data_client.clone(),
        &req.method,
        req.params,
    )
    .await;

    if is_notification {
        return StatusCode::ACCEPTED.into_response();
    }

    let resp = JsonRpcResponse::from_result(result, req.id);
    let mut response = Json(resp).into_response();

    response
        .headers_mut()
        .insert("Content-Type", HeaderValue::from_static("application/json"));

    if let Some(sid) = session_id {
        response
            .headers_mut()
            .insert("Mcp-Session-Id", HeaderValue::from_str(&sid).unwrap());
    }

    response
}

async fn handle_sse_get(
    State(_state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if headers.get("mcp-session-id").is_some() {
        // Streamable HTTP: no notification stream needed (unless we implement server push)
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    // Legacy SSE: create session + stream with endpoint event
    // For legacy clients that expect SSE immediately without Mcp-Session-Id header on GET
    // We should probably just return an endpoint event pointing to POST /sse

    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost:3000");
    let scheme = if host.contains("localhost") || host.contains("127.0.0.1") {
        "http"
    } else {
        "https"
    };
    let endpoint_url = format!("{}://{}/sse", scheme, host);

    // Initial event: endpoint
    let endpoint_event = Event::default().event("endpoint").data(format!(
        "{}?sessionId={}",
        endpoint_url,
        Uuid::new_v4()
    )); // Legacy clients might expect sessionId param

    // Keep the stream open but send nothing else
    let pending = stream::pending::<Result<Event, Infallible>>();
    let stream = stream::once(async { Ok(endpoint_event) }).chain(pending);

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn handle_sse_delete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(id_val) = headers.get("mcp-session-id") {
        if let Ok(id) = id_val.to_str() {
            let mut sessions = state.sessions.write().unwrap();
            if sessions.remove(id).is_some() {
                return StatusCode::OK.into_response();
            }
        }
    }
    StatusCode::NOT_FOUND.into_response()
}
