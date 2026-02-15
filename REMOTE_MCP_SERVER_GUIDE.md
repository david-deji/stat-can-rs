# Remote MCP Server Implementation Guide

This document summarizes the findings and technical requirements for successfully deploying a remote Model Context Protocol (MCP) server that is compatible with modern clients like **Claude Code**, **Antigravity**, and **Claude Desktop**.

## 1. Unified Transport Endpoint (/mcp)
Modern MCP clients often attempt to use **Streamable HTTP** (aka "standalone SSE") as the transport protocol. 
- **Requirement**: The server must expose a single endpoint (e.g., `/mcp`) that handles both `GET` and `POST` requests.
- **GET /mcp**: This should initiate the Server-Sent Events (SSE) stream.
- **POST /mcp**: This is used for sending JSON-RPC messages (tools/call, etc.).
- **SSE "endpoint" event**: When the client connects via `GET`, the server *may* send an `endpoint` event. However, for many Go-based clients (like the one in Antigravity), **this event must be valid JSON**.
  - **Bad**: `data: http://localhost:3000/mcp`
  - **Good**: `data: "http://localhost:3000/mcp"` (Properly quoted string)
  - **Note**: In our case, we found that simply relying on the client to POST back to the same URL used for the SSE connection was more stable than sending the `endpoint` event.

## 2. Strict JSON-RPC 2.0 Compliance
Some MCP clients are extremely strict about the JSON-RPC response structure.
- **Constraint**: A response must contain **either** a `result` field **or** an `error` field, but **should not contain both** even if one is null.
- **Implementation**: In Rust (Axum/Serde), use `#[serde(skip_serializing_if = "Option::is_none")]` on the `result`, `error`, and `id` fields.
- **Notification Handling**: Notifications (requests without an `id`) should ideally return a `204 No Content` or `200 OK` with an empty body, rather than a full JSON-RPC response with a null ID.

## 3. Session Management (Mcp-Session-Id)
The MCP HTTP spec requires a session ID to track stateful interactions (like the `initialize` handshake).
- **Requirement**: During the `initialize` request handling, the server MUST generate a unique session ID and return it in the **`Mcp-Session-Id`** HTTP header.
- **Client behavior**: Subsequent POST requests from the client will include this header to identify the session.

## 4. Proxies and Buffering
When deploying to platforms like **Render** or behind **Cloudflare/Nginx**:
- **SSE Buffering**: Proxies often buffer responses, which "kills" SSE streams.
- **Fix**: The server must send the following headers with the SSE response:
  - `X-Accel-Buffering: no`
  - `Cache-Control: no-cache`
  - `Connection: keep-alive`
- **Rate Limiting**: Rate limiters that rely on "Smart IP Extraction" (like `tower-governor`) often fail inside Docker or behind proxies because they cannot extract a trusted client IP.
  - **Recommendation**: Disable or simplify rate limiting during the initial connection phase to avoid `500 Internal Server Error`.

## 5. Authentication Challenges
- **Claude Code Bug**: As of Feb 2026, the `claude mcp add` command for remote servers often ignores the `Authorization: Bearer` header and attempts an OAuth2 handshake instead.
- **Debugging Tip**: Temporarily disable server-side authentication (e.g., bypass `auth_middleware`) to verify that transport and JSON-RPC layers are working before re-enabling security.
- **CORS**: Ensure your server allows `OPTIONS` requests and handles CORS headers properly if the MCP client is running in a browser-like environment.

## 6. Cold Starts
- **Behavior**: On platforms like Render (Free Tier), the server spins down after 15 minutes.
- **Symptom**: The first tool call from the IDE may timeout or report "Connection Closed."
- **Recovery**: Manually "waking" the server with a `curl` or simply retrying the tool call after 30 seconds usually resolves the issue.

## 7. Configuration Example (Antigravity)
In `mcp_config.json`:
```json
{
  "mcpServers": {
    "statcan-remote": {
      "serverUrl": "https://your-app.onrender.com/mcp"
    }
  }
}
```
*Note: We found that removing explicit headers and relying on the unified `/mcp` path provided the best compatibility.*
