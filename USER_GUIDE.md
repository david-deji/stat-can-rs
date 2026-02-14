# StatCan MCP Server User Guide

This Model Context Protocol (MCP) server exposes Statistics Canada data capabilities to AI agents and LLMs. It allows agents to search for data cubes, retrieve metadata, and fetch specific data points with filtering.

## Features

-   **Search**: Find data tables by title (e.g., "Consumer Price Index").
-   **Fetch**: Retrieve data with powerful filters:
    -   **Geography**: Filter by specific regions (e.g., "British Columbia").
    -   **Recent Months**: Get the latest N months of data.
-   **Performance**:
    -   **Local Caching**: Data is cached locally (`/tmp/statcan/`) for instant subsequent access.
    -   **Streaming**: efficient handling of large datasets without memory crashes.
    -   **Type Safety**: Robust handling of API data types.

## Installation

### Prerequisites
-   Rust (stable)
-   Network access to `www150.statcan.gc.ca`

### Build
```bash
cargo build --release --bin mcp_server
```
The binary will be located at `./target/release/mcp_server`.

## Running the Server

### 1. Stdio Mode (Default)
Ideal for integration with local LLM clients (e.g., Claude Desktop, custom scripts).

```bash
./target/release/mcp_server
```

### 2. HTTP/SSE Mode
Ideal for remote access or web-based agents.

```bash
export MCP_API_KEY="your-secure-api-key"
./target/release/mcp_server --port 3000
```
*   **Port**: Specified by `--port` or `MCP_PORT` env var.
*   **Auth**: Bearer token required via `Authorization: Bearer <key>`.
*   **Registration**: If configured, you can self-register for a key:
    ```bash
    curl -X POST http://localhost:3000/register
    # Response: {"api_key": "sk_live_..."}
    ```

## Available Tools

| Tool Name | Description | Arguments |
| :--- | :--- | :--- |
| `list_cubes` | Lists all available data cubes (summary). | None |
| `search_cubes` | Search for cubes by title. | `query` (string) |
| `get_metadata` | Get metadata for a specific cube. | `pid` (string) |
| `fetch_data_snippet` | Fetch and filter data from a cube. | `pid` (string), `geo` (optional string), `recent_months` (optional int) |
| `fetch_data_by_vector` | Fetch specific data points by Vector ID. | `vectors` (array of strings) |
| `fetch_data_by_coords` | Fetch specific data points by Coordinate. | `pid` (string), `coords` (array of strings) |

## Example Usage (AI Agent)

An example Python script `demo_agent.py` is provided to demonstrate an AI agent workflow:

1.  **Search**: `search_cubes(query="Consumer Price Index")`
2.  **Select**: Agent picks PID `18100004`.
3.  **Fetch**: `fetch_data_snippet(pid="18100004", geo="British Columbia", recent_months=12)`
4.  **Result**: JSON data containing the last year of CPI values for BC.

## Docker Support

A `Dockerfile` is included for containerized deployment.

```bash
docker build -t stat-can-mcp .
docker run -p 3000:3000 -e MCP_API_KEY="test" stat-can-mcp
```
