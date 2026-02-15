# Universal CKAN-MCP Strategy: From StatCan-Specific to Global Data Standard

## 1. Architectural Pivot: The `CKANDriver` Trait System

To transition from a StatCan-specific tool to a universal CKAN interface while preserving high-performance optimizations, we will adopt a **Driver/Adapter Pattern**.

### Core Abstraction: `CKANClient` Trait
The `StatCanClient` struct will be refactored into a trait, allowing multiple implementations to coexist. This ensures that specialized logic for Statistics Canada (WDS API) remains intact while opening the door for a generic CKAN driver.

```rust
#[async_trait]
pub trait CKANClient: Send + Sync {
    /// Initial handshake/health check
    async fn ping(&self) -> Result<String>;

    /// Semantic Discovery: Search for datasets (packages) across portals
    async fn search_packages(&self, query: &str, limit: usize) -> Result<Vec<PackageMetadata>>;

    /// Deep Metadata Retrieval: Get full details for a specific dataset
    async fn get_package_metadata(&self, id: &str) -> Result<PackageMetadata>;

    /// Data Access Strategy:
    /// Returns a `DataHandler` enum which can be either a Direct Query (Datastore)
    /// or a Blob Download (CSV/Parquet) for local Polars processing.
    async fn get_resource_handler(&self, resource_id: &str) -> Result<DataHandler>;
}

pub enum DataHandler {
    DatastoreQuery(String), // SQL-like endpoint for precise queries
    BlobDownload(String),   // URL for bulk download (CSV, ZIP, Parquet)
}
```

### Drivers

1.  **`StatCanDriver` (Legacy/Specialized):**
    *   **Endpoint:** `https://www150.statcan.gc.ca/t1/wds/rest`
    *   **Capabilities:** Optimized for WDS cubes, vector lookups, and coordinate-based access.
    *   **Data Access:** Primarily `BlobDownload` (CSV/ZIP) with local Polars caching, as WDS doesn't expose a standard CKAN Datastore SQL interface.

2.  **`GenericCKANDriver` (New/Standard):**
    *   **Endpoint:** Dynamic (e.g., `https://catalog.data.gov`, `https://data.humdata.org`).
    *   **Capabilities:** Uses standard CKAN API v3 (`package_search`, `package_show`, `datastore_search_sql`).
    *   **Data Access:** Hybrid. It first checks for `datastore_active: true` to offer `DatastoreQuery`. If unavailable, it falls back to `BlobDownload` for Polars ingestion.

---

## 2. Advisory on "Juicy Data" Features & Ingestion Strategy

To make data "tasty" for LLMs (context-rich, metadata-heavy), we leverage specific CKAN features:

### A. The "Hot Path": CKAN Datastore API (SQL)
*   **Why:** Many modern CKAN instances (like `data.gov`) index CSVs into a PostgreSQL backend (DataStore).
*   **Strategy:** Instead of downloading a 1GB file to find 5 rows, the MCP agent can execute **SQL queries** directly against the portal.
*   **Agentic Tool:** `query_datastore(resource_id, sql_query)`
*   **Benefit:** Zero latency, minimal context usage, precise answers.

### B. The "Cold Path": Polars In-Memory Cache (The "Killer Feature")
*   **Why:** Datastore is often missing or disabled. Or the user needs *analytical* power (aggregations, pivots) that the Datastore API limits.
*   **Strategy:** Maintain the current architecture where the MCP downloads the raw resource (CSV, Parquet, Excel) into a local `statcan/` cache directory.
*   **Agentic Tool:** `analyze_dataset(resource_id, analysis_code)`
*   **Benefit:** Full power of Polars. The MCP can "read" the whole dataset and summarize it for the LLM.

### C. Semantic Discovery (Metadata)
*   **Strategy:** Index `notes`, `title`, `tags`, and `extras` fields from CKAN.
*   **LLM "Hook":** When an LLM asks "What data do we have on housing?", the MCP searches these fields across configured portals.
*   **Data Density:** We strip verbose HTML from `notes` and present a concise "Semantic Header" to the LLM.

---

## 3. Problem-Solving Framework: Decision Matrix

Handling heterogeneous data formats is the main challenge. The MCP will follow this decision logic:

1.  **Discovery Phase:**
    *   User asks for data.
    *   MCP calls `search_packages`.
    *   **Result:** List of `PackageMetadata`.

2.  **Assessment Phase (Per Resource):**
    *   **Check 1: Is Datastore Active?** (`datastore_active: true`)
        *   **YES:** Prefer `DatastoreQuery` mode. Offer SQL tool to Agent.
        *   **NO:** Proceed to Check 2.
    *   **Check 2: File Format?**
        *   **CSV/Parquet/JSON:** Compatible with Polars.
        *   **YES:** Prefer `BlobDownload` mode. Offer "Download & Analyze" tool.
        *   **Excel/XML:** Requires specialized parsing (maintain existing libraries or suggest conversion).
    *   **Check 3: Size?**
        *   **> 100MB:** Warn user. Prefer Datastore if possible. If strict Download mode, stream to disk (don't load fully into RAM unless necessary).

---

## 4. AI-Friendly Output: "Data Density"

To prevent "hallucinations" and ensure "grounding," output must be optimized for the context window.

1.  **Semantic Headers:**
    Before returning *any* data rows, inject a metadata block:
    ```markdown
    ---
    Dataset: "US Housing Starts 2020-2024"
    Source: data.gov (Official)
    Last Updated: 2024-05-01
    Columns: [Date (Date), Starts (Int), Region (Cat)]
    Completeness: 99.8% (rows with nulls dropped)
    ---
    ```

2.  **Smart Truncation (JSON-L vs. Markdown):**
    *   **Small Result (< 50 rows):** Render as a Markdown Table for readability.
    *   **Large Result (> 50 rows):** Render as compact JSON-L (one object per line) or a specialized "Columnar Summary" (e.g., "Mean: 50, Max: 100").
    *   **Polars Integration:** Use Polars' `describe()` feature to send *statistical summaries* instead of raw rows when the dataset is huge.

---

## 5. Next Steps

1.  **Refactor `lib.rs`:** Introduce the `CKANClient` trait.
2.  **Implement `GenericCKANDriver`:** Focus on `data.gov` API compatibility.
3.  **Enhance Tooling:** Add `query_datastore` tool for SQL-over-HTTP.
