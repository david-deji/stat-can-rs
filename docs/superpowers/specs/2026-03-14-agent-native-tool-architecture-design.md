# Design Spec: Agent-Native Tool Architecture for statcan-rs

**Date:** 2026-03-14
**Status:** Draft
**Topic:** Reducing tool complexity to make AI agents first-class citizens in the StatCan/OpenData ecosystem.

## 1. Executive Summary
The current `statcan-rs` MCP server provides a large number of specialized tools (14+) that are split by source (StatCan vs. Open Data) and access method (Vectors, Coordinates, Snippets). This creates high cognitive load for AI agents and limits "Emergent Capability." 

This design implements an **Agent-Native Architecture** based on the "Resource Explorer" pattern, reducing the toolset to three high-power primitives that allow agents to discover, understand, and query any economic dataset in Canada through a unified interface.

## 2. Goals & Success Criteria
- **Parity:** The agent can achieve anything a human can do through the StatCan web portal.
- **Granularity:** Tools are atomic primitives; features are outcomes achieved by agents in a loop.
- **Composability:** New economic analysis features (e.g., "CPI vs. Wage growth") can be built by agents using prompts alone.
- **Simplicity:** Reduce tool surface area by >70% while increasing total capability.

## 3. Tool Architecture (The Three Primitives)

### 3.1 `discover_datasets`
**Purpose:** Unified search across StatCan and Canadian Open Government portals.
- **Input:** 
  - `query` (string): Fuzzy search term (e.g., "inflation", "housing starts").
  - `limit` (integer, default: 10).
- **Behavior:** 
  - Hits both CKAN and StatCan indices concurrently.
  - Normalizes results into a common format: `id`, `title`, `source`, `type`.
  - Flags "Best Source" based on data freshness/completeness.
- **Outcome:** The agent identifies a target `dataset_id`.

### 3.2 `inspect_dataset`
**Purpose:** Understand the dimensional shape and "language" of a dataset.
- **Input:** 
  - `dataset_id` (string).
- **Behavior:**
  - Retrieves full metadata for the target ID.
  - Returns:
    - **Schema:** Column names and data types.
    - **Affinities:** Identifies which columns are Temporal (Date) and which are Geography.
    - **Members:** Provides unique samples for each dimension (e.g., which Provinces are available).
    - **Unit/Scale:** Clarifies if values are "Thousands," "Index," or "Percent."
- **Outcome:** The agent knows *exactly* which filters to apply in the next step.

### 3.3 `query_data`
**Purpose:** The universal data bridge for fetching structured records.
- **Input:**
  - `dataset_id` (string).
  - `filters` (object): Key-value pairs matching dimension members (fuzzy matching permitted).
  - `date_range` (object): Support for `start`, `end`, or `last_n_periods`.
  - `format` (enum: `json`, `csv`).
- **Behavior:**
  - Routes the request to the appropriate back-end (StatCan API or CKAN Datastore).
  - Performs server-side filtering and slicing using the Rust/Polars engine.
  - Returns standardized JSON/CSV records.
- **Outcome:** The agent receives high-quality data for its analysis.

## 4. Implementation Details (Rust/MCP)
- **Normalizers:** We will implement a `DiscoveryNormalizer` trait to unify CKAN and StatCan search results.
- **Fuzzy Resolution:** Leverage the existing `wrapper.rs` logic for case-insensitive and substring column resolution.
- **Error Handling:** Map all source errors to standard MCP JSON-RPC error codes. Provide helpful suggestions in the error message (e.g., "No match for 'Ont.'; did you mean 'Ontario'?").

## 5. Migration Plan
1. Deprecate existing specialized tools (but keep them hidden for backward compatibility if needed).
2. Implement the Three Primitives.
3. Update the System Prompt to guide agents through the "Discover -> Inspect -> Query" workflow.
4. Verify by tasking a "new" agent with a complex query: *"Fetch the correlation between Ontario Rental prices and National Inflation for the last 12 months."*
