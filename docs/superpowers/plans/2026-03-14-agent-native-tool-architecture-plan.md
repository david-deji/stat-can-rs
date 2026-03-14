# Implementation Plan: Agent-Native Tool Architecture

## Feature: Consolidating and Rebuilding StatCan and OpenData Tools into Primitives

### Overview
We are transitioning `statcan-rs` from a complex suite of 14+ specific tools to three powerful, agent-native primitives: `discover_datasets`, `inspect_dataset`, and `query_data`. This makes the AI agents using the MCP server "first-class citizens" by empowering them to discover, inspect, and query without needing to know specific CANSIM/CKAN schemas upfront.

### Architecture Decisions
- **DiscoveryNormalizer Trait:** We will create a trait in `src/data_helpers.rs` to normalize the outputs of both the StatCan API and the CKAN Open Data portal into a unified `DatasetSummary` format for `discover_datasets`.
- **Three-Tool Surface Area:** The `list_tools` endpoint in `src/handlers.rs` will be updated to only surface the 3 new primitives, deprecating old tools.

### Implementation Tasks (Jules Workstreams)

#### Workstream 1: Discovery Tool and Normalizer
**Delegate Command:** `jules new "Implement discover_datasets MCP tool and DiscoveryNormalizer trait. Read docs/superpowers/specs/2026-03-14-agent-native-tool-architecture-design.md for context."`
- **Files**: `src/data_helpers.rs`, `src/handlers.rs`
- **Description**: Implement a unified discovery mechanism.
- **Details**:
  - Add a `DiscoveryNormalizer` trait or set of functions in `src/data_helpers.rs` to convert `models::Cube` (StatCan) and `models::Package` (CKAN) into a unified `DatasetSummary` struct.
  - Implement the `discover_datasets(query: String, limit: usize)` handler in `src/handlers.rs`. It should execute `get_all_cubes_list_lite()` and `search_packages()`, aggregate, assign a relevance score, and return the top `limit` results.
- **Dependencies**: None.

#### Workstream 2: Inspection Tool
**Delegate Command:** `jules new "Implement inspect_dataset MCP tool taking a dataset_id. Determine if it's StatCan or CKAN, fetch metadata and schema, and return dimensions/members. See docs/superpowers/specs/2026-03-14-agent-native-tool-architecture-design.md"`
- **File**: `src/handlers.rs`
- **Description**: Build the tool that answers "What is the shape and language of this dataset?"
- **Details**:
  - Implement `inspect_dataset(dataset_id: String)`.
  - If `dataset_id` is a StatCan PID (numbers only, e.g. "18100004"), call `get_cube_metadata`. Return the dimensions list, the "temporal" column (usually `REF_DATE`), and the GEO column.
  - If `dataset_id` is an OpenData Resource ID (UUID format), fetch the schema via `get_open_data_resource_schema`.
- **Dependencies**: None.

#### Workstream 3: Universal Query Tool and Tool Consolidation
**Delegate Command:** `jules new "Implement the universal query_data tool in statcan-rs and update list_tools to ONLY serve discover_datasets, inspect_dataset, and query_data. Deprecate all other tools."`
- **Files**: `src/handlers.rs`, `src/wrapper.rs`
- **Description**: Bring all data querying into a single function and clean up the `list_tools` manifest.
- **Details**:
  - Implement `query_data(dataset_id: String, filters: Option<Map>, date_range: Option<Value>, format: Option<String>)`.
  - Route the query to either StatCan `fetch_full_table_scan` (or fast snippet) or CKAN `fetch_open_data_resource_snippet` based on the ID shape.
  - Apply filters using the existing Polars `StatCanDataFrame` logic in `src/wrapper.rs`.
  - Update `list_tools` to expose *only* `discover_datasets`, `inspect_dataset`, and `query_data`. Remove the legacy definitions.
- **Dependencies**: Task 1 and Task 2.

### Testing Strategy
- **Unit Tests**: Ensure the `DiscoveryNormalizer` correctly maps fields in `src/data_helpers.rs`.
- **Integration Tests**: Add a test in `src/handlers.rs` invoking `handle_request` with the new methodologies.

### Integration Points
- This heavily impacts the `list_tools` manifest. Agents currently relying on the old tool names (like `search_cubes`) will break if they don't dynamically read the server capabilities. This is acceptable per the new agent-native vision.
