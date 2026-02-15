# OpenData Job Bank MCP Server

This is a Model Context Protocol (MCP) server that provides standardized access to the Canadian Job Bank OpenData API.

## Features

- **List Datasets**: data discovery for monthly job posting datasets.
- **Search Jobs**: Advanced querying capabilities including SQL-like searches and specialized filters for Title, Location, NOC, and Salary.

## Installation

### From Source (Development)

1.  **Create a virtual environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

2.  **Install the package:**
    ```bash
    pip install .
    ```

### Usage

Run the server using:

```bash
mcp run src/server.py
```

## Tools

- `list_available_datasets`
- `search_jobs_global`
- `search_jobs_by_title`
- `search_jobs_by_location`
- `search_jobs_by_noc`
- `search_jobs_by_salary`
- `search_jobs_by_employment_type`
