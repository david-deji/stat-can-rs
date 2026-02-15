from mcp.server.fastmcp import FastMCP
from .client import OpenDataClient
import asyncio
import json
from typing import Optional

# Initialize FastMCP Server
mcp = FastMCP("OpenDataJobBank")
client = OpenDataClient()

@mcp.tool()
async def list_available_datasets() -> str:
    """
    Lists available monthly job posting datasets from the Canadian Job Bank.
    Returns details including Resource ID, Language, and Date.
    """
    resources = await client.list_available_resources()
    return json.dumps(resources, indent=2)

async def _get_resource_id(resource_id: Optional[str]) -> str:
    """Helper to resolve resource ID or get latest English one."""
    if resource_id:
        return resource_id
    # TODO: Cache this result?
    return await client.get_latest_english_resource_id()

@mcp.tool()
async def search_jobs_global(sql_query: str = None, resource_id: str = None, limit: int = 20) -> str:
    """
    General purpose search tool. 
    Ideally use specialized tools, but this allows raw SQL querying via 'sql_query'.
    If 'sql_query' is provided, it must be a valid SQL statement for the CKAN Datastore.
    You can use the placeholder 'TABLE' to refer to the resource table, or provide the resource_id directly.
    Example: 'SELECT * FROM "c00b8591..." WHERE "Job Title" LIKE "%Manager%"'
    """
    res_id = await _get_resource_id(resource_id)
    
    if sql_query:
        # Simple prevention of basic injection or errors if user expects string formatting
        # We replace a convenient placeholder TABLE with the actual resource ID quoted
        final_query = sql_query.replace("TABLE", f'"{res_id}"')
        if f'"{res_id}"' not in final_query and res_id not in final_query:
             # If user didn't specify table, we strictly can't run it unless we inject it.
             # But let's assume informed usage or simple SELECT * FROM "ID"
             pass
        
        result = await client.datastore_search_sql(final_query)
        return json.dumps(result, indent=2)
    else:
        # Fallback to basic list
        result = await client.datastore_search(res_id, limit=limit)
        return json.dumps(result.get("records", []), indent=2)

@mcp.tool()
async def search_jobs_by_title(title_keywords: str, resource_id: str = None, limit: int = 20) -> str:
    """
    Search for jobs by title.
    Args:
        title_keywords: Keywords to search for in 'Job Title'. 
        resource_id: Optional dataset ID. Defaults to latest English.
    """
    res_id = await _get_resource_id(resource_id)
    # Sanitize keywords for SQL
    sanitized_kw = title_keywords.replace("'", "''") 
    sql = f'SELECT * FROM "{res_id}" WHERE "Job Title" LIKE \'%{sanitized_kw}%\' LIMIT {limit}'
    
    try:
        results = await client.datastore_search_sql(sql)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error searching by title: {str(e)}"

@mcp.tool()
async def search_jobs_by_location(
    province: str = None, 
    city: str = None, 
    postal_code: str = None, 
    resource_id: str = None,
    limit: int = 20
) -> str:
    """
    Search by location fields.
    Args:
        province: Full name e.g. 'Alberta'
        city: City name
        postal_code: Postal code prefix or full.
    """
    res_id = await _get_resource_id(resource_id)
    clauses = []
    
    if province:
        clauses.append(f'"Province/Territory" LIKE \'%{province.replace("\'", "\'\'")}%\'')
    if city:
        clauses.append(f'"City" LIKE \'%{city.replace("\'", "\'\'")}%\'')
    if postal_code:
        clauses.append(f'"Work Location Postal Code" LIKE \'%{postal_code.replace("\'", "\'\'")}%\'')
        
    if not clauses:
        return "Error: At least one location parameter (province, city, postal_code) must be provided."
        
    where_clause = " AND ".join(clauses)
    sql = f'SELECT * FROM "{res_id}" WHERE {where_clause} LIMIT {limit}'
    
    results = await client.datastore_search_sql(sql)
    return json.dumps(results, indent=2)

@mcp.tool()
async def search_jobs_by_noc(
    noc_code: str, 
    version: str = "2021", 
    resource_id: str = None,
    limit: int = 20
) -> str:
    """
    Search by NOC code.
    Args:
        noc_code: The numeric code (e.g. '0112', '10011')
        version: '2016' or '2021'. Defaults to '2021'.
    """
    res_id = await _get_resource_id(resource_id)
    
    column = "NOC21 Code" if version == "2021" else "NOC 2016 Code"
    
    sql = f'SELECT * FROM "{res_id}" WHERE "{column}" LIKE \'{noc_code}%\' LIMIT {limit}'
    results = await client.datastore_search_sql(sql)
    return json.dumps(results, indent=2)

@mcp.tool()
async def search_jobs_by_salary(
    min_salary: float, 
    max_salary: float = None, 
    pay_period: str = "Hour", 
    resource_id: str = None,
    limit: int = 20
) -> str:
    """
    Search by salary range.
    Args:
        min_salary: Minimum salary value.
        max_salary: Maximum salary value (optional).
        pay_period: 'Hour', 'Year', 'Month'. Defaults to 'Hour'.
    """
    res_id = await _get_resource_id(resource_id)
    
    # We need to cast "Salary Minimum" to numeric/float logic if possible, 
    # but the JSON suggests they are text fields "16.5".  CKAN datastore often stores as text unless type guessed.
    # Casting in SQL: "Salary Minimum"::numeric
    
    clauses = [f'"Salary Per" = \'{pay_period}\'']
    clauses.append(f'CAST("Salary Minimum" AS NUMERIC) >= {min_salary}')
    
    if max_salary:
        clauses.append(f'CAST("Salary Maximum" AS NUMERIC) <= {max_salary}')
        
    where_clause = " AND ".join(clauses)
    sql = f'SELECT * FROM "{res_id}" WHERE {where_clause} LIMIT {limit}'
    
    try:
        results = await client.datastore_search_sql(sql)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error searching by salary: {str(e)}"

@mcp.tool()
async def search_jobs_by_employment_type(
    emp_type: str, 
    resource_id: str = None,
    limit: int = 20
) -> str:
    """
    Search by Employment Type (e.g., 'Full time', 'Part time').
    """
    res_id = await _get_resource_id(resource_id)
    sql = f'SELECT * FROM "{res_id}" WHERE "Employment Type" = \'{emp_type.replace("\'", "\'\'")}\' LIMIT {limit}'
    results = await client.datastore_search_sql(sql)
    return json.dumps(results, indent=2)

def main():
    mcp.run()

if __name__ == "__main__":
    main()
