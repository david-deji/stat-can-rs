import asyncio
import json
from opendata_mcp.client import OpenDataClient

async def run_demo():
    print("Initializing Client...")
    client = OpenDataClient()
    
    # 1. List datasets to find January
    print("\n1. Listing available datasets...")
    resources = await client.list_available_resources()
    
    jan_resource = None
    for res in resources:
        # Looking for "January" and "202" (2025, 2026, etc) in English
        # The resource name often contains the Month and Year
        if "January" in res['name'] and res['language'] == 'en':
             jan_resource = res
             break
    
    if not jan_resource:
        print("Could not find a specific 'January' English dataset. Falling back to the latest available.")
        # Fallback to latest English if exact match fails
        english_resources = [r for r in resources if "en" in r.get("language", "en")]
        english_resources.sort(key=lambda x: x.get("created", ""), reverse=True)
        jan_resource = english_resources[0]

    print(f"Target Resource: {jan_resource['name']} (ID: {jan_resource['id']})")
    
    # 2. Search for Truck Driver jobs using basic search (safer than SQL)
    print("\n2. Searching for 'Truck Driver' in 'Montreal'...")
    
    # We use the full text search 'q' parameter which searches all fields
    query = "Truck Driver Montréal"
    print(f"Executing query: {query}")
    
    try:
        # Using the low-level client method directly for clear demonstration
        results_data = await client.datastore_search(
            resource_id=jan_resource['id'], 
            q=query,
            limit=5
        )
        results = results_data.get("records", [])
        
        print(f"\nFound {len(results)} results:")
        print(json.dumps(results, indent=2))
    except Exception as e:
        print(f"\nSearch Failed: {e}")

if __name__ == "__main__":
    asyncio.run(run_demo())
