import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import logging

# Constants
BASE_URL = "https://open.canada.ca/data/api/3/action/"
JOB_BANK_PACKAGE_ID = "ea639e28-c0fc-48bf-b5dd-b8899bd43072"

class OpenDataClient:
    def __init__(self):
        self.client = httpx.AsyncClient(base_url=BASE_URL, timeout=30.0)
        self.logger = logging.getLogger(__name__)

    async def get_job_bank_package(self) -> Dict[str, Any]:
        """Fetch the full metadata for the Job Bank Open Data package."""
        try:
            response = await self.client.get("package_show", params={"id": JOB_BANK_PACKAGE_ID})
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                raise Exception(f"API Error: {data.get('error', 'Unknown error')}")
            return data["result"]
        except httpx.HTTPError as e:
            self.logger.error(f"HTTP Error fetching package: {e}")
            raise

    async def list_available_resources(self) -> List[Dict[str, Any]]:
        """
        Parses the package metadata and returns a simplified list of available CSV resources.
        Filters for resources that are active and have a CSV format.
        """
        package = await self.get_job_bank_package()
        resources = []
        for res in package.get("resources", []):
            # We filter for CSV format and active state
            if res.get("format", "").upper() == "CSV" and res.get("state") == "active":
                # Extract Month/Year from name if possible, or just return raw name
                resources.append({
                    "id": res["id"],
                    "name": res["name"],
                    "language": res.get("language", ["unknown"])[0],
                    "created": res.get("created"),
                    "url": res.get("url")
                })
        return resources

    async def datastore_search(self, resource_id: str, limit: int = 10, filters: Dict[str, Any] = None, q: str = None) -> Dict[str, Any]:
        """
        Performs a basic datastore search.
        """
        params = {
            "resource_id": resource_id,
            "limit": limit
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if q:
            params["q"] = q

        try:
            response = await self.client.get("datastore_search", params=params)
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                raise Exception(f"API Error: {data.get('error', 'Unknown error')}")
            return data["result"]
        except httpx.HTTPError as e:
            self.logger.error(f"HTTP Error searching datastore: {e}")
            raise

    async def datastore_search_sql(self, sql: str) -> List[Dict[str, Any]]:
        """
        Performs a SQL query on the datastore.
        """
        try:
            response = await self.client.get("datastore_search_sql", params={"sql": sql})
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                raise Exception(f"API Error: {data.get('error', 'Unknown error')}")
            return data["result"]["records"]
        except httpx.HTTPError as e:
            self.logger.error(f"HTTP Error executing SQL: {e}")
            if hasattr(e, 'response') and e.response is not None:
                 print(f"DEBUG: HTTP Error Response: {e.response.text}")
            raise

    async def get_latest_english_resource_id(self) -> str:
        """
        Helper to get the resource ID of the most recent English CSV dataset.
        """
        resources = await self.list_available_resources()
        # Sort by creation date descending
        english_resources = [r for r in resources if "en" in r.get("language", "en")]
        english_resources.sort(key=lambda x: x.get("created", ""), reverse=True)
        
        if not english_resources:
             raise Exception("No English CSV resources found.")
             
        return english_resources[0]["id"]
        
