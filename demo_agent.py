import subprocess
import json
import sys

MCP_SERVER_BIN = "./target/release/mcp_server"


def colored(text, color):
    colors = {
        "green": "\033[92m",
        "red": "\033[91m",
        "blue": "\033[94m",
        "yellow": "\033[93m",
        "reset": "\033[0m",
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"


class Agent:
    def __init__(self):
        print(colored("Initializing AI Agent...", "blue"))
        self.process = subprocess.Popen(
            [MCP_SERVER_BIN],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            text=True,
            bufsize=1,
        )
        self.msg_id = 0

    def send_request(self, method, params=None):
        self.msg_id += 1
        req = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": self.msg_id,
        }
        json_req = json.dumps(req)
        self.process.stdin.write(json_req + "\n")
        self.process.stdin.flush()

        response_line = self.process.stdout.readline()
        if not response_line:
            raise Exception("Server hung up")

        return json.loads(response_line)

    def close(self):
        self.process.terminate()

    def run_flow(self):
        print(
            colored(
                "🤖 Agent: I need to find the latest CPI data for British Columbia.",
                "green",
            )
        )

        # Step 1: Search
        print(colored("🤖 Agent: searching for 'Consumer Price Index'...", "green"))
        res = self.send_request(
            "call_tool",
            {"name": "search_cubes", "arguments": {"query": "Consumer Price Index"}},
        )

        if "error" in res and res["error"]:
            print(
                colored(f"❌ Agent: Error calling search_cubes: {res['error']}", "red")
            )
            return

        if "result" not in res:
            print(colored(f"❌ Agent: Unexpected response structure: {res}", "red"))
            return

        content_str = res["result"]["content"][0]["text"]
        cubes = json.loads(content_str)

        print(colored(f"🤖 Agent: Found {len(cubes)} cubes.", "green"))

        # Step 2: Select
        target_pid = None
        for cube in cubes:
            title = cube.get("cubeTitleEn", "").lower()
            # PID is now a string in the Rust model, so it comes as a string in JSON
            # But let's be safe and cast to str just in case
            pid = str(cube.get("productId", ""))
            if (
                "consumer price index" in title
                and "monthly" in title
                and "not seasonally adjusted" in title
            ):
                target_pid = pid
                print(
                    colored(
                        f"🤖 Agent: Found the standard monthly CPI table: {cube['cubeTitleEn']} (PID: {target_pid})",
                        "green",
                    )
                )
                break

        if not target_pid:
            print(colored("❌ Agent: Could not find suitable table.", "red"))
            return

        # Step 2.5: Get Dimensions
        print(
            colored(f"🤖 Agent: Fetching dimensions for table {target_pid}...", "green")
        )
        res = self.send_request(
            "call_tool",
            {"name": "get_cube_dimensions", "arguments": {"pid": target_pid}},
        )

        if "error" in res and res["error"]:
            print(
                colored(f"❌ Agent: Error fetching dimensions: {res['error']}", "red")
            )
        else:
            dims_str = res["result"]["content"][0]["text"]
            dims = json.loads(dims_str)
            print(
                colored(
                    f"🤖 Agent: Found {len(dims)} dimensions. Keys: {list(dims.keys())}",
                    "green",
                )
            )

        # Step 3: Fetch
        print(
            colored(
                f"🤖 Agent: Fetching last 12 months of data for British Columbia from table {target_pid}...",
                "green",
            )
        )
        res = self.send_request(
            "call_tool",
            {
                "name": "fetch_data_snippet",
                "arguments": {
                    "pid": target_pid,
                    "geo": "British Columbia",
                    "recent_months": 12,
                },
            },
        )

        if "error" in res and res["error"]:
            print(colored(f"❌ Agent: Error fetching data: {res['error']}", "red"))
            return

        data_str = res["result"]["content"][0]["text"]
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            print(
                colored(
                    f"❌ Agent: Failed to parse JSON data. Raw output: {data_str[:100]}...",
                    "red",
                )
            )
            return

        print(colored(f"🤖 Agent: Received {len(data)} rows of data.", "green"))

        # Step 4: Analyze and Verify New Tools
        # Find "All-items" category usually. Or just list what we have.

        latest_date = ""
        latest_val = 0.0
        sample_vector = None
        sample_coord = None

        # Filter for "All-items" if possible
        input_data = [
            row
            for row in data
            if "All-items" in row.get("Products and product groups", "")
        ]

        target_row = input_data[0] if input_data else (data[0] if data else None)

        if target_row:
            latest_date = target_row.get("REF_DATE")
            latest_val = target_row.get("VALUE")
            prod = target_row.get("Products and product groups", "Unknown")
            sample_vector = target_row.get("VECTOR")
            sample_coord = target_row.get("COORDINATE")

            print(
                colored(
                    f"🔍 Agent Analysis: Found '{prod}' CPI for {latest_date}: {latest_val}",
                    "yellow",
                )
            )
            print(
                colored(
                    f"   Details: Vector={sample_vector}, Coordinate={sample_coord}",
                    "yellow",
                )
            )
        else:
            print(colored("❌ Agent: No data found to analyze.", "red"))
            return

        # Step 5: Verify fetch_data_by_vector
        if sample_vector:
            print(
                colored(
                    f"🤖 Agent: Verifying fetch_data_by_vector for {sample_vector}...",
                    "green",
                )
            )
            res = self.send_request(
                "call_tool",
                {
                    "name": "fetch_data_by_vector",
                    "arguments": {"vectors": [sample_vector]},
                },
            )

            if "error" in res and res["error"]:
                print(
                    colored(
                        f"❌ Agent: Error fetching by vector: {res['error']}", "red"
                    )
                )
            else:
                content = res["result"]["content"][0]["text"]
                vec_data = json.loads(content)
                print(
                    colored(
                        f"✅ Agent: Successfully fetched {len(vec_data)} rows by vector.",
                        "green",
                    )
                )
                if vec_data:
                    print(
                        colored(
                            f"   Value: {vec_data[0].get('value')} (Should match {latest_val})",
                            "blue",
                        )
                    )

        # Step 6: Verify fetch_data_by_coords
        if sample_coord:
            print(
                colored(
                    f"🤖 Agent: Verifying fetch_data_by_coords for {sample_coord}...",
                    "green",
                )
            )
            res = self.send_request(
                "call_tool",
                {
                    "name": "fetch_data_by_coords",
                    "arguments": {"pid": str(target_pid), "coords": [sample_coord]},
                },
            )

            if "error" in res and res["error"]:
                print(
                    colored(
                        f"❌ Agent: Error fetching by coords: {res['error']}", "red"
                    )
                )
            else:
                content = res["result"]["content"][0]["text"]
                coord_data = json.loads(content)
                print(
                    colored(
                        f"✅ Agent: Successfully fetched {len(coord_data)} rows by coord.",
                        "green",
                    )
                )
                if coord_data:
                    # API might return slightly different field names in full response vs snippet
                    # But usually 'value' or 'VALUE'
                    val = coord_data[0].get("value") or coord_data[0].get("VALUE")
                    print(
                        colored(f"   Value: {val} (Should match {latest_val})", "blue")
                    )

        # Step 7: Report
        print(colored("\n" + "=" * 40, "blue"))
        print(
            colored(
                f"📢 Agent Report: \n'Based on the latest Statistics Canada data, the Consumer Price Index for British Columbia in {latest_date} was {latest_val}.'",
                "blue",
            )
        )
        print(colored("=" * 40 + "\n", "blue"))


if __name__ == "__main__":
    agent = Agent()
    try:
        agent.run_flow()
    finally:
        agent.close()
