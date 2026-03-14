#!/bin/bash

# Build the project first
cargo build --bin mcp_server

# Define Input File
INPUT_FILE="test_csv_requests.jsonl"

# Create JSON protocols (one per line)
cat > $INPUT_FILE <<EOF
{"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "fetch_open_data_resource_snippet", "arguments": {"resource_id": "c00b8591-7e90-4ac8-8fa3-652c2cce0ab6", "rows": 5, "format": "csv"}}, "id": 1}
{"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "fetch_data_snippet", "arguments": {"pid": "18100004", "rows": 5, "format": "csv"}}, "id": 2}
EOF

# Run in Stdio Mode
echo "Running StatCan MCP Server in Stdio mode..."
./target/debug/mcp_server < $INPUT_FILE > output_stdio.jsonl

# Check Results
echo "---------------------------------------------------"
echo "Test 1: fetch_open_data_resource_snippet (CSV)"
RESULT1=$(grep '"id":1' output_stdio.jsonl)
if echo "$RESULT1" | grep -q "result"; then
    CONTENT=$(echo "$RESULT1" | jq -r '.result.content[0].text')
    if [[ "$CONTENT" == *"year"* ]] || [[ "$CONTENT" == *","* ]]; then
        echo "PASS: CSV Content Detected"
        echo "$CONTENT" | head -n 3
    else
        echo "FAIL: Content does not look like CSV"
        echo "$CONTENT"
    fi
else
    echo "FAIL: Error response"
    echo "$RESULT1"
fi

echo "---------------------------------------------------"
echo "Test 2: fetch_data_snippet (CSV)"
RESULT2=$(grep '"id":2' output_stdio.jsonl)
if echo "$RESULT2" | grep -q "result"; then
    CONTENT=$(echo "$RESULT2" | jq -r '.result.content[0].text')
    if [[ "$CONTENT" == *"REF_DATE"* ]] || [[ "$CONTENT" == *","* ]]; then
        echo "PASS: CSV Content Detected"
        echo "$CONTENT" | head -n 3
    else
        echo "FAIL: Content does not look like CSV"
        echo "$CONTENT"
    fi
else
    echo "FAIL: Error response"
    echo "$RESULT2"
fi
