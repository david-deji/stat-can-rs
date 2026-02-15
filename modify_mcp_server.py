import sys

def modify_file():
    path = "src/bin/mcp_server.rs"
    with open(path, "r") as f:
        lines = f.readlines()

    output_lines = []
    i = 0
    in_json_rpc_struct = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Check for #[allow(dead_code)] immediately followed by JsonRpcRequest
        if stripped == "#[allow(dead_code)]":
            if i + 1 < len(lines) and "struct JsonRpcRequest" in lines[i+1]:
                i += 1
                continue

        if "struct JsonRpcRequest {" in line:
            in_json_rpc_struct = True
            output_lines.append(line)
            i += 1
            continue

        if in_json_rpc_struct:
            if stripped == "}":
                in_json_rpc_struct = False
                output_lines.append(line)
                i += 1
                continue

            if "jsonrpc: String," in line:
                # Calculate indentation
                indent = line[:line.find("jsonrpc")]
                output_lines.append(f'{indent}#[serde(rename = "jsonrpc")]\n')
                output_lines.append(f'{indent}_jsonrpc: String,\n')
                i += 1
                continue

        output_lines.append(line)
        i += 1

    with open(path, "w") as f:
        f.writelines(output_lines)

if __name__ == "__main__":
    modify_file()
