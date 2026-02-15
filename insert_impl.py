import sys

filepath = "src/bin/mcp_server.rs"
marker = "// --- Core Logic ---"

impl_code = r"""
impl From<StatCanError> for JsonRpcError {
    fn from(e: StatCanError) -> Self {
        match e {
            StatCanError::TableNotFound => JsonRpcError::new(-32000, "Table not found"),
            StatCanError::Api(ref msg) if msg == "Invalid PID format" || msg == "PID cannot be empty" => {
                JsonRpcError::new(-32602, msg.clone())
            }
            e => {
                error!("Internal error: {:?}", e);
                JsonRpcError::new(-32000, "Internal server error")
            }
        }
    }
}

"""

with open(filepath, "r") as f:
    lines = f.readlines()

new_lines = []
inserted = False
for line in lines:
    if marker in line and not inserted:
        new_lines.append(impl_code)
        inserted = True
    new_lines.append(line)

with open(filepath, "w") as f:
    f.writelines(new_lines)

print("Inserted impl code.")
