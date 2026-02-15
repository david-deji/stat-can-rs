import sys

filepath = "src/bin/mcp_server.rs"

with open(filepath, "r") as f:
    lines = f.readlines()

new_lines = []
skip = False
brace_count = 0
found = False

for line in lines:
    if "fn log_and_map_error(e: StatCanError)" in line:
        skip = True
        found = True
        brace_count = line.count("{") - line.count("}")
        continue

    if skip:
        brace_count += line.count("{")
        brace_count -= line.count("}")
        if brace_count <= 0:
            skip = False
        continue

    new_lines.append(line)

with open(filepath, "w") as f:
    f.writelines(new_lines)

if found:
    print("Function removed.")
else:
    print("Function not found!")
