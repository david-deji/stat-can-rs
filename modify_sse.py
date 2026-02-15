import sys

with open('src/bin/mcp_server.rs', 'r') as f:
    lines = f.readlines()

new_lines = []
skip = False
for line in lines:
    if 'async fn sse_handler(' in line:
        new_lines.append(line)
        new_lines.append('    State(_state): State<AppState>,\n')
        new_lines.append(') -> Sse<impl Stream<Item = Result<Event, Infallible>>> {\n')
        new_lines.append('    // 1. Send the endpoint event immediately so the client knows where to POST\n')
        new_lines.append('    let endpoint_event = Event::default().event("endpoint").data("/mcp/messages");\n')
        new_lines.append('\n')
        new_lines.append('    // 2. Keep the stream open\n')
        new_lines.append('    let pending = stream::pending::<Result<Event, Infallible>>();\n')
        new_lines.append('\n')
        new_lines.append('    let stream = stream::once(async { Ok(endpoint_event) }).chain(pending);\n')
        new_lines.append('\n')
        new_lines.append('    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())\n')
        new_lines.append('}\n')
        skip = True
    elif skip:
        if line.strip() == '}':
             skip = False
    else:
        new_lines.append(line)

with open('src/bin/mcp_server.rs', 'w') as f:
    f.writelines(new_lines)
