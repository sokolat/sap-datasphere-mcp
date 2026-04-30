# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MCP server exposing SAP Datasphere **task-monitoring** tools over Streamable HTTP. Fork of MarioDeFelipe/sap-datasphere-mcp, trimmed to task-monitoring surface. Other tools remain in codebase but are disabled—see `_TASK_MONITORING_TOOLS` in `sap_datasphere_mcp_server.py`.

## Active Tools (Exposed via MCP)

Only these 5 tools are exposed to clients:
- `list_task_chains` - List task chains in a space (uses Datasphere CLI)
- `get_task_status` - Latest status of a specific task chain
- `get_task_history` - Historical runs of a task chain
- `get_task_log` - Log output of a specific run
- `run_task_chain` - Trigger execution of a task chain

## Commands

### Run Server
```bash
# Local development (stdio mode for Claude Desktop)
python sap_datasphere_mcp_server.py

# HTTP mode (Streamable HTTP for cloud deployment)
python sap_datasphere_mcp_server.py http 0.0.0.0 8080
```

### Docker
```bash
docker build -t sap-datasphere-mcp .
docker run -d -p 8080:8080 --env-file .env sap-datasphere-mcp
```

### Tests
```bash
# All tests
pytest

# Single test file
pytest test_mcp_server.py

# Single test
pytest test_mcp_server.py::test_function_name -v

# With markers
pytest -m integration   # requires live API
pytest -m unit          # no external deps
pytest -m "not slow"    # skip slow tests
```

### Linting
```bash
black .
flake8 .
mypy .
```

## Architecture

```
sap_datasphere_mcp_server.py   # Main MCP server entry point
├── Server handlers: @server.list_tools(), @server.call_tool(), etc.
├── _TASK_MONITORING_TOOLS     # Set controlling which tools are exposed
├── handle_list_tools()        # Filters tools to only those in _TASK_MONITORING_TOOLS
└── handle_call_tool()         # Rejects calls to disabled tools

auth/                          # Authorization & OAuth
├── authorization.py           # AuthorizationManager - permission levels, consent tracking
├── oauth_handler.py           # OAuth2 client_credentials flow
├── datasphere_auth_connector.py  # Authenticated API connector
├── tool_validators.py         # Per-tool input validation
├── consent_manager.py         # User consent tracking
├── data_filter.py             # PII/credential redaction
├── input_validator.py         # Input sanitization
└── sql_sanitizer.py           # SQL injection prevention

tool_descriptions.py           # ToolDescriptions class - all tool schemas/descriptions
mock_data.py                   # Mock data for USE_MOCK_DATA=true mode
cache_manager.py               # Response caching with TTL
telemetry.py                   # TelemetryManager for monitoring
error_helpers.py               # User-friendly error formatting
```

## Key Patterns

### Tool Registration
Tools defined in `ToolDescriptions.get_all_enhanced_descriptions()` return dict with `description` and `inputSchema`. Main server builds `Tool` objects from these in `handle_list_tools()`.

### Tool Filtering
`_TASK_MONITORING_TOOLS` set gates which tools are exposed:
```python
# In handle_list_tools():
tools = [t for t in _all_tools if t.name in _TASK_MONITORING_TOOLS]

# In handle_call_tool():
if name not in _TASK_MONITORING_TOOLS:
    return [TextContent(type="text", text=f"Tool {name} is disabled")]
```

### Permission Levels
`auth/authorization.py` defines `PermissionLevel` enum (READ, WRITE, ADMIN, SENSITIVE) and `TOOL_PERMISSIONS` dict mapping tools to their permission config.

### OAuth Flow
Uses client_credentials grant. Config from env vars: `DATASPHERE_CLIENT_ID`, `DATASPHERE_CLIENT_SECRET`, `DATASPHERE_TOKEN_URL`. DatasphereAuthConnector handles token refresh on 401.

### CLI Integration
Task chain and view tools shell out to `@sap/datasphere-cli`:
- `datasphere objects task-chains list`
- `datasphere objects views read/create/update/delete`

Docker includes Node.js 20 and `@sap/datasphere-cli` for this.

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `DATASPHERE_BASE_URL` | Yes | Tenant URL (e.g., `https://tenant.eu10.hcs.cloud.sap`) |
| `DATASPHERE_CLIENT_ID` | Yes | OAuth client ID |
| `DATASPHERE_CLIENT_SECRET` | Yes | OAuth client secret |
| `DATASPHERE_TOKEN_URL` | Yes | OAuth token endpoint |
| `MCP_API_KEY` | Yes (HTTP) | Bearer token for `/mcp` endpoint |
| `USE_MOCK_DATA` | No | `true` for mock data mode |
| `LOG_LEVEL` | No | `DEBUG`, `INFO`, `WARNING`, etc. |
| `SERVER_PORT` | No | HTTP port (default 8080) |

## HTTP Endpoints (HTTP mode)

- `GET /health` - Health check, no auth
- `POST /mcp` - MCP Streamable HTTP, requires `Authorization: Bearer <MCP_API_KEY>`

## Testing Notes

Test markers defined in `tests/conftest.py`:
- `@pytest.mark.integration` - requires live Datasphere API
- `@pytest.mark.unit` - no external dependencies
- `@pytest.mark.slow` - long-running tests
- `@pytest.mark.cache` - cache-related tests

Fixtures provide `test_config`, `oauth_credentials`, `mock_api_response`, etc.
