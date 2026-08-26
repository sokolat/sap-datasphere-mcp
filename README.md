# SAP Datasphere MCP Server

MCP server exposing SAP Datasphere **task-monitoring** tools over Streamable HTTP. Built for remote deployment behind a TLS-terminating proxy so MCP clients (e.g., SAP Studio) can call it over the network.

Fork of [MarioDeFelipe/sap-datasphere-mcp](https://github.com/MarioDeFelipe/sap-datasphere-mcp), trimmed to the task-monitoring surface. Other tools remain in the codebase but are disabled — see `_TASK_MONITORING_TOOLS` in `sap_datasphere_mcp_server.py`.

## Exposed Tools

| Tool | Purpose |
|------|---------|
| `list_task_chains` | List task chains in a space |
| `get_task_status` | Latest status of a specific task chain |
| `get_task_history` | Historical runs of a task chain |
| `get_task_log` | Log output of a specific run |
| `run_task_chain` | Trigger execution of a task chain |

## Quick Start (prebuilt image)

The image is published to GHCR on every push to `main`, with Node 20 and `@sap/datasphere-cli` baked in:

```bash
docker pull ghcr.io/sokolat/sap-datasphere-mcp:latest
docker run -d -p 8080:8080 --env-file .env ghcr.io/sokolat/sap-datasphere-mcp:latest
curl http://localhost:8080/health   # {"status":"ok"}
```

The entrypoint (`docker-entrypoint.sh`) logs the Datasphere CLI in with `client_credentials` before the server starts, so `list_task_chains` works on the first call. If the login fails it warns and continues — the four REST-backed tools still work.

## Cloud Deployment (GitHub Actions + Kubernetes)

Automated CI/CD: push to `main` → build → test → image published to GHCR.

### 1. GitHub Secrets (for build/test)

Go to repo → Settings → Secrets → Actions. Add:

| Secret | Value |
|--------|-------|
| `DATASPHERE_BASE_URL` | Datasphere tenant URL |
| `DATASPHERE_CLIENT_ID` | OAuth client ID |
| `DATASPHERE_CLIENT_SECRET` | OAuth client secret |
| `DATASPHERE_TOKEN_URL` | OAuth token endpoint |

### 2. Build

Push to `main` or: Actions → "Build MCP Server" → Run workflow.

Image published to: `ghcr.io/<your-org>/sap-datasphere-mcp:latest`

### 3. Kubernetes Deployment (for admin)

Pull image and deploy with these env vars:

```yaml
env:
  - name: DATASPHERE_BASE_URL
    value: "<tenant-url>"
  - name: DATASPHERE_CLIENT_ID
    valueFrom:
      secretKeyRef:
        name: datasphere-secrets
        key: client-id
  - name: DATASPHERE_CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: datasphere-secrets
        key: client-secret
  - name: DATASPHERE_TOKEN_URL
    value: "<token-url>"
  - name: MCP_API_KEY
    valueFrom:
      secretKeyRef:
        name: mcp-secrets
        key: api-key
```

**Port:** 8080
**Health check:** `GET /health`
**MCP endpoint:** `POST /mcp`

### Required env vars

See `.env.example` for the full list.

| Variable | Purpose |
|----------|---------|
| `DATASPHERE_BASE_URL` | Datasphere tenant URL |
| `DATASPHERE_CLIENT_ID` | OAuth client ID (technical user) |
| `DATASPHERE_CLIENT_SECRET` | OAuth client secret |
| `DATASPHERE_TOKEN_URL` | OAuth token endpoint |
| `MCP_API_KEY` | Bearer token required on `/mcp`. Generate: `python -c "import secrets;print(secrets.token_urlsafe(32))"` |
| `SERVER_PORT` | HTTP port (default `8080`) |
| `SERVER_HOST` | Bind address (default `0.0.0.0`) |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` (default `INFO`) |
| `USE_MOCK_DATA` | `true` serves canned responses. **Code default is `true`** — the Docker image sets it to `false`, so set it explicitly when running from source |

### Endpoints

- `GET /health` (alias `GET /healthz`) — healthcheck, no auth, returns `{"status":"ok"}`
- `POST /mcp` — MCP Streamable HTTP, requires header `Authorization: Bearer <MCP_API_KEY>`

### TLS

Server speaks plain HTTP. Terminate TLS at a proxy or load balancer (nginx, Cloudflare, Cloud Run, ECS Fargate, Fly.io).

### Client configuration

```
URL:    https://<your-public-host>/mcp
Header: Authorization: Bearer <MCP_API_KEY>
```

### Validate

```bash
curl https://<host>/health
# {"status":"ok"}

curl -i -X POST https://<host>/mcp
# 401 Unauthorized  (expected without token)

curl -X POST https://<host>/mcp \
  -H "Authorization: Bearer $MCP_API_KEY" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
# Lists the 5 task-monitoring tools
```

## Local Development (stdio)

```bash
pip install -r requirements.txt
cp .env.example .env                       # sets USE_MOCK_DATA=false
python sap_datasphere_mcp_server.py        # stdio mode (for Claude Desktop)
python sap_datasphere_mcp_server.py http   # HTTP mode on 0.0.0.0:8080
python sap_datasphere_mcp_server.py http 127.0.0.1 9000   # explicit host/port
```

Two things that bite outside the container:

- **Mock data is on by default.** `USE_MOCK_DATA` defaults to `true` in code, so a run without `.env` answers from `mock_data.py` instead of the tenant. `.env.example` sets it to `false`.
- **`list_task_chains` needs the CLI.** It shells out to `@sap/datasphere-cli`, which needs Node >= 20 and a prior login:

  ```bash
  npm install -g @sap/datasphere-cli
  datasphere login --authorization-flow client_credentials --force \
    --host "$DATASPHERE_BASE_URL" \
    --client-id "$DATASPHERE_CLIENT_ID" --client-secret "$DATASPHERE_CLIENT_SECRET" \
    --authorization-url "${DATASPHERE_TOKEN_URL%/oauth/token}/oauth/authorize" \
    --token-url "$DATASPHERE_TOKEN_URL"
  ```

  The other four tools go straight to the REST API and need no CLI.

`mcp` is pinned to `>=1.8.0,<2.0.0`: mcp 2.0.0 dropped the decorator methods (`@server.list_tools()` and friends) from the lowlevel `Server` class, so an unpinned install crashes at import.

## Scheduled Teams Digest (optional)

`.github/workflows/scheduled-digest.yml` runs `scripts/scheduled_digest.py` daily at 09:00 UTC (also `workflow_dispatch`). It asks the agent for one chain's status per call, then posts a combined Adaptive Card to Teams.

| Setting | Kind | Purpose |
|---------|------|---------|
| `AGENT_URL` | secret | Agent invoke URL |
| `AGENT_API_KEY` | secret | `x-api-key` header value |
| `TEAMS_WEBHOOK_URL` | secret | Power Automate trigger URL (optional — omit to log only) |
| `CHAIN_SPACE` | variable | Datasphere space, e.g. `DW_SYNTAX` |
| `CHAIN_NAMES` | variable | Comma-separated chain names |
| `CHAIN_PROMPT_TEMPLATE` | variable | Prompt with `{chain}` and `{space}` placeholders |
| `TIMEOUT_SECONDS` | variable | HTTP timeout per agent call (default `300`) |

The job exits non-zero on any failure so the run shows up red.

## License

Apache 2.0. See `LICENSE` and `NOTICE`. Full historical README preserved at `docs/archive/README_full.md`.
