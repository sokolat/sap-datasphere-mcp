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

## Cloud Deployment (GitHub Actions)

Automated CI/CD: push to `main` → build → test → deploy.

### 1. Server Setup (one-time)

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# Create credentials file
sudo mkdir -p /opt/sap-mcp
sudo tee /opt/sap-mcp/.env << 'EOF'
DATASPHERE_BASE_URL=https://your-tenant.eu10.hcs.cloud.sap
DATASPHERE_CLIENT_ID=your-client-id
DATASPHERE_CLIENT_SECRET=your-client-secret
DATASPHERE_TOKEN_URL=https://your-tenant.authentication.eu10.hana.ondemand.com/oauth/token
MCP_API_KEY=your-api-key
EOF
sudo chmod 600 /opt/sap-mcp/.env
```

### 2. GitHub Secrets

Go to repo → Settings → Secrets → Actions. Add:

| Secret | Value |
|--------|-------|
| `SERVER_HOST` | Server IP or hostname |
| `SERVER_USER` | SSH username |
| `SERVER_SSH_KEY` | SSH private key (full content) |
| `GHCR_TOKEN` | GitHub PAT with `read:packages` |
| `DATASPHERE_BASE_URL` | Datasphere tenant URL |
| `DATASPHERE_CLIENT_ID` | OAuth client ID |
| `DATASPHERE_CLIENT_SECRET` | OAuth client secret |
| `DATASPHERE_TOKEN_URL` | OAuth token endpoint |

### 3. Deploy

Push to `main` or: Actions → "Build and Deploy MCP Server" → Run workflow.

### Manual Deployment (alternative)

```bash
docker pull ghcr.io/sokolat/sap-datasphere-mcp:latest
docker run -d -p 8080:8080 --env-file /opt/sap-mcp/.env ghcr.io/sokolat/sap-datasphere-mcp:latest
```

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

### Endpoints

- `GET /health` — healthcheck, no auth, returns `{"status":"ok"}`
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
cp .env.example .env
python sap_datasphere_mcp_server.py        # stdio mode (for Claude Desktop)
python sap_datasphere_mcp_server.py http   # HTTP mode on 0.0.0.0:8080
```

## License

Apache 2.0. See `LICENSE` and `NOTICE`. Full historical README preserved at `docs/archive/README_full.md`.
