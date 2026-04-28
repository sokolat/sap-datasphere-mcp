#!/bin/sh
# Authenticate the SAP Datasphere CLI with client_credentials (headless)
# so tools that shell out to `datasphere objects ...` work in the container.
# Falls through to the command regardless — Python server still serves
# tools that do not depend on the CLI.
set -e

if [ -n "$DATASPHERE_CLIENT_ID" ] \
   && [ -n "$DATASPHERE_CLIENT_SECRET" ] \
   && [ -n "$DATASPHERE_BASE_URL" ] \
   && [ -n "$DATASPHERE_TOKEN_URL" ]; then

  echo "[entrypoint] Setting datasphere CLI host..."
  datasphere config host set "$DATASPHERE_BASE_URL"

  echo "[entrypoint] Fetching access token via client_credentials..."
  ACCESS_TOKEN=$(python3 << 'PYEOF'
import urllib.request
import urllib.parse
import base64
import json
import os
import sys

try:
    client_id = os.environ['DATASPHERE_CLIENT_ID']
    client_secret = os.environ['DATASPHERE_CLIENT_SECRET']
    token_url = os.environ['DATASPHERE_TOKEN_URL']

    credentials = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()
    data = urllib.parse.urlencode({'grant_type': 'client_credentials'}).encode()

    req = urllib.request.Request(token_url, data=data)
    req.add_header('Authorization', f'Basic {credentials}')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')

    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read().decode())
        print(result['access_token'])
except Exception as e:
    print(f'ERROR: {e}', file=sys.stderr)
    sys.exit(1)
PYEOF
)

  if [ -n "$ACCESS_TOKEN" ]; then
    echo "[entrypoint] Access token obtained, logging into CLI..."
    if datasphere login -F -H "$DATASPHERE_BASE_URL" -a "$ACCESS_TOKEN"; then
      echo "[entrypoint] datasphere CLI login OK."
    else
      echo "[entrypoint] WARNING: datasphere CLI login with token failed."
    fi
  else
    echo "[entrypoint] WARNING: Failed to obtain access token — CLI-dependent tools will error at runtime."
  fi
else
  echo "[entrypoint] DATASPHERE_* env vars not all set — skipping CLI login. CLI-dependent tools will be unavailable."
fi

exec "$@"
