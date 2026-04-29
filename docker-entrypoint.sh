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

  # Derive authorization URL from token URL (replace /oauth/token with /oauth/authorize)
  DATASPHERE_AUTH_URL=$(echo "$DATASPHERE_TOKEN_URL" | sed 's|/oauth/token|/oauth/authorize|')

  echo "[entrypoint] Authenticating datasphere CLI (client_credentials)..."
  if datasphere login \
       --authorization-flow client_credentials \
       --force \
       --host "$DATASPHERE_BASE_URL" \
       --client-id "$DATASPHERE_CLIENT_ID" \
       --client-secret "$DATASPHERE_CLIENT_SECRET" \
       --authorization-url "$DATASPHERE_AUTH_URL" \
       --token-url "$DATASPHERE_TOKEN_URL"; then
    echo "[entrypoint] datasphere CLI login OK."
  else
    echo "[entrypoint] WARNING: datasphere CLI login failed — CLI-dependent tools will error at runtime."
  fi
else
  echo "[entrypoint] DATASPHERE_* env vars not all set — skipping CLI login. CLI-dependent tools will be unavailable."
fi

exec "$@"
