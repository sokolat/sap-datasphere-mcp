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

  SECRETS_FILE="/tmp/.datasphere-secrets-$$.json"
  trap "rm -f $SECRETS_FILE" EXIT

  echo "[entrypoint] Setting datasphere CLI host..."
  datasphere config host set "$DATASPHERE_BASE_URL"

  echo "[entrypoint] Authenticating datasphere CLI (client_credentials)..."
  cat > "$SECRETS_FILE" << EOF
{
  "client_id": "$DATASPHERE_CLIENT_ID",
  "client_secret": "$DATASPHERE_CLIENT_SECRET",
  "token_url": "$DATASPHERE_TOKEN_URL"
}
EOF
  chmod 600 "$SECRETS_FILE"

  if datasphere login \
       -d client_credentials \
       -F \
       -H "$DATASPHERE_BASE_URL" \
       -s "$SECRETS_FILE"; then
    echo "[entrypoint] datasphere CLI login OK."
  else
    echo "[entrypoint] WARNING: datasphere CLI login failed — CLI-dependent tools will error at runtime."
  fi
  rm -f "$SECRETS_FILE"
else
  echo "[entrypoint] DATASPHERE_* env vars not all set — skipping CLI login. CLI-dependent tools will be unavailable."
fi

exec "$@"
