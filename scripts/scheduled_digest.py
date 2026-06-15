#!/usr/bin/env python3
"""Kicker for the scheduled task-chain digest.

Posts a standing prompt to the agent's invoke endpoint. The agent itself
calls MCP tools, builds the digest, and posts to the Teams webhook flow
that has already been wired up in the agent configuration.

Required env vars:
  AGENT_URL       — full invoke URL, e.g.
                    https://studio-api.ai.syntax-rnd.com/api/v1/agents/<id>/invoke
  AGENT_API_KEY   — x-api-key header value
  DIGEST_PROMPT   — natural-language instruction for the agent

Optional:
  SESSION_ID      — defaults to digest-YYYY-MM-DD so each day starts fresh
  TIMEOUT_SECONDS — HTTP timeout, default 300

Exits non-zero on any failure so the scheduler surfaces it.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import requests


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if not value:
        sys.stderr.write(f"missing required env var: {name}\n")
        sys.exit(2)
    return value


def main() -> int:
    agent_url = env("AGENT_URL")
    api_key = env("AGENT_API_KEY")
    prompt = env("DIGEST_PROMPT")
    session_id = os.environ.get(
        "SESSION_ID", f"digest-{dt.date.today().isoformat()}"
    )
    timeout = int(os.environ.get("TIMEOUT_SECONDS", "300"))

    payload = {
        "input": [{"type": "text", "text": prompt}],
        "session_id": session_id,
    }

    print(f"[digest] POST {agent_url} session={session_id}")
    response = requests.post(
        agent_url,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        json=payload,
        timeout=timeout,
    )

    print(f"[digest] HTTP {response.status_code}")
    try:
        print(json.dumps(response.json(), indent=2)[:4000])
    except ValueError:
        print(response.text[:4000])

    response.raise_for_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
