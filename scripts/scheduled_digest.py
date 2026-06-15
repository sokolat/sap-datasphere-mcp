#!/usr/bin/env python3
"""Kicker for the scheduled task-chain digest.

Posts a standing prompt to the agent's invoke endpoint, then forwards the
agent's reply to a Teams webhook (a Power Automate "When an HTTP request
is received" flow that fans out to the channel).

Required env vars:
  AGENT_URL          — full invoke URL, e.g.
                       https://studio-api.ai.syntax-rnd.com/api/v1/agents/<id>/invoke
  AGENT_API_KEY      — x-api-key header value
  DIGEST_PROMPT      — natural-language instruction for the agent

Optional:
  TEAMS_WEBHOOK_URL  — Power Automate trigger URL. If set, the agent reply
                       is POSTed there as {"text": "<reply>"}.
  SESSION_ID         — defaults to digest-YYYY-MM-DD so each day starts fresh
  TIMEOUT_SECONDS    — HTTP timeout per call, default 300

Exits non-zero on any failure so the scheduler surfaces it.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from typing import Any

import requests


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if not value:
        sys.stderr.write(f"missing required env var: {name}\n")
        sys.exit(2)
    return value


def extract_reply_text(payload: Any) -> str:
    """Pull the human-readable reply out of common agent response shapes.

    Falls back to a pretty-printed JSON dump so the Teams card always
    contains something useful, even when the schema is unfamiliar.
    """
    if isinstance(payload, str):
        return payload

    if isinstance(payload, dict):
        for key in ("output", "response", "result", "content", "answer"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if isinstance(value, list):
                parts = [
                    item.get("text", "")
                    for item in value
                    if isinstance(item, dict) and item.get("type") == "text"
                ]
                joined = "\n".join(p for p in parts if p)
                if joined.strip():
                    return joined

        messages = payload.get("messages")
        if isinstance(messages, list) and messages:
            last = messages[-1]
            if isinstance(last, dict):
                content = last.get("content")
                if isinstance(content, str):
                    return content

    return json.dumps(payload, indent=2)


def post_to_teams(webhook_url: str, text: str, timeout: int) -> None:
    print(f"[digest] forwarding reply to Teams ({len(text)} chars)")
    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        json={"text": text},
        timeout=timeout,
    )
    print(f"[digest] Teams HTTP {response.status_code}")
    response.raise_for_status()


def main() -> int:
    agent_url = env("AGENT_URL")
    api_key = env("AGENT_API_KEY")
    prompt = env("DIGEST_PROMPT")
    teams_webhook = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
    session_id = os.environ.get(
        "SESSION_ID", f"digest-{dt.date.today().isoformat()}"
    )
    timeout = int(os.environ.get("TIMEOUT_SECONDS", "300"))

    print(f"[digest] POST {agent_url} session={session_id}")
    response = requests.post(
        agent_url,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        json={
            "input": [{"type": "text", "text": prompt}],
            "session_id": session_id,
        },
        timeout=timeout,
    )

    print(f"[digest] Agent HTTP {response.status_code}")
    try:
        body = response.json()
        print(json.dumps(body, indent=2)[:4000])
    except ValueError:
        body = response.text
        print(body[:4000])

    response.raise_for_status()

    if teams_webhook:
        reply_text = extract_reply_text(body)
        post_to_teams(teams_webhook, reply_text, timeout)
    else:
        print("[digest] TEAMS_WEBHOOK_URL not set — skipping Teams forward.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
