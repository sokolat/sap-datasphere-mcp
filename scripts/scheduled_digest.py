#!/usr/bin/env python3
"""Kicker for the scheduled task-chain digest.

Invokes the agent once per chain (sequential 1-call prompts) to sidestep
the agent platform's multi-tool-call stall, then concatenates the bullets
into a single Teams Adaptive Card.

Required env vars:
  AGENT_URL              — full invoke URL
  AGENT_API_KEY          — x-api-key header value
  CHAIN_SPACE            — SAP Datasphere space, e.g. DW_SYNTAX
  CHAIN_NAMES            — comma-separated chain names
  CHAIN_PROMPT_TEMPLATE  — prompt template with {chain} and {space} placeholders

Optional:
  TEAMS_WEBHOOK_URL      — Power Automate trigger URL
  TIMEOUT_SECONDS        — HTTP timeout per call, default 300

Exits non-zero on any failure so the scheduler surfaces it.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from typing import Any
from zoneinfo import ZoneInfo

import requests


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.stderr.write(f"missing required env var: {name}\n")
        sys.exit(2)
    return value


def extract_reply_text(payload: Any) -> str:
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


def invoke_agent_for_chain(
    agent_url: str,
    api_key: str,
    prompt: str,
    session_id: str,
    timeout: int,
) -> str:
    response = requests.post(
        agent_url,
        headers={"Content-Type": "application/json", "x-api-key": api_key},
        json={
            "input": [{"type": "text", "text": prompt}],
            "session_id": session_id,
        },
        timeout=timeout,
    )
    print(f"[digest]   HTTP {response.status_code}")
    response.raise_for_status()
    try:
        body = response.json()
    except ValueError:
        body = response.text
    return extract_reply_text(body).strip()


def status_breakdown(bullets: list[str]) -> str:
    green = failed = running = other = 0
    for bullet in bullets:
        upper = bullet.upper()
        if "FAILED" in upper:
            failed += 1
        elif "RUNNING" in upper:
            running += 1
        elif "COMPLETED" in upper or "SUCCESS" in upper:
            green += 1
        else:
            other += 1
    if failed == 0 and running == 0 and other == 0:
        return "all green"
    parts = []
    if failed:
        parts.append(f"{failed} failed")
    if running:
        parts.append(f"{running} running")
    if green:
        parts.append(f"{green} green")
    if other:
        parts.append(f"{other} unknown")
    return ", ".join(parts)


def build_adaptive_card(text: str) -> dict:
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": [
            {
                "type": "TextBlock",
                "text": "SAP Datasphere task-chain digest",
                "weight": "Bolder",
                "size": "Medium",
                "wrap": True,
            },
            {"type": "TextBlock", "text": text, "wrap": True},
        ],
    }


def post_to_teams(webhook_url: str, text: str, timeout: int) -> None:
    print(f"[digest] forwarding to Teams ({len(text)} chars)")
    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        json=build_adaptive_card(text),
        timeout=timeout,
    )
    print(f"[digest] Teams HTTP {response.status_code}")
    response.raise_for_status()


def main() -> int:
    agent_url = env("AGENT_URL")
    api_key = env("AGENT_API_KEY")
    space = env("CHAIN_SPACE")
    chain_names = [c.strip() for c in env("CHAIN_NAMES").split(",") if c.strip()]
    prompt_template = env("CHAIN_PROMPT_TEMPLATE")
    teams_webhook = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
    timeout = int(os.environ.get("TIMEOUT_SECONDS", "300"))
    today = dt.date.today().isoformat()

    bullets: list[str] = []
    for chain in chain_names:
        prompt = prompt_template.format(chain=chain, space=space)
        session_id = f"digest-{today}-{chain}"
        print(f"[digest] POST chain={chain} session={session_id}")
        bullet = invoke_agent_for_chain(
            agent_url, api_key, prompt, session_id, timeout
        )
        print(f"[digest]   {bullet}")
        bullets.append(bullet)

    now_montreal = dt.datetime.now(ZoneInfo("America/Montreal")).strftime(
        "%H:%M %Z"
    )
    header = (
        f"**Audited {len(chain_names)} chains at {now_montreal} — "
        f"{status_breakdown(bullets)}.**"
    )
    report = header + "\n\n**Chains**\n" + "\n".join(bullets)
    print("\n[digest] final report:\n" + report + "\n")

    if teams_webhook:
        post_to_teams(teams_webhook, report, timeout)
    else:
        print("[digest] TEAMS_WEBHOOK_URL not set — skipping Teams forward.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
