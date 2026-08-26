# Chain Prompt Template (scheduled digest)

Value for the `CHAIN_PROMPT_TEMPLATE` repo variable read by
[`scripts/scheduled_digest.py`](../scripts/scheduled_digest.py). The script
formats it once per chain with `{chain}` and `{space}`, then concatenates the
returned bullets into the Teams card.

Both placeholders are required, and no other `{...}` may appear — the script
calls `str.format()`, so a stray brace raises `KeyError` and fails the run.

```text
You are auditing task chain {chain} in SAP Datasphere space {space} for the morning
standup. Call get_task_status for this chain ONCE. Do NOT call any other tools.
Respond with EXACTLY one Markdown bullet, no preamble, no other text, in this exact
shape: "- {chain} — STATUS · last run TIMESTAMP · duration DURATION". Replace STATUS
with the value returned by the tool (e.g. COMPLETED, FAILED, RUNNING). TIMESTAMP is
the lastRun.startTime converted to America/Montreal time, formatted YYYY-MM-DD HH:MM.
DURATION is h:mm when runTimeSeconds >= 3600, otherwise "M min". If status is FAILED,
append " (logId LOGID)" to the bullet.
```

One call per chain is deliberate: the agent platform stalls on multi-tool-call
prompts, so the kicker fans out sequential single-call invocations instead.
