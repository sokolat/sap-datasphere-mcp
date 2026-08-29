"""Coverage for ``list_task_chains``, the CLI-backed chain discovery tool.

The tool shells out, so the paths worth pinning are the ones the REST tools
never hit: a non-zero exit, a missing binary, and CLI output that is not the
JSON we expect. None of those may raise out of the handler — each has to come
back as a TextContent the model can act on.

Run with:  pytest tests/test_list_task_chains.py -v
"""

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("USE_MOCK_DATA", "true")

import sap_datasphere_mcp_server as srv  # noqa: E402
from auth.tool_validators import ToolValidators  # noqa: E402
from auth.input_validator import InputValidator  # noqa: E402


def _call(arguments, mock=True):
    """Invoke the tool handler with mock mode forced on or off."""
    original = srv.DATASPHERE_CONFIG["use_mock_data"]
    srv.DATASPHERE_CONFIG["use_mock_data"] = mock
    try:
        return asyncio.run(srv.handle_call_tool("list_task_chains", arguments))
    finally:
        srv.DATASPHERE_CONFIG["use_mock_data"] = original


class _FakeProcess:
    """Stand-in for the CLI child process."""

    def __init__(self, returncode=0, stdout=b"", stderr=b""):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    async def communicate(self):
        return self._stdout, self._stderr


def _patch_cli(monkeypatch, process=None, exc=None):
    async def fake_exec(*args, **kwargs):
        if exc is not None:
            raise exc
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)


# ── Registration ────────────────────────────────────────────────────────────


def test_tool_is_registered_everywhere():
    """A tool the server advertises but the auth layer does not know is a hole."""
    from auth.authorization import AuthorizationManager

    descriptions = srv.ToolDescriptions.get_all_enhanced_descriptions()
    assert "list_task_chains" in descriptions
    assert "list_task_chains" in AuthorizationManager.TOOL_PERMISSIONS
    assert ToolValidators.get_validator_rules("list_task_chains")


# ── Mock mode ───────────────────────────────────────────────────────────────


def test_mock_mode_returns_chains_with_pagination_fields():
    result = _call({"space_id": "DEMO_SALES"})
    payload = json.loads(result[0].text.split("\n\n")[1])

    assert payload["space_id"] == "DEMO_SALES"
    assert isinstance(payload["task_chains"], list)
    for field in ("count", "skip", "top", "has_more"):
        assert field in payload


def test_mock_mode_paginates():
    first = json.loads(_call({"space_id": "DEMO_SALES", "top": 1})[0].text.split("\n\n")[1])
    assert first["count"] <= 1
    assert first["skip"] == 0


# ── CLI failure paths ───────────────────────────────────────────────────────


def test_cli_non_zero_exit_reports_stderr(monkeypatch):
    _patch_cli(monkeypatch, _FakeProcess(returncode=1, stderr=b"not authenticated"))

    text = _call({"space_id": "DEMO_SALES"}, mock=False)[0].text

    assert "not authenticated" in text
    assert "DEMO_SALES" in text


def test_missing_cli_explains_how_to_install(monkeypatch):
    _patch_cli(monkeypatch, exc=FileNotFoundError("datasphere"))

    text = _call({"space_id": "DEMO_SALES"}, mock=False)[0].text

    assert "@sap/datasphere-cli" in text


def test_cli_timeout_is_reported(monkeypatch):
    _patch_cli(monkeypatch, exc=asyncio.TimeoutError())

    text = _call({"space_id": "DEMO_SALES"}, mock=False)[0].text

    assert "timed out" in text.lower()


def test_non_json_cli_output_is_passed_through(monkeypatch):
    _patch_cli(monkeypatch, _FakeProcess(stdout=b"CHAIN_A  COMPLETED"))

    text = _call({"space_id": "DEMO_SALES"}, mock=False)[0].text

    assert "CHAIN_A" in text


def test_empty_cli_output_is_not_an_error(monkeypatch):
    _patch_cli(monkeypatch, _FakeProcess(stdout=b""))

    text = _call({"space_id": "DEMO_SALES"}, mock=False)[0].text

    assert "No task chains found" in text


# ── Parsing ─────────────────────────────────────────────────────────────────


def test_chains_are_normalised_to_name_and_status(monkeypatch):
    payload = json.dumps([
        {"technicalName": "DAILY_ETL", "status": "COMPLETED"},
        {"name": "LEGACY_SHAPE"},
    ]).encode()
    _patch_cli(monkeypatch, _FakeProcess(stdout=payload))

    text = _call({"space_id": "DEMO_SALES"}, mock=False)[0].text
    result = json.loads(text.split("\n\n", 1)[1])

    assert result["task_chains"] == [
        {"name": "DAILY_ETL", "status": "COMPLETED"},
        {"name": "LEGACY_SHAPE", "status": "unknown"},
    ]
    assert result["count"] == 2


def test_full_page_signals_has_more(monkeypatch):
    payload = json.dumps([{"technicalName": "A"}, {"technicalName": "B"}]).encode()
    _patch_cli(monkeypatch, _FakeProcess(stdout=payload))

    text = _call({"space_id": "DEMO_SALES", "top": 2}, mock=False)[0].text
    result = json.loads(text.split("\n\n", 1)[1])

    assert result["has_more"] is True


# ── Validation ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("value", ["../x", "a/b", "a?b", "DEMO SALES", ""])
def test_hostile_space_id_rejected(value):
    validator = InputValidator()
    ok, _ = validator.validate_params(
        {"space_id": value},
        ToolValidators.get_validator_rules("list_task_chains"),
    )
    assert not ok, f"{value!r} should be rejected"


def test_legitimate_arguments_accepted():
    validator = InputValidator()
    ok, errors = validator.validate_params(
        {"space_id": "DEMO_SALES", "top": 25, "skip": 0},
        ToolValidators.get_validator_rules("list_task_chains"),
    )
    assert ok, errors
