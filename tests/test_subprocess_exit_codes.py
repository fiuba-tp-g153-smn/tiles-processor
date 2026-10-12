"""Tests for the subprocess entrypoint's exit-code signalling."""

import os
import sys
import time
from unittest.mock import ANY, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from exceptions import UnprocessableInputError  # noqa: E402
from processors.base_processor import ShutdownRequested  # noqa: E402
from worker import subprocess_processor  # noqa: E402
from worker.exit_codes import (  # noqa: E402
    EXIT_ERROR_CODE,
    EXIT_SHUTDOWN_CODE,
    EXIT_SKIP_CODE,
    EXIT_SUCCESS_CODE,
)

_ARGV = ["subprocess_processor", "{}", "/tmp/in.nc"]


def test_main_returns_skip_code_and_emits_reason_to_stderr(capsys):
    """UnprocessableInputError → EXIT_SKIP_CODE, with the reason on stderr."""
    with patch.object(sys, "argv", _ARGV), patch.object(
        subprocess_processor,
        "run_processing",
        side_effect=UnprocessableInputError("Incompatible sweep range geometry"),
    ):
        code = subprocess_processor.main()

    assert code == EXIT_SKIP_CODE
    # The parent reads the reason off stderr (normal logs go to stdout).
    assert "Incompatible sweep range geometry" in capsys.readouterr().err


def test_main_returns_error_code_on_generic_exception():
    """A genuine error still exits non-zero ERROR (retry/DLQ path), not skip."""
    with patch.object(sys, "argv", _ARGV), patch.object(
        subprocess_processor, "run_processing", side_effect=RuntimeError("boom")
    ):
        assert subprocess_processor.main() == EXIT_ERROR_CODE


def test_main_returns_shutdown_code_on_shutdown_requested():
    """Graceful shutdown is a DISTINCT code, not ERROR, so the parent nack-requeues
    (redeliver) instead of burning a retry/DLQ slot."""
    with patch.object(sys, "argv", _ARGV), patch.object(
        subprocess_processor, "run_processing", side_effect=ShutdownRequested("stop")
    ):
        assert subprocess_processor.main() == EXIT_SHUTDOWN_CODE


def test_main_returns_success_code_on_clean_run():
    with patch.object(sys, "argv", _ARGV), patch.object(
        subprocess_processor, "run_processing", return_value=None
    ):
        assert subprocess_processor.main() == EXIT_SUCCESS_CODE


def test_main_forwards_work_token_to_run_processing():
    """A 5th argv (per-attempt token) is forwarded so the processor scopes its dir."""
    argv = ["subprocess_processor", "{}", "/tmp/in.nc", "/tmp/m.json", "tok7"]
    with patch.object(sys, "argv", argv), patch.object(
        subprocess_processor, "run_processing", return_value=None
    ) as run_processing:
        assert subprocess_processor.main() == EXIT_SUCCESS_CODE
    run_processing.assert_called_once_with(
        "{}", "/tmp/in.nc", "/tmp/m.json", "tok7", main_started_at=ANY
    )


def test_main_without_token_forwards_none():
    """The legacy 4-arg form (no token) still passes None through."""
    argv = ["subprocess_processor", "{}", "/tmp/in.nc", "/tmp/m.json"]
    with patch.object(sys, "argv", argv), patch.object(
        subprocess_processor, "run_processing", return_value=None
    ) as run_processing:
        assert subprocess_processor.main() == EXIT_SUCCESS_CODE
    run_processing.assert_called_once_with(
        "{}", "/tmp/in.nc", "/tmp/m.json", None, main_started_at=ANY
    )


class _RecordingProcessor:
    """Collects record_stage calls instead of timing a real pipeline."""

    def __init__(self):
        self.stages: dict[str, float] = {}

    def record_stage(self, name: str, seconds: float) -> None:
        self.stages[name] = seconds


def test_startup_stages_measure_spawn_to_main_and_main_to_ready(monkeypatch):
    """startup = main − spawn mark; setup = now − main."""
    monkeypatch.setenv(subprocess_processor.SPAWNED_AT_ENV, "100.0")
    monkeypatch.setattr(subprocess_processor.time, "monotonic", lambda: 103.5)
    processor = _RecordingProcessor()

    subprocess_processor._record_startup_stages(processor, main_started_at=102.0)

    assert processor.stages == {"startup": 2.0, "setup": 1.5}


def test_startup_stage_dropped_without_spawn_mark(monkeypatch):
    """No env mark (e.g. a manual run) → only setup is recorded, no error."""
    monkeypatch.delenv(subprocess_processor.SPAWNED_AT_ENV, raising=False)
    processor = _RecordingProcessor()

    subprocess_processor._record_startup_stages(processor, time.monotonic())

    assert set(processor.stages) == {"setup"}


def test_startup_stage_dropped_on_malformed_or_future_mark(monkeypatch):
    """A garbage or later-than-main mark must not break processing nor go negative."""
    processor = _RecordingProcessor()
    for raw in ("not-a-number", "999999999.0"):
        monkeypatch.setenv(subprocess_processor.SPAWNED_AT_ENV, raw)
        subprocess_processor._record_startup_stages(processor, main_started_at=10.0)
        assert "startup" not in processor.stages
