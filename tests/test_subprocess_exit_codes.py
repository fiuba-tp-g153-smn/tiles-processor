"""Tests for the subprocess entrypoint's exit-code signalling."""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from exceptions import UnprocessableInputError  # noqa: E402
from worker import subprocess_processor  # noqa: E402
from worker.exit_codes import (  # noqa: E402
    EXIT_ERROR_CODE,
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


def test_main_returns_success_code_on_clean_run():
    with patch.object(sys, "argv", _ARGV), patch.object(
        subprocess_processor, "run_processing", return_value=None
    ):
        assert subprocess_processor.main() == EXIT_SUCCESS_CODE
