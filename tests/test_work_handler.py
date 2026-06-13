"""Tests for WorkHandler's async subprocess execution and multi-process abort."""

import asyncio
import os
import signal
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from models.work_unit import WorkUnit
from worker.work_handler import WorkHandler


def _work_unit(image_id: str = "img.nc") -> WorkUnit:
    return WorkUnit.create(
        image_id=image_id,
        source_uri="uri",
        data_source_id="goes19_abi_band_13",
        processor_id="goes_band_13",
        output_prefix="tiles/x",
        bounds={"minx": 0.0, "miny": 0.0, "maxx": 1.0, "maxy": 1.0},
        band_id="band_13",
    )


def _handler() -> WorkHandler:
    config = MagicMock()
    config.TMP_DIR = "/tmp/test"
    return WorkHandler(
        config=config,
        progress_tracker=MagicMock(),
        data_source_registry=MagicMock(),
    )


class _FakeStream:
    """Async-iterable stand-in for a subprocess StreamReader (yields bytes lines)."""

    def __init__(self, lines):
        self._lines = list(lines)

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if not self._lines:
            raise StopAsyncIteration
        return self._lines.pop(0)


class _FakeProcess:
    """Minimal stand-in for asyncio.subprocess.Process."""

    def __init__(self, *, returncode=0, stdout=(), stderr=(), pid=4242, hang=False):
        self._final_returncode = returncode
        self.returncode = None  # mirrors asyncio: None until wait() completes
        self.pid = pid
        self.stdout = _FakeStream(stdout)
        self.stderr = _FakeStream(stderr)
        self._hang = hang

    async def wait(self) -> int:
        if self._hang:
            await asyncio.Event().wait()  # never returns; the test patches wait_for
        self.returncode = self._final_returncode
        return self._final_returncode


def _patch_spawn(proc: _FakeProcess):
    return patch(
        "worker.work_handler.asyncio.create_subprocess_exec",
        AsyncMock(return_value=proc),
    )


@pytest.mark.asyncio
async def test_subprocess_success_streams_and_discards_process(tmp_path):
    handler = _handler()
    proc = _FakeProcess(returncode=0, stdout=[b"progress line\n"])

    with _patch_spawn(proc):
        await handler._run_processing_subprocess(
            _work_unit(), "/tmp/in.nc", tmp_path / "m.json"
        )

    assert proc.returncode == 0
    assert handler._processes == set()  # tracked during, discarded after


@pytest.mark.asyncio
async def test_subprocess_nonzero_exit_raises_with_stderr_tail(tmp_path):
    handler = _handler()
    proc = _FakeProcess(returncode=1, stderr=[b"Traceback...\n", b"BoomError: nope\n"])

    with _patch_spawn(proc):
        with pytest.raises(RuntimeError, match="exit code 1") as excinfo:
            await handler._run_processing_subprocess(
                _work_unit(), "/tmp/in.nc", tmp_path / "m.json"
            )

    assert "BoomError: nope" in str(excinfo.value)
    assert handler._processes == set()


@pytest.mark.asyncio
async def test_subprocess_timeout_kills_group_and_raises(tmp_path):
    handler = _handler()
    proc = _FakeProcess(hang=True, pid=9100)

    killed = []

    async def fake_wait_for(coro, timeout):
        coro.close()  # avoid 'coroutine never awaited' warning
        raise asyncio.TimeoutError

    with _patch_spawn(proc), patch(
        "worker.work_handler.asyncio.wait_for", side_effect=fake_wait_for
    ), patch("worker.work_handler.os.getpgid", side_effect=lambda pid: pid), patch(
        "worker.work_handler.os.killpg",
        side_effect=lambda pgid, sig: killed.append((pgid, sig)),
    ):
        with pytest.raises(RuntimeError, match="timed out"):
            await handler._run_processing_subprocess(
                _work_unit(), "/tmp/in.nc", tmp_path / "m.json"
            )

    assert (9100, signal.SIGKILL) in killed  # process group SIGKILLed
    assert handler._processes == set()


def test_abort_signals_all_live_process_groups_only():
    handler = _handler()
    live1 = _FakeProcess(pid=111)
    live2 = _FakeProcess(pid=222)
    exited = _FakeProcess(pid=333)
    exited.returncode = 0  # already finished — must be skipped
    handler._processes = {live1, live2, exited}

    signalled = []
    with patch("worker.work_handler.Thread") as mock_thread, patch(
        "worker.work_handler.os.getpgid", side_effect=lambda pid: pid
    ), patch(
        "worker.work_handler.os.killpg",
        side_effect=lambda pgid, sig: signalled.append((pgid, sig)),
    ):
        handler.abort()

    assert sorted(pgid for pgid, _ in signalled) == [111, 222]
    assert all(sig == signal.SIGTERM for _, sig in signalled)
    mock_thread.assert_called_once()  # SIGKILL escalation scheduled (not started)


def test_abort_is_a_noop_when_no_live_processes():
    handler = _handler()
    with patch("worker.work_handler.os.killpg") as mock_killpg, patch(
        "worker.work_handler.Thread"
    ) as mock_thread:
        handler.abort()

    mock_killpg.assert_not_called()
    mock_thread.assert_not_called()
