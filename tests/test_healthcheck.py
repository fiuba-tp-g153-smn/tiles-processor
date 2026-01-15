import pytest
import time
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from jobs.heartbeat_job import HeartbeatJob
import healthcheck


@pytest.fixture
def mock_health_file(tmp_path):
    """Fixture to provide a temporary health file path."""
    return tmp_path / "healthy"


@pytest.mark.asyncio
async def test_heartbeat_job_creates_file(mock_health_file):
    """Verify HeartbeatJob creates/updates the file."""
    # Monkeypatch the class attribute to use our temp file
    with patch.object(HeartbeatJob, "HEALTH_FILE", mock_health_file):
        job = HeartbeatJob()
        await job.run()

        assert mock_health_file.exists()
        # Verify access/mod time is recent
        assert (time.time() - mock_health_file.stat().st_mtime) < 1.0


@pytest.mark.asyncio
async def test_heartbeat_job_updates_existing_file(mock_health_file):
    """Verify HeartbeatJob updates timestamp of existing file."""
    # Create file with old timestamp
    mock_health_file.touch()
    old_mtime = time.time() - 100
    os.utime(mock_health_file, (old_mtime, old_mtime))

    with patch.object(HeartbeatJob, "HEALTH_FILE", mock_health_file):
        job = HeartbeatJob()
        await job.run()

        new_mtime = mock_health_file.stat().st_mtime
        assert new_mtime > old_mtime
        assert (time.time() - new_mtime) < 1.0


def test_check_health_passes_fresh_file(mock_health_file):
    """Verify check_health passes (exit 0) when file is fresh."""
    mock_health_file.touch()

    with patch("healthcheck.HEALTH_FILE", mock_health_file):
        with pytest.raises(SystemExit) as e:
            healthcheck.check_health()
        assert e.value.code == 0


def test_check_health_fails_missing_file(mock_health_file):
    """Verify check_health fails (exit 1) when file is missing."""
    if mock_health_file.exists():
        mock_health_file.unlink()

    with patch("healthcheck.HEALTH_FILE", mock_health_file):
        with pytest.raises(SystemExit) as e:
            healthcheck.check_health()
        assert e.value.code == 1


def test_check_health_fails_stale_file(mock_health_file):
    """Verify check_health fails (exit 1) when file is too old."""
    mock_health_file.touch()

    # Make file 3 minutes old (older than default 2 min max)
    old_time = time.time() - 180
    os.utime(mock_health_file, (old_time, old_time))

    with patch("healthcheck.HEALTH_FILE", mock_health_file):
        with pytest.raises(SystemExit) as e:
            healthcheck.check_health()
        assert e.value.code == 1
