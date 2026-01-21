import pytest
import time
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import healthcheck
from worker.worker import Worker


@pytest.fixture
def mock_health_file(tmp_path):
    """Fixture to provide a temporary health file path."""
    return tmp_path / "healthy"


@pytest.fixture
def mock_worker(mock_health_file):
    """Fixture to provide a mock worker with configuration."""
    config = MagicMock()
    rabbitmq = MagicMock()
    tracker = MagicMock()

    # Patch the global HEALTH_FILE in worker module
    with patch("worker.worker.HEALTH_FILE", mock_health_file):
        worker = Worker(config, rabbitmq, tracker)
        yield worker


def test_update_heartbeat_creates_file(mock_worker, mock_health_file):
    """Verify _update_heartbeat creates the file."""
    # Ensure it starts empty
    if mock_health_file.exists():
        mock_health_file.unlink()

    mock_worker._update_heartbeat()

    assert mock_health_file.exists()
    # Verify access/mod time is recent
    assert (time.time() - mock_health_file.stat().st_mtime) < 1.0


def test_update_heartbeat_updates_existing_file(mock_worker, mock_health_file):
    """Verify _update_heartbeat updates timestamp of existing file."""
    # Create file with old timestamp
    mock_health_file.touch()
    old_mtime = time.time() - 100
    os.utime(mock_health_file, (old_mtime, old_mtime))

    mock_worker._update_heartbeat()

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

    # Make file in future to ensure age calc doesn't break?
    # Actually we want past. 301 seconds ago.
    # checking imports in healthcheck.py: MAX_DELAY_SECONDS = 300

    old_time = time.time() - 301
    os.utime(mock_health_file, (old_time, old_time))

    with patch("healthcheck.HEALTH_FILE", mock_health_file):
        with pytest.raises(SystemExit) as e:
            healthcheck.check_health()
        assert e.value.code == 1
