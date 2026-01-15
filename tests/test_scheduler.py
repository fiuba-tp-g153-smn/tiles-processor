"""
Tests for the APScheduler-based job scheduler.
"""

import asyncio
import json
import sys
import os
from unittest import mock
from unittest.mock import MagicMock, AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from scheduler import start_scheduler, _get_directory_size
from config import Config


@pytest.fixture
def temp_settings_file(tmp_path):
    """Create a temporary settings.json file."""
    settings = {
        "timezone": "UTC",
        "scheduler": {
            "band_13_cron": "*/10 * * * *",
            "band_9_cron": "0 0 * * *",
        },
        "features": {"enable_band_13": True, "enable_band_9": True},
        "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
    }
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps(settings))
    return settings_path


@pytest.fixture
def env_vars(tmp_path):
    """Required environment variables for Config."""
    return {
        "LOG_LEVEL": "DEBUG",
        "DATA_DIR_CONTAINER": str(tmp_path / "data"),
    }


@pytest.fixture
def config_fixture(temp_settings_file, env_vars):
    """Create a Config instance for testing."""
    with mock.patch.dict(os.environ, env_vars, clear=True):
        return Config(settings_path=temp_settings_file)


class TestStartScheduler:
    """Tests for the start_scheduler function."""

    @pytest.mark.asyncio
    async def test_scheduler_registers_jobs_with_correct_settings(self, config_fixture):
        """Test that scheduler registers jobs with APScheduler best practices."""
        mock_job_a = MagicMock()
        mock_job_a.__name__ = "JobA"
        mock_job_b = MagicMock()
        mock_job_b.__name__ = "JobB"

        job_registry = {"process_band_13": mock_job_a, "process_band_9": mock_job_b}

        with patch("scheduler.SQLAlchemyJobStore") as MockJobStore:
            with patch("scheduler.AsyncIOScheduler") as MockScheduler:
                scheduler_instance = MockScheduler.return_value
                scheduler_instance.get_jobs.return_value = [1, 2]
                scheduler_instance.get_job.return_value = None  # Jobs don't exist yet

                stop_event = asyncio.Event()

                # Schedule stop after brief delay
                async def stop_after_delay():
                    await asyncio.sleep(0.05)
                    stop_event.set()

                asyncio.create_task(stop_after_delay())

                await start_scheduler(config_fixture, job_registry, stop_event)

                # Verify job store was created with SQLite URL
                MockJobStore.assert_called_once()
                assert "sqlite:///" in MockJobStore.call_args.kwargs["url"]

                # Verify scheduler was created with jobstores and timezone
                MockScheduler.assert_called_once()
                call_kwargs = MockScheduler.call_args.kwargs
                assert "jobstores" in call_kwargs
                assert "executors" in call_kwargs
                assert call_kwargs["timezone"] == "UTC"

                # Verify add_job was called (2 cron jobs + 2 startup jobs)
                assert scheduler_instance.add_job.call_count == 4

                scheduler_instance.start.assert_called_once()
                scheduler_instance.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_scheduler_skips_jobs_without_schedule(
        self, tmp_path, temp_settings_file
    ):
        """Test that jobs without schedules are skipped."""
        # Create config with only one job schedule
        settings = {
            "timezone": "UTC",
            "scheduler": {
                "band_13_cron": "*/10 * * * *",
                "band_9_cron": "0 0 * * *",
            },
            "features": {"enable_band_13": True, "enable_band_9": True},
            "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        }
        settings_path = tmp_path / "settings2.json"
        settings_path.write_text(json.dumps(settings))

        env_vars = {
            "LOG_LEVEL": "DEBUG",
            "DATA_DIR_CONTAINER": str(tmp_path / "data"),
        }

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=settings_path)

        mock_job_a = MagicMock()
        mock_job_a.__name__ = "JobA"
        mock_job_b = MagicMock()
        mock_job_b.__name__ = "JobB"

        # job_c has no schedule in config
        job_registry = {
            "process_band_13": mock_job_a,
            "unknown_job": mock_job_b,  # This one has no schedule
        }

        with patch("scheduler.SQLAlchemyJobStore"):
            with patch("scheduler.AsyncIOScheduler") as MockScheduler:
                scheduler_instance = MockScheduler.return_value
                scheduler_instance.get_jobs.return_value = [1]
                scheduler_instance.get_job.return_value = None

                stop_event = asyncio.Event()
                stop_event.set()  # Stop immediately

                await start_scheduler(config, job_registry, stop_event)

                # Only process_band_13 should be added (cron + startup)
                assert scheduler_instance.add_job.call_count == 2

    @pytest.mark.asyncio
    async def test_scheduler_handles_cancellation(self, config_fixture):
        """Test graceful shutdown on cancellation."""
        job_registry = {}

        with patch("scheduler.SQLAlchemyJobStore"):
            with patch("scheduler.AsyncIOScheduler") as MockScheduler:
                scheduler_instance = MockScheduler.return_value
                scheduler_instance.get_jobs.return_value = []

                stop_event = asyncio.Event()

                # Create task and cancel it
                task = asyncio.create_task(
                    start_scheduler(config_fixture, job_registry, stop_event)
                )
                await asyncio.sleep(0.01)
                task.cancel()

                with pytest.raises(asyncio.CancelledError):
                    await task

                scheduler_instance.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_scheduler_uses_cron_triggers(self, config_fixture):
        """Test that CRON triggers are created correctly."""
        mock_job = MagicMock()
        mock_job.__name__ = "TestJob"

        job_registry = {"process_band_13": mock_job}

        with patch("scheduler.SQLAlchemyJobStore"):
            with patch("scheduler.AsyncIOScheduler") as MockScheduler:
                scheduler_instance = MockScheduler.return_value
                scheduler_instance.get_jobs.return_value = [1]
                scheduler_instance.get_job.return_value = MagicMock()  # Job exists

                stop_event = asyncio.Event()
                stop_event.set()

                await start_scheduler(config_fixture, job_registry, stop_event)

                # Check that CronTrigger was used (only cron job, no startup)
                call_kwargs = scheduler_instance.add_job.call_args.kwargs
                from apscheduler.triggers.cron import CronTrigger

                assert isinstance(call_kwargs["trigger"], CronTrigger)


class TestGetDirectorySize:
    """Tests for the _get_directory_size function."""

    def test_returns_zero_for_nonexistent_path(self, tmp_path):
        """Test that non-existent path returns 0."""
        result = _get_directory_size(tmp_path / "nonexistent")
        assert result == 0

    def test_calculates_size_correctly(self, tmp_path):
        """Test that directory size is calculated correctly."""
        # Create some files
        (tmp_path / "file1.txt").write_bytes(b"a" * 100)
        (tmp_path / "file2.txt").write_bytes(b"b" * 200)

        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_bytes(b"c" * 300)

        result = _get_directory_size(tmp_path)
        assert result == 600

    def test_skips_symlinks(self, tmp_path):
        """Test that symbolic links are not counted."""
        real_file = tmp_path / "real.txt"
        real_file.write_bytes(b"x" * 100)

        link_file = tmp_path / "link.txt"
        try:
            link_file.symlink_to(real_file)
        except OSError:
            pytest.skip("Symlinks not supported on this platform")

        result = _get_directory_size(tmp_path)
        # Should only count real.txt, not the symlink
        assert result == 100
