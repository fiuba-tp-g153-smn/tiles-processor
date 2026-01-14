"""
Tests for the APScheduler-based job scheduler.
"""
import asyncio
import sys
import os
from unittest import mock
from unittest.mock import MagicMock, AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import pytest
from scheduler import start_scheduler, _create_job_runner, _get_directory_size


@pytest.fixture
def mock_config():
    """Mock config for scheduler tests."""
    with mock.patch('scheduler.config') as mock_cfg:
        mock_cfg.TIMEZONE = 'UTC'
        mock_cfg.TMP_DIR = '.tmp'
        mock_cfg.MAX_TMP_DIR_SIZE_BYTES = 10 * 1024 * 1024 * 1024  # 10GB
        mock_cfg.get_job_schedules.return_value = {
            "job_a": "*/10 * * * *",
            "job_b": "0 0 * * *"
        }
        yield mock_cfg


class TestCreateJobRunner:
    """Tests for the _create_job_runner function."""

    @pytest.mark.asyncio
    async def test_job_runner_executes_job(self, mock_config):
        """Test that job runner executes the job's run method."""
        mock_job_instance = MagicMock()
        mock_job_instance.run = AsyncMock()

        mock_job_cls = MagicMock(return_value=mock_job_instance)
        mock_job_cls.__name__ = "TestJob"

        runner = _create_job_runner(mock_job_cls, "test_job")

        with patch('scheduler._get_directory_size', return_value=0):
            await runner()

        mock_job_cls.assert_called_once()
        mock_job_instance.run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_job_runner_skips_when_disk_full(self, mock_config):
        """Test that job runner skips execution when disk limit exceeded."""
        mock_job_instance = MagicMock()
        mock_job_instance.run = AsyncMock()

        mock_job_cls = MagicMock(return_value=mock_job_instance)
        mock_job_cls.__name__ = "TestJob"

        runner = _create_job_runner(mock_job_cls, "test_job")

        # Simulate disk full (11GB > 10GB limit)
        with patch('scheduler._get_directory_size', return_value=11 * 1024**3):
            await runner()

        # Job should not be instantiated or run
        mock_job_cls.assert_not_called()
        mock_job_instance.run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_job_runner_handles_exceptions(self, mock_config):
        """Test that job runner catches exceptions without crashing."""
        mock_job_instance = MagicMock()
        mock_job_instance.run = AsyncMock(side_effect=Exception("Job failed"))

        mock_job_cls = MagicMock(return_value=mock_job_instance)
        mock_job_cls.__name__ = "FailingJob"

        runner = _create_job_runner(mock_job_cls, "failing_job")

        with patch('scheduler._get_directory_size', return_value=0):
            # Should not raise
            await runner()

        mock_job_instance.run.assert_awaited_once()

    def test_job_runner_has_correct_name(self, mock_config):
        """Test that job runner function has descriptive name."""
        mock_job_cls = MagicMock()
        mock_job_cls.__name__ = "MyJob"

        runner = _create_job_runner(mock_job_cls, "my_job")

        assert runner.__name__ == "run_my_job"


class TestStartScheduler:
    """Tests for the start_scheduler function."""

    @pytest.mark.asyncio
    async def test_scheduler_registers_jobs_with_correct_settings(self, mock_config):
        """Test that scheduler registers jobs with APScheduler best practices."""
        mock_job_a = MagicMock()
        mock_job_a.__name__ = "JobA"
        mock_job_b = MagicMock()
        mock_job_b.__name__ = "JobB"

        job_registry = {
            "job_a": mock_job_a,
            "job_b": mock_job_b
        }

        with patch('scheduler.AsyncIOScheduler') as MockScheduler:
            scheduler_instance = MockScheduler.return_value
            scheduler_instance.get_jobs.return_value = [1, 2]

            stop_event = asyncio.Event()

            # Schedule stop after brief delay
            async def stop_after_delay():
                await asyncio.sleep(0.05)
                stop_event.set()

            asyncio.create_task(stop_after_delay())

            await start_scheduler(job_registry, stop_event)

            # Verify scheduler was created with correct timezone
            MockScheduler.assert_called_once_with(timezone='UTC')

            # Verify add_job was called twice
            assert scheduler_instance.add_job.call_count == 2

            # Verify APScheduler best practices are used
            for call in scheduler_instance.add_job.call_args_list:
                kwargs = call.kwargs
                assert kwargs['max_instances'] == 1  # Prevent overlap
                assert kwargs['coalesce'] is True    # Merge missed runs
                assert kwargs['replace_existing'] is True
                assert 'misfire_grace_time' in kwargs

            scheduler_instance.start.assert_called_once()
            scheduler_instance.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_scheduler_skips_jobs_without_schedule(self, mock_config):
        """Test that jobs without schedules are skipped."""
        mock_config.get_job_schedules.return_value = {
            "job_a": "*/10 * * * *"
            # job_b has no schedule
        }

        mock_job_a = MagicMock()
        mock_job_a.__name__ = "JobA"
        mock_job_b = MagicMock()
        mock_job_b.__name__ = "JobB"

        job_registry = {
            "job_a": mock_job_a,
            "job_b": mock_job_b  # This one has no schedule
        }

        with patch('scheduler.AsyncIOScheduler') as MockScheduler:
            scheduler_instance = MockScheduler.return_value
            scheduler_instance.get_jobs.return_value = [1]

            stop_event = asyncio.Event()
            stop_event.set()  # Stop immediately

            await start_scheduler(job_registry, stop_event)

            # Only job_a should be added
            assert scheduler_instance.add_job.call_count == 1
            call_kwargs = scheduler_instance.add_job.call_args.kwargs
            assert call_kwargs['id'] == 'job_a'

    @pytest.mark.asyncio
    async def test_scheduler_handles_cancellation(self, mock_config):
        """Test graceful shutdown on cancellation."""
        job_registry = {}

        with patch('scheduler.AsyncIOScheduler') as MockScheduler:
            scheduler_instance = MockScheduler.return_value
            scheduler_instance.get_jobs.return_value = []

            stop_event = asyncio.Event()

            # Create task and cancel it
            task = asyncio.create_task(start_scheduler(job_registry, stop_event))
            await asyncio.sleep(0.01)
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

            scheduler_instance.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_scheduler_uses_cron_triggers(self, mock_config):
        """Test that CRON triggers are created correctly."""
        mock_job = MagicMock()
        mock_job.__name__ = "TestJob"

        job_registry = {"job_a": mock_job}

        with patch('scheduler.AsyncIOScheduler') as MockScheduler:
            scheduler_instance = MockScheduler.return_value
            scheduler_instance.get_jobs.return_value = [1]

            stop_event = asyncio.Event()
            stop_event.set()

            await start_scheduler(job_registry, stop_event)

            # Check that CronTrigger was used
            call_kwargs = scheduler_instance.add_job.call_args.kwargs
            from apscheduler.triggers.cron import CronTrigger
            assert isinstance(call_kwargs['trigger'], CronTrigger)


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
