import asyncio
import sys
import os
from unittest import mock
from unittest.mock import MagicMock, AsyncMock

# Ensure src is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import pytest
from scheduler import start_scheduler, _create_job_callback, _run_job

# Config mock needs to be applied before importing/running scheduler.start_scheduler
# but start_scheduler imports config.
# We can mock `config.config.get_job_schedules`

@pytest.fixture
def mock_config():
    with mock.patch('scheduler.config.get_job_schedules') as mock_get:
        yield mock_get

@pytest.mark.asyncio
async def test_start_scheduler_registers_jobs(mock_config):
    # Setup
    mock_config.return_value = {
        "job_a": "*/10 * * * *",
        "job_b": "0 0 * * *"
    }

    # Mock config.TIMEZONE
    with mock.patch('scheduler.config.TIMEZONE', 'America/New_York'):
        mock_job_a = MagicMock()
        mock_job_a.__name__ = "JobA"
        mock_job_b = MagicMock()
        mock_job_b.__name__ = "JobB"
        
        job_registry = {
            "job_a": mock_job_a,
            "job_b": mock_job_b
        }

        # Mock APScheduler
        with mock.patch('scheduler.AsyncIOScheduler') as MockScheduler:
            scheduler_instance = MockScheduler.return_value
            scheduler_instance.start = MagicMock()
            scheduler_instance.shutdown = MagicMock()
            scheduler_instance.get_jobs.return_value = [1, 2] # just for the logging count

            # We need to interrupt the infinite wait in start_scheduler
            # start_scheduler waits on `stop_event.wait()`
            # We can mock asyncio.Event to return immediately or throw CancelledError
            
            with mock.patch('asyncio.Event') as MockEvent:
                event_instance = MockEvent.return_value
                # Make wait() raise CancelledError immediately to exit the loop
                event_instance.wait = AsyncMock(side_effect=asyncio.CancelledError)

                await start_scheduler(job_registry)
                
                # Check Scheduler init timezone
                MockScheduler.assert_called_with(timezone='America/New_York')

                # Verification
                assert scheduler_instance.add_job.call_count == 2
                
                # Verify triggers
                calls = scheduler_instance.add_job.call_args_list
                from apscheduler.triggers.cron import CronTrigger
                
                # Check job_a (*/10 * * * *)
                job_a_call = next(c for c in calls if c.kwargs['id'] == 'job_a')
                trigger_a = job_a_call.kwargs['trigger']
                assert isinstance(trigger_a, CronTrigger)
                assert "minute='*/10'" in str(trigger_a) 
                # assert trigger timezone. APScheduler converts string to tzinfo.
                # checking str(trigger_a.timezone) should be enough or str(trigger_a) contains 'America/New_York'
                assert str(trigger_a.timezone) == 'America/New_York'

                # Check job_b (0 0 * * *)
                job_b_call = next(c for c in calls if c.kwargs['id'] == 'job_b')
                trigger_b = job_b_call.kwargs['trigger']
                assert isinstance(trigger_b, CronTrigger)
                assert "hour='0', minute='0'" in str(trigger_b)
                assert str(trigger_b.timezone) == 'America/New_York'
            
            scheduler_instance.start.assert_called_once()
            scheduler_instance.shutdown.assert_called_once()

@pytest.mark.asyncio
async def test_create_job_callback():
    mock_job_cls = MagicMock()
    mock_job_cls.__name__ = "TestJob"
    mock_instance = AsyncMock()
    mock_job_cls.return_value = mock_instance
    
    callback = _create_job_callback(mock_job_cls)
    await callback()
    
    mock_job_cls.assert_called_once()
    mock_instance.run.assert_called_once()


@pytest.mark.asyncio
async def test_run_job_respects_size_limit(mock_config):
    # Setup
    mock_job_cls = MagicMock()
    mock_job_cls.__name__ = "TestJob"
    mock_instance = AsyncMock()
    mock_job_cls.return_value = mock_instance

    # Mock _get_directory_size
    with mock.patch('scheduler._get_directory_size') as mock_get_size:
        # Mock config values
        with mock.patch('scheduler.config.MAX_TMP_DIR_SIZE_BYTES', 1000):
            
            # Case 1: Size OK (500 < 1000)
            mock_get_size.return_value = 500
            await _run_job(mock_job_cls)
            mock_instance.run.assert_called()
            mock_instance.run.reset_mock()

            # Case 2: Size Exceeded (1500 > 1000)
            mock_get_size.return_value = 1500
            await _run_job(mock_job_cls)
            mock_instance.run.assert_not_called()

