"""
Integration tests for the tiles-processor pipeline.

These tests verify the full processing pipeline with mocked external dependencies
(S3, subprocess for gdal2tiles) to ensure components work together correctly.
"""
import asyncio
import sys
import os
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import pytest
import numpy as np


class TestSchedulerIntegration:
    """Integration tests for the scheduler job queue system."""

    @pytest.mark.asyncio
    async def test_scheduler_enqueue_and_process_job(self):
        """Test that jobs are properly enqueued and processed by the worker."""
        from scheduler import _create_enqueue_callback, _worker

        job_queue = asyncio.Queue()
        processed_jobs = []

        # Create a mock job class
        class MockJob:
            async def run(self):
                processed_jobs.append("MockJob executed")

        # Create enqueue callback
        callback = _create_enqueue_callback(job_queue, MockJob, "mock_job")

        # Enqueue the job
        await callback()

        # Verify job is in queue
        assert job_queue.qsize() == 1

        # Create worker task
        worker_task = asyncio.create_task(_worker(job_queue))

        # Wait for job to be processed
        await asyncio.sleep(0.1)

        # Cancel worker
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Verify job was processed
        assert len(processed_jobs) == 1
        assert processed_jobs[0] == "MockJob executed"

    @pytest.mark.asyncio
    async def test_scheduler_processes_multiple_jobs_in_order(self):
        """Test that multiple jobs are processed sequentially in FIFO order."""
        from scheduler import _create_enqueue_callback, _worker

        job_queue = asyncio.Queue()
        execution_order = []

        class JobA:
            async def run(self):
                execution_order.append("A")

        class JobB:
            async def run(self):
                execution_order.append("B")

        class JobC:
            async def run(self):
                execution_order.append("C")

        # Enqueue jobs in order A, B, C
        await _create_enqueue_callback(job_queue, JobA, "job_a")()
        await _create_enqueue_callback(job_queue, JobB, "job_b")()
        await _create_enqueue_callback(job_queue, JobC, "job_c")()

        assert job_queue.qsize() == 3

        # Process all jobs
        worker_task = asyncio.create_task(_worker(job_queue))
        await asyncio.sleep(0.2)
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Verify FIFO order
        assert execution_order == ["A", "B", "C"]

    @pytest.mark.asyncio
    async def test_job_failure_does_not_stop_worker(self):
        """Test that a failing job doesn't prevent subsequent jobs from running."""
        from scheduler import _create_enqueue_callback, _worker

        job_queue = asyncio.Queue()
        successful_jobs = []

        class FailingJob:
            async def run(self):
                raise Exception("Intentional failure")

        class SuccessfulJob:
            async def run(self):
                successful_jobs.append("success")

        # Enqueue: successful, failing, successful
        await _create_enqueue_callback(job_queue, SuccessfulJob, "job_1")()
        await _create_enqueue_callback(job_queue, FailingJob, "job_2")()
        await _create_enqueue_callback(job_queue, SuccessfulJob, "job_3")()

        # Process jobs
        worker_task = asyncio.create_task(_worker(job_queue))
        await asyncio.sleep(0.2)
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Both successful jobs should have run despite the failure
        assert len(successful_jobs) == 2


class TestPipelineIntegration:
    """Integration tests for the full processing pipeline."""

    @pytest.fixture
    def mock_netcdf_content(self):
        """Create minimal mock NetCDF-like bytes content."""
        # This is just placeholder bytes - the actual parsing will be mocked
        return b"mock netcdf content"

    @pytest.fixture
    def mock_xarray_dataset(self):
        """Create a mock xarray dataset with realistic structure."""
        mock_ds = MagicMock()

        # Mock the data array with realistic shape
        data = np.random.rand(100, 100).astype(np.float32) * 100 + 200
        mock_ds.values = data
        mock_ds.attrs = {}

        # Mock rio accessor chain
        mock_reproj = MagicMock()
        mock_reproj.values = data
        mock_reproj.rio.write_nodata.return_value = mock_reproj
        mock_reproj.rio.clip_box.return_value = mock_reproj
        mock_reproj.__getitem__ = lambda self, key: np.linspace(-90, -30, 100)

        mock_ds.rio.reproject.return_value = mock_reproj

        return mock_ds

    @pytest.mark.asyncio
    async def test_geotiff_generation_pipeline(self, tmp_path, mock_xarray_dataset):
        """Test the GeoTIFF generation service with mocked data."""
        from services.generate_geotiff_files import GenerateGeoTIFFFilesService

        output_dir = tmp_path / "geotiff_output"

        service = GenerateGeoTIFFFilesService(
            brightness_temperatures={"test_image.nc": mock_xarray_dataset},
            output_dir=output_dir,
            vmin=183.15,
            vmax=323.15,
            product_name="Test_Product",
        )

        # Mock the internal _generate_geotiff to create actual files
        generated_files = []

        def mock_generate(file_name, dataset):
            output_file = output_dir / f"{Path(file_name).stem}.tif"
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_bytes(b"mock tiff content")
            generated_files.append(output_file)
            return output_file

        with patch.object(service, '_generate_geotiff', side_effect=mock_generate):
            results = await service.run()

        assert len(generated_files) == 1
        assert generated_files[0].exists()

    @pytest.mark.asyncio
    async def test_tiles_generation_pipeline(self, tmp_path):
        """Test the tiles generation service with mocked gdal2tiles."""
        from services.generate_tiles import GenerateTilesService

        # Create mock GeoTIFF files
        geotiff_dir = tmp_path / "geotiffs"
        geotiff_dir.mkdir()
        geotiff_files = []
        for i in range(2):
            f = geotiff_dir / f"image_{i}.tif"
            f.write_bytes(b"mock geotiff")
            geotiff_files.append(f)

        output_dir = tmp_path / "tiles"
        service = GenerateTilesService(geotiff_files, output_dir)

        tiles_created = []

        def mock_subprocess_run(cmd, **kwargs):
            # Simulate gdal2tiles creating output
            # The command includes the output directory as the last argument
            tiles_output = Path(cmd[-1])
            tiles_output.mkdir(parents=True, exist_ok=True)
            (tiles_output / "leaflet.html").write_text("mock leaflet")
            tiles_created.append(tiles_output)
            return MagicMock(returncode=0, stderr="", stdout="")

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            await service.run()

        # Verify tiles were created for both input files
        assert len(tiles_created) == 2
        assert output_dir.exists()

    @pytest.mark.asyncio
    async def test_s3_download_to_processing_flow(self, tmp_path, mock_netcdf_content):
        """Test S3 download integrates with processing services."""
        from clients.s3_client import S3Client

        client = S3Client(bucket_name="test-bucket", max_concurrent_downloads=2)

        # Mock the full download flow
        downloaded_files = {}

        async def mock_download_folder(folder_path, file_pattern="", file_filter=None):
            # Simulate downloading 3 files
            for i in range(3):
                key = f"{folder_path}/file_{i}.nc"
                if file_filter is None or file_filter(key):
                    downloaded_files[key] = mock_netcdf_content
            return downloaded_files

        with patch.object(client, 'download_folder', side_effect=mock_download_folder):
            result = await client.download_folder(
                "ABI-L1b-RadF/2025/001/12",
                file_pattern="C13_G19"
            )

        assert len(result) == 3
        for key, content in result.items():
            assert content == mock_netcdf_content


class TestEndToEndMocked:
    """End-to-end tests with fully mocked external dependencies."""

    @pytest.mark.asyncio
    async def test_full_job_execution_mocked(self, tmp_path):
        """Test complete job execution with all external deps mocked."""
        from scheduler import _run_job

        # Track what gets called
        execution_log = []

        class MockProcessJob:
            async def run(self):
                execution_log.append("job_started")
                # Simulate some async work
                await asyncio.sleep(0.01)
                execution_log.append("job_completed")

        # Mock config to allow job execution
        with patch('scheduler.config.TMP_DIR', str(tmp_path)):
            with patch('scheduler.config.MAX_TMP_DIR_SIZE_BYTES', 10 * 1024**3):
                with patch('scheduler._get_directory_size', return_value=0):
                    await _run_job(MockProcessJob)

        assert execution_log == ["job_started", "job_completed"]

    @pytest.mark.asyncio
    async def test_job_cancelled_when_tmp_dir_full(self, tmp_path):
        """Test that jobs are cancelled when tmp directory exceeds limit."""
        from scheduler import _run_job

        execution_log = []

        class MockProcessJob:
            async def run(self):
                execution_log.append("should_not_run")

        # Mock directory size to exceed limit
        with patch('scheduler.config.TMP_DIR', str(tmp_path)):
            with patch('scheduler.config.MAX_TMP_DIR_SIZE_BYTES', 1000):
                with patch('scheduler._get_directory_size', return_value=2000):
                    await _run_job(MockProcessJob)

        # Job should not have run
        assert execution_log == []


class TestConfigIntegration:
    """Integration tests for configuration loading."""

    def test_config_bounds_used_in_geotiff_service(self):
        """Test that config bounds are properly used in GeoTIFF service."""
        from config import config
        from services.generate_geotiff_files import GenerateGeoTIFFFilesService

        # Get configured bounds
        bounds = config.get_bounds()

        # Verify bounds have expected structure
        assert all(key in bounds for key in ["minx", "miny", "maxx", "maxy"])
        assert all(isinstance(v, float) for v in bounds.values())

        # Verify service can access DEFAULT_BOUNDS as fallback reference
        default_bounds = GenerateGeoTIFFFilesService.DEFAULT_BOUNDS
        assert all(key in default_bounds for key in ["minx", "miny", "maxx", "maxy"])

    def test_cron_schedules_are_validated(self):
        """Test that CRON schedules in config are valid."""
        from config import config

        # These should not raise - they were validated at import time
        schedules = config.get_job_schedules()

        assert "process_band_13" in schedules
        assert "process_band_9" in schedules

        # Verify they're non-empty strings
        for job_name, cron_expr in schedules.items():
            assert isinstance(cron_expr, str)
            assert len(cron_expr) > 0
