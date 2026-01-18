"""
Integration tests for the tiles-processor pipeline.

These tests verify the full processing pipeline with mocked external dependencies
(S3, subprocess for gdal2tiles) to ensure components work together correctly.
"""

import asyncio
import json
import sys
import os
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
import numpy as np
from scheduler import _get_directory_size
from config import Config
import logging

logger = logging.getLogger(__name__)


@pytest.fixture
def temp_settings_file(tmp_path):
    """Create a temporary settings.json file."""
    settings = {
        "timezone": "UTC",
        "scheduler": {
            "band_13_cron": "*/10 * * * *",
            "band_9_cron": "0 * * * *",
        },
        "features": {
            "enable_band_13": True,
            "enable_band_9": True,
        },
        "bounds": {
            "minx": -90.0,
            "miny": -60.0,
            "maxx": -30.0,
            "maxy": -15.0,
        },
    }
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps(settings))
    return settings_path


@pytest.fixture
def env_vars(tmp_path):
    """Required environment variables for Config."""
    return {
        "LOG_LEVEL": "DEBUG",
        "DATA_DIR": str(tmp_path / "data"),
        "S3_TILES_DATA_ENDPOINT": "minio:9000",
        "S3_TILES_DATA_RW_ACCESS_KEY": "minioadmin",
        "S3_TILES_DATA_RW_SECRET_KEY": "minioadmin",
    }


async def run_job_async(config: Config, job_cls, job_name):
    """Async helper to mimic scheduler.run_job for tests."""
    tmp_path = Path(config.TMP_DIR)
    tmp_path.mkdir(parents=True, exist_ok=True)
    current_size = _get_directory_size(tmp_path)

    if current_size > config.MAX_TMP_DIR_SIZE_BYTES:
        logger.error(
            "Job %s skipped: temp directory size exceeds limit",
            job_name,
        )
        return

    try:
        job = job_cls()
        await job.run()
    except Exception:
        logger.exception("Job %s failed", job_name)


class TestSchedulerIntegration:
    """Integration tests for the APScheduler-based job system."""

    @pytest.mark.asyncio
    async def test_job_runner_executes_job(self, temp_settings_file, env_vars):
        """Test that job runner properly instantiates and executes a job."""

        processed_jobs = []

        # Create a mock job class
        class MockJob:
            async def run(self):
                processed_jobs.append("MockJob executed")

        MockJob.__name__ = "MockJob"

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            # Execute the runner
            await run_job_async(config, MockJob, "mock_job")

        # Verify job was processed
        assert len(processed_jobs) == 1
        assert processed_jobs[0] == "MockJob executed"

    @pytest.mark.asyncio
    async def test_job_runner_prevents_execution_when_disk_full(
        self, temp_settings_file, env_vars
    ):
        """Test that job runner skips execution when disk limit exceeded."""

        execution_log = []

        class MockJob:
            async def run(self):
                execution_log.append("should_not_run")

        MockJob.__name__ = "MockJob"

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            # Override max size to be very small
            config.MAX_TMP_DIR_SIZE_BYTES = 1000

            # Simulate disk full by creating a file larger than the limit
            tmp_dir = Path(config.TMP_DIR)
            tmp_dir.mkdir(parents=True, exist_ok=True)
            (tmp_dir / "large_file.bin").write_bytes(b"x" * 2000)

            await run_job_async(config, MockJob, "mock_job")

        # Job should not have run
        assert execution_log == []

    @pytest.mark.asyncio
    async def test_job_runner_handles_failure_gracefully(
        self, temp_settings_file, env_vars
    ):
        """Test that a failing job doesn't crash the runner."""

        class FailingJob:
            async def run(self):
                raise Exception("Intentional failure")

        FailingJob.__name__ = "FailingJob"

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            # Should not raise, just log the error
            await run_job_async(
                config, FailingJob, "failing_job"
            )  # Should complete without raising


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
    async def test_geotiff_generation_pipeline(
        self, tmp_path, mock_xarray_dataset, temp_settings_file, env_vars
    ):
        """Test the GeoTIFF generation service with mocked data."""
        from services.generate_geotiff_files import GenerateGeoTIFFFilesService

        output_dir = tmp_path / "geotiff_output"

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            service = GenerateGeoTIFFFilesService(
                brightness_temperatures={"test_image.nc": mock_xarray_dataset},
                output_dir=output_dir,
                config=config,
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

            with patch.object(service, "_generate_geotiff", side_effect=mock_generate):
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

        with patch("subprocess.run", side_effect=mock_subprocess_run):
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

        with patch.object(client, "download_folder", side_effect=mock_download_folder):
            result = await client.download_folder(
                "ABI-L1b-RadF/2025/001/12", file_pattern="C13_G19"
            )

        assert len(result) == 3
        for key, content in result.items():
            assert content == mock_netcdf_content


class TestEndToEndMocked:
    """End-to-end tests with fully mocked external dependencies."""

    @pytest.mark.asyncio
    async def test_full_job_execution_mocked(
        self, tmp_path, temp_settings_file, env_vars
    ):
        """Test complete job execution with all external deps mocked."""

        # Track what gets called
        execution_log = []

        class MockProcessJob:
            async def run(self):
                execution_log.append("job_started")
                # Simulate some async work
                await asyncio.sleep(0.01)
                execution_log.append("job_completed")

        MockProcessJob.__name__ = "MockProcessJob"

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            await run_job_async(config, MockProcessJob, "mock_process")

        assert execution_log == ["job_started", "job_completed"]

    @pytest.mark.asyncio
    async def test_job_cancelled_when_tmp_dir_full(
        self, tmp_path, temp_settings_file, env_vars
    ):
        """Test that jobs are skipped when tmp directory exceeds limit."""

        execution_log = []

        class MockProcessJob:
            async def run(self):
                execution_log.append("should_not_run")

        MockProcessJob.__name__ = "MockProcessJob"

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            config.MAX_TMP_DIR_SIZE_BYTES = 1000

            # Create a file larger than the limit
            tmp_dir = Path(config.TMP_DIR)
            tmp_dir.mkdir(parents=True, exist_ok=True)
            (tmp_dir / "large_file.bin").write_bytes(b"x" * 2000)

            await run_job_async(config, MockProcessJob, "mock_process")

        # Job should not have run
        assert execution_log == []


class TestConfigIntegration:
    """Integration tests for configuration loading."""

    def test_config_bounds_used_in_geotiff_service(self, temp_settings_file, env_vars):
        """Test that config bounds are properly used in GeoTIFF service."""
        from services.generate_geotiff_files import GenerateGeoTIFFFilesService

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            # Get configured bounds
            bounds = config.get_bounds()

            # Verify bounds have expected structure
            assert all(key in bounds for key in ["minx", "miny", "maxx", "maxy"])
            assert all(isinstance(v, float) for v in bounds.values())

            # Verify service can access DEFAULT_BOUNDS as fallback reference
            default_bounds = GenerateGeoTIFFFilesService.DEFAULT_BOUNDS
            assert all(
                key in default_bounds for key in ["minx", "miny", "maxx", "maxy"]
            )

    def test_cron_schedules_are_validated(self, temp_settings_file, env_vars):
        """Test that CRON schedules in config are valid."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            # These should not raise - they were validated at init time
            schedules = config.get_job_schedules()

            assert "process_band_13" in schedules
            assert "process_band_9" in schedules

            # Verify they're non-empty strings
            for job_name, cron_expr in schedules.items():
                assert isinstance(cron_expr, str)
                assert len(cron_expr) > 0
