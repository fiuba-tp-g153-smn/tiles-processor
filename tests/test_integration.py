"""
Integration tests for the tiles-processor pipeline.

These tests verify the full processing pipeline with mocked external dependencies
(RabbitMQ, S3, subprocess for gdal2tiles) to ensure components work together correctly.
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
from config import Config
import logging
from data_sources.ecmwf_producer_source import TransientDownloadError
from exceptions import UnprocessableInputError
from worker.worker import Worker
from worker.work_handler import WorkHandler
from models.work_unit import WorkUnit
from models.band_config import BandConfig

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
        "S3_TILES_DATA_ENDPOINT": "s3-service:9000",
        "S3_TILES_DATA_TILES_PROCESSOR_USER": "s3admin",
        "S3_TILES_DATA_TILES_PROCESSOR_PASSWORD": "s3admin",
        "S3_TILES_DATA_BUCKET_NAME": "tiles-data",
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_QUEUE": "tiles_work_queue",
        "RABBITMQ_DLQ": "tiles_dead_letter_queue",
        "RABBITMQ_DLX": "tiles_dlx",
        "JOB_TTL_MINUTES": "20",
    }


class TestWorkerIntegration:
    """Integration tests for the Worker-based processing system."""

    @pytest.fixture
    def mock_rabbitmq(self):
        return MagicMock()

    @pytest.fixture
    def mock_tracker(self):
        return MagicMock()

    def test_worker_processes_message_successfully(
        self, temp_settings_file, env_vars, mock_rabbitmq, mock_tracker
    ):
        """Test that worker processes a work unit and acknowledges it."""

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            worker = Worker(config, mock_rabbitmq, mock_tracker)

            # Mock the handler to simulate successful processing
            mock_handler = MagicMock()
            mock_handler.handle = AsyncMock()
            worker._handler = mock_handler

            # Create a test work unit using new format
            work_unit = WorkUnit.create(
                image_id="test_image.nc",
                source_uri="ABI-L1b-RadF/2025/001/12/test_image.nc",
                data_source_id="goes19_abi_band_13",
                processor_id="goes_band_13",
                output_prefix="tiles/band_13",
                bounds=config.get_bounds(),
                band_id="band_13",
            )

            # Process the message (the coroutine acks via the MQ client now)
            asyncio.run(worker._process_message_async(work_unit, 1, "tiles_work_queue"))

            # Verify acknowledgement
            mock_rabbitmq.ack.assert_called_once_with(1)

            # Verify handler was called with the work unit (plus the per-job
            # metrics collector the worker now threads through as 2nd arg).
            mock_handler.handle.assert_called_once_with(work_unit, mock.ANY)

            # Verify no retry or DLQ publish since processing succeeded
            mock_rabbitmq.publish.assert_not_called()
            mock_rabbitmq.publish_to_dlq.assert_not_called()

    def test_worker_handles_failure_and_retries(
        self, temp_settings_file, env_vars, mock_rabbitmq, mock_tracker
    ):
        """Test that worker retries on failure."""

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            worker = Worker(config, mock_rabbitmq, mock_tracker)

            # Mock handler to raise exception
            mock_handler = MagicMock()
            mock_handler.handle = AsyncMock(side_effect=Exception("Processing failed"))
            worker._handler = mock_handler

            # Create work unit
            work_unit = WorkUnit.create(
                image_id="test_image.nc",
                source_uri="ABI-L1b-RadF/2025/001/12/test_image.nc",
                data_source_id="goes19_abi_band_13",
                processor_id="goes_band_13",
                output_prefix="tiles/band_13",
                bounds=config.get_bounds(),
                band_id="band_13",
            )

            # Process (light unit stolen by a normal worker: came from a light queue)
            asyncio.run(
                worker._process_message_async(work_unit, 1, "tiles_radar_light_queue")
            )

            # Should still acknowledge (to remove the original message)
            mock_rabbitmq.ack.assert_called_once_with(1)

            # Should publish retry back to the queue it came from, not the
            # worker's primary queue.
            assert mock_rabbitmq.publish.call_count == 1
            retry_unit = mock_rabbitmq.publish.call_args[0][0]
            assert retry_unit.retry_count == 1
            assert mock_rabbitmq.publish.call_args.kwargs["queue_name"] == (
                "tiles_radar_light_queue"
            )

    def test_worker_sends_to_dlq_after_max_retries(
        self, temp_settings_file, env_vars, mock_rabbitmq, mock_tracker
    ):
        """Test that worker sends to DLQ after max retries."""

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            worker = Worker(config, mock_rabbitmq, mock_tracker)

            # Mock handler to raise exception
            mock_handler = MagicMock()
            mock_handler.handle = AsyncMock(side_effect=Exception("Persistent failure"))
            worker._handler = mock_handler

            # Create work unit with max retries reached
            work_unit = WorkUnit.create(
                image_id="test_image.nc",
                source_uri="ABI-L1b-RadF/2025/001/12/test_image.nc",
                data_source_id="goes19_abi_band_13",
                processor_id="goes_band_13",
                output_prefix="tiles/band_13",
                bounds=config.get_bounds(),
                band_id="band_13",
            )
            work_unit.retry_count = 3
            work_unit.max_retries = 3

            # Process
            asyncio.run(worker._process_message_async(work_unit, 1, "tiles_work_queue"))

            # Should not publish retry
            mock_rabbitmq.publish.assert_not_called()

            # Should send to DLQ, then ack the original
            mock_rabbitmq.publish_to_dlq.assert_called_once()
            mock_rabbitmq.ack.assert_called_once_with(1)

    def test_worker_skips_unprocessable_input(
        self, temp_settings_file, env_vars, mock_rabbitmq, mock_tracker
    ):
        """Unprocessable input is acked as SKIPPED — no retry, no DLQ, no release."""

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            worker = Worker(config, mock_rabbitmq, mock_tracker)

            mock_handler = MagicMock()
            mock_handler.handle = AsyncMock(
                side_effect=UnprocessableInputError(
                    "Incompatible sweep range geometry for RMA11_KDP_x.H5"
                )
            )
            worker._handler = mock_handler

            work_unit = WorkUnit.create(
                image_id="RMA11_KDP_20260114T170040Z",
                source_uri="/data/radar/RMA11_KDP_20260114T170040Z.H5",
                data_source_id="radar_KDP",
                processor_id="radar",
                output_prefix="tiles/radar",
                bounds=config.get_bounds(),
                band_id="radar_KDP",
            )

            asyncio.run(
                worker._process_message_async(work_unit, 1, "tiles_radar_light_queue")
            )

            # Acked once (removed), and NOT retried / DLQ'd / re-discovered.
            mock_rabbitmq.ack.assert_called_once_with(1)
            mock_rabbitmq.publish.assert_not_called()
            mock_rabbitmq.publish_to_dlq.assert_not_called()
            # Deterministic skip: must NOT release progress (would only re-skip).
            mock_handler.release_progress.assert_not_called()

    def test_worker_releases_and_acks_on_transient_download_error(
        self, temp_settings_file, env_vars, mock_rabbitmq, mock_tracker
    ):
        """A 503 (rate limit) releases for re-discovery and acks — no instant requeue."""

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            worker = Worker(config, mock_rabbitmq, mock_tracker)

            mock_handler = MagicMock()
            mock_handler.handle = AsyncMock(
                side_effect=TransientDownloadError("S3 rate limit (503 Slow Down)")
            )
            worker._handler = mock_handler

            work_unit = WorkUnit.create(
                image_id="20260217T0000Z",
                source_uri="2026-02-17T00:00:00+00:00",
                data_source_id="ecmwf_tp_producer",
                processor_id="ecmwf_tp_grib_downloader",
                output_prefix="grib/models/ecmwf",
                bounds=config.get_bounds(),
                band_id="ecmwf_tp_producer",
            )

            asyncio.run(worker._process_message_async(work_unit, 1, "tiles_work_queue"))

            # Released for the next discovery tick, acked, and NOT republished.
            mock_handler.release_progress.assert_called_once_with(work_unit)
            mock_rabbitmq.ack.assert_called_once_with(1)
            mock_rabbitmq.publish.assert_not_called()
            mock_rabbitmq.publish_to_dlq.assert_not_called()


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

        with patch(
            "services.processing_steps.subprocess.run",
            side_effect=mock_subprocess_run,
        ):
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
