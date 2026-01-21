"""
Tests for the PROCESS stage handler and GoesProcessor.
"""

import sys
import os
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from worker.stage_handlers.process_handler import ProcessHandler
from processors.goes_processor import GoesProcessor
from models.work_unit import WorkUnit, Stage, WorkUnitPaths
from config import Config


@pytest.fixture
def mock_config():
    config = MagicMock(spec=Config)
    config.TMP_DIR = "/tmp/test"
    config.S3_TILES_DATA_BUCKET_NAME = "test-bucket"
    config.S3_TILES_DATA_ENDPOINT = "mock:9000"
    config.S3_TILES_DATA_RW_ACCESS_KEY = "user"
    config.S3_TILES_DATA_RW_SECRET_KEY = "pass"
    config.S3_TILES_DATA_SECURE = False
    return config


@pytest.fixture
def mock_tracker():
    return MagicMock()


class TestProcessHandler:
    """Tests for the ProcessHandler."""

    @pytest.mark.asyncio
    async def test_handle_dispatches_to_correct_processor(
        self, mock_config, mock_tracker
    ):
        """Test that handle() calls the correct processor for the band."""
        handler = ProcessHandler(mock_config, mock_tracker)

        # Mock the processors
        mock_b13 = AsyncMock()
        mock_b9 = AsyncMock()
        handler._processors = {
            "band_13": mock_b13,
            "band_9": mock_b9,
        }

        # Create work unit
        work_unit = WorkUnit(
            work_unit_id="1",
            image_id="img1",
            band_id="band_13",
            stage=Stage.PROCESS,
            paths=WorkUnitPaths(
                source_s3_uri="s3://src/img1.nc", downloaded_file="/tmp/img1.nc"
            ),
            bounds={},
            processor_type="band_13",
        )

        # Execute
        result = await handler.handle(work_unit)

        # Verify
        mock_b13.process.assert_awaited_once_with("/tmp/img1.nc", work_unit)
        mock_b9.process.assert_not_awaited()

        # Verify tracker updated
        mock_tracker.mark_completed.assert_called_once_with("img1", "band_13")

        # Verify result is passed back
        assert result is work_unit

    @pytest.mark.asyncio
    async def test_handle_cleanup_downloaded_file(self, mock_config, mock_tracker):
        """Test that downloaded file is cleaned up after processing."""
        handler = ProcessHandler(mock_config, mock_tracker)

        mock_processor = AsyncMock()
        handler._processors = {"band_13": mock_processor}

        # Create dummy file
        downloaded_file = Path("/tmp/test_download.nc")
        # We mock _cleanup_file to check it's called, avoiding actual file IO issues in generic test env

        with patch.object(handler, "_cleanup_file") as mock_cleanup:
            work_unit = WorkUnit(
                work_unit_id="1",
                image_id="img1",
                band_id="band_13",
                stage=Stage.PROCESS,
                paths=WorkUnitPaths(
                    source_s3_uri="s3://src/img1.nc",
                    downloaded_file=str(downloaded_file),
                ),
                bounds={},
                processor_type="band_13",
            )

            await handler.handle(work_unit)

            mock_cleanup.assert_called_once_with(str(downloaded_file))


class TestGoesProcessor:
    """Tests for the GoesProcessor logic."""

    @pytest.mark.asyncio
    async def test_process_flow(self, mock_config):
        """Test the sequence of steps in process()."""

        # Patch S3Client to avoid connection attempts in init
        with patch("processors.goes_processor.S3Client"):
            processor = GoesProcessor(mock_config)

            # Mock all the internal steps
            processor._apply_georeferencing = MagicMock()
            processor._compute_brightness_temperature = MagicMock()
            processor._generate_geotiff = MagicMock(return_value=Path("/tmp/out.tif"))
            processor._generate_tiles = MagicMock(return_value=Path("/tmp/tiles"))
            processor._minio_client.upload_directory = AsyncMock()
            processor._minio_client.ensure_bucket_exists = AsyncMock()
            processor._enforce_retention_policy = AsyncMock()
            processor._cleanup_file = MagicMock()
            processor._cleanup_directory = MagicMock()

            # Create work unit
            work_unit = WorkUnit(
                work_unit_id="1",
                image_id="img1",
                band_id="band_13",
                stage=Stage.PROCESS,
                paths=WorkUnitPaths(
                    source_s3_uri="s3://src/img1.nc", downloaded_file="/tmp/img1.nc"
                ),
                bounds={"minx": 0, "miny": 0, "maxx": 10, "maxy": 10},
                processor_type="band_13",
            )

            # Mock existence of input file
            with patch("pathlib.Path.exists", return_value=True):
                await processor.process("/tmp/img1.nc", work_unit)

            # Verify order of operations
            processor._apply_georeferencing.assert_called_once()
            processor._compute_brightness_temperature.assert_called_once()
            processor._generate_geotiff.assert_called_once()
            processor._generate_tiles.assert_called_once()
            processor._minio_client.upload_directory.assert_awaited_once()
            processor._enforce_retention_policy.assert_awaited_once()

            # Helper: verify s3 path populated
            assert work_unit.paths.s3_tileset_prefix is not None
            assert "tiles" in work_unit.paths.s3_tileset_prefix
