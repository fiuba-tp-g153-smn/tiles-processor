"""
Tests for the GoesProcessor.
"""

import sys
import os
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from processors.goes_processor import GoesProcessor
from models.work_unit import WorkUnit
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


class TestGoesProcessor:
    """Tests for the GoesProcessor logic."""

    @pytest.mark.asyncio
    async def test_process_flow(self, mock_config):
        """Test the sequence of steps in process()."""

        with patch("processors.goes_processor.create_s3_client"):
            processor = GoesProcessor(mock_config)

            # Mock all the internal steps (instance methods for polymorphism)
            processor._apply_georeferencing = MagicMock()
            processor._compute_brightness_temperature = MagicMock()
            processor._reproject_and_clip = MagicMock(return_value=MagicMock())
            processor._colorize_and_save = MagicMock(return_value=Path("/tmp/out.tif"))
            processor._generate_tiles = MagicMock(return_value=Path("/tmp/tiles"))
            processor._s3_client.upload_directory = AsyncMock()
            processor._s3_client.ensure_bucket_exists = AsyncMock()
            processor._s3_client.upload_file = AsyncMock(return_value=True)
            processor._cleanup_file = MagicMock()
            processor._cleanup_directory = MagicMock()

            with patch("processors.goes_processor.save_as_cog", return_value=Path("/tmp/out_cog.tif")):
                # Create work unit using new format
                work_unit = WorkUnit.create(
                    image_id="img1.nc",
                    source_uri="s3://noaa-goes19/path/to/img1.nc",
                    data_source_id="goes19_abi_band_13",
                    processor_id="goes_band_13",
                    output_prefix="tiles/band_13",
                    bounds={"minx": 0, "miny": 0, "maxx": 10, "maxy": 10},
                    band_id="band_13",
                )

                # Mock existence of input file
                with patch("pathlib.Path.exists", return_value=True):
                    await processor.process("/tmp/img1.nc", work_unit)

            # Verify order of operations
            processor._apply_georeferencing.assert_called_once()
            processor._compute_brightness_temperature.assert_called_once()
            processor._reproject_and_clip.assert_called_once()
            processor._colorize_and_save.assert_called_once()
            processor._generate_tiles.assert_called_once()
            processor._s3_client.upload_directory.assert_awaited_once()
            processor._s3_client.upload_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_process_continues_when_cog_upload_fails(self, mock_config):
        """COG upload failures should be logged and not abort GOES processing."""

        with patch("processors.goes_processor.create_s3_client"):
            processor = GoesProcessor(mock_config)

            processor._apply_georeferencing = MagicMock()
            processor._compute_brightness_temperature = MagicMock()
            processor._reproject_and_clip = MagicMock(return_value=MagicMock())
            processor._colorize_and_save = MagicMock(return_value=Path("/tmp/out.tif"))
            processor._generate_tiles = MagicMock(return_value=Path("/tmp/tiles"))
            processor._s3_client.upload_directory = AsyncMock()
            processor._s3_client.ensure_bucket_exists = AsyncMock()
            processor._s3_client.upload_file = AsyncMock(return_value=False)
            processor._cleanup_file = MagicMock()
            processor._cleanup_directory = MagicMock()

            with patch(
                "processors.goes_processor.save_as_cog",
                return_value=Path("/tmp/out_cog.tif"),
            ):
                work_unit = WorkUnit.create(
                    image_id="img1.nc",
                    source_uri="s3://noaa-goes19/path/to/img1.nc",
                    data_source_id="goes19_abi_band_13",
                    processor_id="goes_band_13",
                    output_prefix="tiles/band_13",
                    bounds={"minx": 0, "miny": 0, "maxx": 10, "maxy": 10},
                    band_id="band_13",
                )

                with patch("pathlib.Path.exists", return_value=True):
                    await processor.process("/tmp/img1.nc", work_unit)

            processor._s3_client.upload_directory.assert_awaited_once()
            processor._s3_client.upload_file.assert_awaited_once()
