import pytest
from unittest.mock import MagicMock, patch, call
import asyncio
from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from jobs.process_band_13_job import ProcessBand13Job


@pytest.mark.asyncio
async def test_process_band_13_batches_execution(tmp_path):
    """Test that the processing pipeline executes in batches."""

    # Setup mocks
    mock_files = {f"file_{i}": MagicMock() for i in range(10)}  # 10 files
    mock_dirs = {"geotiff": tmp_path / "geotiff", "tiles": tmp_path / "tiles"}

    # Mock services
    with patch(
        "jobs.process_band_13_job.SetupGOESGeorreferencingService"
    ) as mock_geo_service, patch(
        "jobs.process_band_13_job.ComputeBrightnessTemperaturesService"
    ) as mock_bt_service, patch(
        "jobs.process_band_13_job.GenerateGeoTIFFFilesService"
    ) as mock_geotiff_service, patch(
        "jobs.process_band_13_job.GenerateTilesService"
    ) as mock_tiles_service, patch(
        "jobs.process_band_13_job.Config"
    ) as mock_config:

        # Configure mocks to return awaitables via side_effect (fresh coroutine per call)
        async def mock_geo_run():
            return {"geo": "data"}

        mock_geo_instance = mock_geo_service.return_value
        mock_geo_instance.run.side_effect = mock_geo_run

        async def mock_bt_run():
            return {"bt": "data"}

        mock_bt_instance = mock_bt_service.return_value
        mock_bt_instance.run.side_effect = mock_bt_run

        async def mock_geotiff_run():
            return [Path(f"out_{i}.tif") for i in range(4)]

        mock_geotiff_instance = mock_geotiff_service.return_value
        mock_geotiff_instance.run.side_effect = mock_geotiff_run

        async def mock_tiles_run():
            pass

        mock_tiles_instance = mock_tiles_service.return_value
        mock_tiles_instance.run.side_effect = mock_tiles_run

        job = ProcessBand13Job()
        # Bypass __init__ config loading if needed, but handled by mock_config patch

        # Execute
        await job._run_processing_pipeline(mock_files, mock_dirs)

        # Verification
        # 10 files, batch size 4 -> 3 batches (4, 4, 2)
        assert mock_geo_service.call_count == 3
        assert mock_bt_service.call_count == 3
        assert mock_geotiff_service.call_count == 3

        # Check batch sizes passed to SetupGOESGeorreferencingService
        calls = mock_geo_service.call_args_list
        assert len(calls[0].args[0]) == 4
        assert len(calls[1].args[0]) == 4
        assert len(calls[2].args[0]) == 2

        # Verify tiles service called once with accumulated results
        assert mock_tiles_service.call_count == 1
        # 3 batches * 4 (mocked return) = 12 files.
        # Wait, the last batch had 2 files input, but our mock returns 4 paths regardless of input size in this simplified test.
        # That's fine, we just verify it received the accumulated list.
        # 3 calls * 4 paths = 12 paths
        tiles_call_args = mock_tiles_service.call_args[0][0]
        assert len(tiles_call_args) == 12
