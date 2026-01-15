import os
import sys
import pytest
import asyncio
from unittest.mock import MagicMock, patch
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from services.compute_brightness_temperatures import (
    ComputeBrightnessTemperaturesService,
)
from services.setup_goes_georreferencing import SetupGOESGeorreferencingService
from services.generate_geotiff_files import GenerateGeoTIFFFilesService


@pytest.mark.asyncio
async def test_compute_brightness_temperatures_concurrency_limit():
    """Verify that ComputeBrightnessTemperaturesService respects concurrency limit."""
    mock_datasets = {f"file_{i}": MagicMock() for i in range(10)}
    max_concurrency = 4

    current_concurrent_tasks = 0
    max_observed_concurrency = 0

    async def fast_mock_computation(func, dataset):
        nonlocal current_concurrent_tasks, max_observed_concurrency
        current_concurrent_tasks += 1
        max_observed_concurrency = max(
            max_observed_concurrency, current_concurrent_tasks
        )
        await asyncio.sleep(0.01)  # Small delay to allow overlap
        current_concurrent_tasks -= 1
        return MagicMock()

    service = ComputeBrightnessTemperaturesService(
        mock_datasets, max_concurrency=max_concurrency
    )

    with patch(
        "asyncio.to_thread", side_effect=fast_mock_computation
    ) as mock_to_thread:
        await service.run()

    assert max_observed_concurrency <= max_concurrency
    assert mock_to_thread.call_count == 10


@pytest.mark.asyncio
async def test_setup_goes_georreferencing_concurrency_limit():
    """Verify that SetupGOESGeorreferencingService respects concurrency limit."""
    mock_data = {f"file_{i}": b"" for i in range(8)}
    max_concurrency = 2

    current_concurrent_tasks = 0
    max_observed_concurrency = 0

    async def fast_mock_georeferencing(func, content):
        nonlocal current_concurrent_tasks, max_observed_concurrency
        current_concurrent_tasks += 1
        max_observed_concurrency = max(
            max_observed_concurrency, current_concurrent_tasks
        )
        await asyncio.sleep(0.01)
        current_concurrent_tasks -= 1
        return MagicMock()

    service = SetupGOESGeorreferencingService(
        mock_data, max_concurrency=max_concurrency
    )

    with patch(
        "asyncio.to_thread", side_effect=fast_mock_georeferencing
    ) as mock_to_thread:
        await service.run()

    assert max_observed_concurrency <= max_concurrency
    assert mock_to_thread.call_count == 8


@pytest.mark.asyncio
async def test_generate_geotiff_files_concurrency_limit(tmp_path):
    """Verify that GenerateGeoTIFFFilesService respects concurrency limit."""
    mock_data = {f"file_{i}": MagicMock() for i in range(6)}
    max_concurrency = 3
    mock_config = MagicMock()

    current_concurrent_tasks = 0
    max_observed_concurrency = 0

    async def fast_mock_generation(func, file_name, dataset):
        nonlocal current_concurrent_tasks, max_observed_concurrency
        current_concurrent_tasks += 1
        max_observed_concurrency = max(
            max_observed_concurrency, current_concurrent_tasks
        )
        await asyncio.sleep(0.01)
        current_concurrent_tasks -= 1
        return tmp_path / f"{file_name}.tif"

    service = GenerateGeoTIFFFilesService(
        mock_data,
        tmp_path,
        mock_config,
        product_name="test",
        max_concurrency=max_concurrency,
    )

    with patch("asyncio.to_thread", side_effect=fast_mock_generation) as mock_to_thread:
        await service.run()

    assert max_observed_concurrency <= max_concurrency
    assert mock_to_thread.call_count == 6
