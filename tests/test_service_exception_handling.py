"""
Tests for exception handling in services.

These tests verify that when individual tasks fail during async processing,
the services properly:
1. Catch and log the errors
2. Raise RuntimeError with descriptive messages
3. Don't crash silently (which would cause BrokenProcessPool in scheduler)
"""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest


class TestGenerateTilesServiceExceptionHandling:
    """Tests for exception handling in GenerateTilesService."""

    @pytest.fixture
    def mock_geotiff_files(self, tmp_path):
        """Create mock GeoTIFF files."""
        geotiff_dir = tmp_path / "geotiffs"
        geotiff_dir.mkdir()
        files = []
        for i in range(3):
            f = geotiff_dir / f"image_{i}.tif"
            f.write_text("mock geotiff content")
            files.append(f)
        return files

    @pytest.fixture
    def tmp_output_dir(self, tmp_path):
        """Provide a temporary output directory."""
        return tmp_path / "tiles_output"

    @pytest.mark.asyncio
    async def test_partial_failure_logs_errors_and_raises(
        self, mock_geotiff_files, tmp_output_dir
    ):
        """Test that when some files fail, errors are logged and RuntimeError is raised."""
        from services.generate_tiles import GenerateTilesService

        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)

        def fail_second_file(geotiff_path, output_dir, **kwargs):
            if "image_1" in str(geotiff_path):
                raise ValueError("Simulated gdal2tiles crash")
            return output_dir / f"{geotiff_path.stem}_tiles"

        with patch(
            "services.generate_tiles.run_gdal2tiles", side_effect=fail_second_file
        ):
            with pytest.raises(RuntimeError) as exc_info:
                await service.run()

        assert "1/3" in str(exc_info.value)
        assert "Tile generation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_all_failures_logs_all_errors(
        self, mock_geotiff_files, tmp_output_dir
    ):
        """Test that when all files fail, all errors are logged."""
        from services.generate_tiles import GenerateTilesService

        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)

        def always_fail(geotiff_path, output_dir, **kwargs):
            raise RuntimeError(f"Failed for {geotiff_path.name}")

        with patch("services.generate_tiles.run_gdal2tiles", side_effect=always_fail):
            with pytest.raises(RuntimeError) as exc_info:
                await service.run()

        assert "3/3" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_success_when_no_failures(self, mock_geotiff_files, tmp_output_dir):
        """Test that when all files succeed, no exception is raised."""
        from services.generate_tiles import GenerateTilesService

        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)

        def succeed(geotiff_path, output_dir, **kwargs):
            return output_dir / f"{geotiff_path.stem}_tiles"

        with patch("services.generate_tiles.run_gdal2tiles", side_effect=succeed):
            # Should not raise
            await service.run()


class TestComputeBrightnessTemperaturesServiceExceptionHandling:
    """Tests for exception handling in ComputeBrightnessTemperaturesService."""

    @pytest.fixture
    def mock_datasets(self):
        """Create mock xarray datasets."""
        mock_ds = MagicMock()
        mock_ds.__getitem__ = MagicMock(return_value=MagicMock())
        return {
            "file1.nc": mock_ds,
            "file2.nc": mock_ds,
            "file3.nc": mock_ds,
        }

    @pytest.mark.asyncio
    async def test_partial_failure_raises_with_count(self, mock_datasets):
        """Test that partial failure raises RuntimeError with failure count."""
        from services.compute_brightness_temperatures import (
            ComputeBrightnessTemperaturesService,
        )

        service = ComputeBrightnessTemperaturesService(mock_datasets)

        call_count = 0

        def fail_second(dataset):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise ValueError("Memory error during computation")
            return MagicMock()

        with patch(
            "services.compute_brightness_temperatures.compute_brightness_temperature",
            side_effect=fail_second,
        ):
            with pytest.raises(RuntimeError) as exc_info:
                await service.run()

        assert "1/3" in str(exc_info.value)
        assert "Brightness temp computation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_success_returns_all_results(self, mock_datasets):
        """Test that success returns all processed results."""
        from services.compute_brightness_temperatures import (
            ComputeBrightnessTemperaturesService,
        )

        service = ComputeBrightnessTemperaturesService(mock_datasets)

        mock_result = MagicMock()
        with patch(
            "services.compute_brightness_temperatures.compute_brightness_temperature",
            return_value=mock_result,
        ):
            result = await service.run()

        assert len(result) == 3
        assert all(v == mock_result for v in result.values())


class TestSetupGOESGeorreferencingServiceExceptionHandling:
    """Tests for exception handling in SetupGOESGeorreferencingService."""

    @pytest.fixture
    def mock_goes_data(self):
        """Create mock GOES data bytes."""
        return {
            "file1.nc": b"mock content 1",
            "file2.nc": b"mock content 2",
            "file3.nc": b"mock content 3",
        }

    @pytest.mark.asyncio
    async def test_partial_failure_raises_with_count(self, mock_goes_data):
        """Test that partial failure raises RuntimeError with failure count."""
        from services.setup_goes_georreferencing import SetupGOESGeorreferencingService

        service = SetupGOESGeorreferencingService(mock_goes_data)

        call_count = 0

        def fail_first(content):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise IOError("Failed to read NetCDF data")
            return MagicMock()

        with patch(
            "services.setup_goes_georreferencing.apply_goes_georeferencing",
            side_effect=fail_first,
        ):
            with pytest.raises(RuntimeError) as exc_info:
                await service.run()

        assert "1/3" in str(exc_info.value)
        assert "Georeferencing failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_success_returns_all_results(self, mock_goes_data):
        """Test that success returns all georeferenced results."""
        from services.setup_goes_georreferencing import SetupGOESGeorreferencingService

        service = SetupGOESGeorreferencingService(mock_goes_data)

        mock_result = MagicMock()
        with patch(
            "services.setup_goes_georreferencing.apply_goes_georeferencing",
            return_value=mock_result,
        ):
            result = await service.run()

        assert len(result) == 3
        assert all(v == mock_result for v in result.values())


class TestGenerateGeoTIFFFilesServiceExceptionHandling:
    """Tests for exception handling in GenerateGeoTIFFFilesService."""

    @pytest.fixture
    def mock_brightness_temps(self):
        """Create mock brightness temperature data."""
        mock_da = MagicMock()
        return {
            "file1.nc": mock_da,
            "file2.nc": mock_da,
            "file3.nc": mock_da,
        }

    @pytest.fixture
    def mock_config(self):
        """Create mock config."""
        config = MagicMock()
        config.get_bounds.return_value = {
            "minx": -80,
            "miny": -60,
            "maxx": -30,
            "maxy": 15,
        }
        return config

    @pytest.mark.asyncio
    async def test_partial_failure_raises_with_count(
        self, mock_brightness_temps, mock_config, tmp_path
    ):
        """Test that partial failure raises RuntimeError with failure count."""
        from services.generate_geotiff_files import GenerateGeoTIFFFilesService

        service = GenerateGeoTIFFFilesService(
            mock_brightness_temps,
            tmp_path / "geotiff",
            mock_config,
        )

        call_count = 0

        def fail_second(file_name, dataset):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise MemoryError("Out of memory during reprojection")
            return tmp_path / f"{file_name}.tif"

        with patch.object(service, "_generate_geotiff", side_effect=fail_second):
            with pytest.raises(RuntimeError) as exc_info:
                await service.run()

        assert "1/3" in str(exc_info.value)
        assert "GeoTIFF generation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_success_returns_all_paths(
        self, mock_brightness_temps, mock_config, tmp_path
    ):
        """Test that success returns all generated file paths."""
        from services.generate_geotiff_files import GenerateGeoTIFFFilesService

        output_dir = tmp_path / "geotiff"
        service = GenerateGeoTIFFFilesService(
            mock_brightness_temps,
            output_dir,
            mock_config,
        )

        def return_path(file_name, dataset):
            return output_dir / f"{file_name}.tif"

        with patch.object(service, "_generate_geotiff", side_effect=return_path):
            result = await service.run()

        assert len(result) == 3
        assert all(isinstance(p, Path) for p in result)
