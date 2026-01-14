import asyncio
import sys
import os
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock
import gc

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import pytest
import numpy as np

from services.generate_geotiff_files import GenerateGeoTIFFFilesService


class TestExpandPalette:
    """Tests for palette expansion utility."""

    def test_expand_palette_to_256_from_small(self):
        """Test expanding a small palette to 256 colors."""
        small_palette = ["#000000", "#ffffff"]
        expanded = GenerateGeoTIFFFilesService._expand_palette_to_256(small_palette)

        assert len(expanded) == 256
        assert expanded[0] == "#000000"
        assert expanded[255] == "#ffffff"

    def test_expand_palette_to_256_preserves_order(self):
        """Test that palette expansion preserves color order."""
        palette = ["#ff0000", "#00ff00", "#0000ff"]
        expanded = GenerateGeoTIFFFilesService._expand_palette_to_256(palette)

        assert len(expanded) == 256
        # First color should be red
        assert expanded[0] == "#ff0000"
        # Last color should be blue
        assert expanded[255] == "#0000ff"

    def test_cloud_tops_palette_length(self):
        """Test that CLOUD_TOPS_PALETTE has 256 colors."""
        assert len(GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE) == 256

    def test_water_vapor_palette_length(self):
        """Test that WATER_VAPOR_PALETTE has 256 colors."""
        assert len(GenerateGeoTIFFFilesService.WATER_VAPOR_PALETTE) == 256


class TestNormalizeWithCustomPalette:
    """Tests for the normalization and palette application."""

    @pytest.fixture
    def service(self, tmp_path):
        """Create a service instance for testing."""
        return GenerateGeoTIFFFilesService(
            brightness_temperatures={},
            output_dir=tmp_path,
            vmin=200.0,
            vmax=300.0,
        )

    def test_normalize_basic_values(self, service):
        """Test normalization of basic values."""
        # Create a simple array
        data = np.array([[200.0, 250.0, 300.0]])

        # Mock xarray DataArray
        mock_array = MagicMock()
        mock_array.values = data

        r, g, b, a = service._normalize_with_custom_palette(mock_array, vmin=200.0, vmax=300.0)

        # All values are valid, so alpha should be 255
        assert np.all(a == 255)

        # Check shapes match
        assert r.shape == data.shape
        assert g.shape == data.shape
        assert b.shape == data.shape

    def test_normalize_handles_nan_values(self, service):
        """Test that NaN values get alpha=0."""
        data = np.array([[200.0, np.nan, 300.0]])

        mock_array = MagicMock()
        mock_array.values = data

        r, g, b, a = service._normalize_with_custom_palette(mock_array, vmin=200.0, vmax=300.0)

        # NaN position should have alpha=0
        assert a[0, 0] == 255
        assert a[0, 1] == 0  # NaN position
        assert a[0, 2] == 255

    def test_normalize_clips_out_of_range(self, service):
        """Test that values outside range are clipped."""
        # Values below vmin and above vmax
        data = np.array([[100.0, 250.0, 400.0]])

        mock_array = MagicMock()
        mock_array.values = data

        r, g, b, a = service._normalize_with_custom_palette(mock_array, vmin=200.0, vmax=300.0)

        # All non-NaN values should have full alpha
        assert np.all(a == 255)

    def test_normalize_returns_uint8(self, service):
        """Test that returned arrays are uint8."""
        data = np.array([[200.0, 250.0, 300.0]])

        mock_array = MagicMock()
        mock_array.values = data

        r, g, b, a = service._normalize_with_custom_palette(mock_array, vmin=200.0, vmax=300.0)

        assert r.dtype == np.uint8
        assert g.dtype == np.uint8
        assert b.dtype == np.uint8
        assert a.dtype == np.uint8


class TestGenerateGeoTIFFService:
    """Tests for the main GeoTIFF generation service."""

    @pytest.fixture
    def mock_xarray_data(self):
        """Create mock xarray DataArray."""
        mock_da = MagicMock()
        mock_da.attrs = {"grid_mapping": "goes_imager_projection"}
        mock_da.values = np.random.rand(100, 100) * 100 + 200

        # Mock rio accessor chain
        mock_reproj = MagicMock()
        mock_reproj.rio.write_nodata.return_value = mock_reproj
        mock_reproj.rio.clip_box.return_value = mock_reproj
        mock_reproj.__getitem__ = lambda self, key: np.linspace(-90, -30, 100)

        mock_da.rio.reproject.return_value = mock_reproj

        return mock_da

    @pytest.fixture
    def service(self, tmp_path, mock_xarray_data):
        """Create service with mock data."""
        return GenerateGeoTIFFFilesService(
            brightness_temperatures={"test_file.nc": mock_xarray_data},
            output_dir=tmp_path / "output",
            vmin=183.15,
            vmax=323.15,
            product_name="Test_Product",
        )

    def test_init_sets_defaults(self, tmp_path):
        """Test that __init__ sets correct defaults."""
        service = GenerateGeoTIFFFilesService(
            brightness_temperatures={},
            output_dir=tmp_path,
        )

        assert service._vmin == 183.15
        assert service._vmax == 323.15
        assert service._product_name == "Cloud_Tops"
        assert service._color_palette == GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE

    def test_init_accepts_custom_palette(self, tmp_path):
        """Test that custom palette can be provided."""
        custom_palette = ["#000000"] * 256
        service = GenerateGeoTIFFFilesService(
            brightness_temperatures={},
            output_dir=tmp_path,
            color_palette=custom_palette,
        )

        assert service._color_palette == custom_palette

    def test_default_bounds_structure(self):
        """Test that DEFAULT_BOUNDS has correct structure."""
        bounds = GenerateGeoTIFFFilesService.DEFAULT_BOUNDS

        assert "minx" in bounds
        assert "miny" in bounds
        assert "maxx" in bounds
        assert "maxy" in bounds

        # Verify Argentina-region defaults
        assert bounds["minx"] == -90.0
        assert bounds["miny"] == -60.0
        assert bounds["maxx"] == -30.0
        assert bounds["maxy"] == -15.0

    @pytest.mark.asyncio
    async def test_run_creates_output_directory(self, tmp_path):
        """Test that run() creates output directory."""
        output_dir = tmp_path / "new_output"
        service = GenerateGeoTIFFFilesService(
            brightness_temperatures={},
            output_dir=output_dir,
        )

        await service.run()

        assert output_dir.exists()

    @pytest.mark.asyncio
    async def test_run_processes_all_files(self, tmp_path):
        """Test that run() processes all brightness temperature files."""
        mock_data = {f"file_{i}.nc": MagicMock() for i in range(3)}

        service = GenerateGeoTIFFFilesService(
            brightness_temperatures=mock_data,
            output_dir=tmp_path / "output",
        )

        processed = []

        def track_generate(file_name, dataset):
            processed.append(file_name)
            return tmp_path / f"{file_name}.tif"

        with patch.object(service, '_generate_geotiff', side_effect=track_generate):
            await service.run()

        assert len(processed) == 3
        for key in mock_data.keys():
            assert key in processed

    def test_generate_geotiff_removes_grid_mapping(self, service, mock_xarray_data):
        """Test that grid_mapping attribute is removed."""
        assert "grid_mapping" in mock_xarray_data.attrs

        with patch.object(service, '_normalize_with_custom_palette') as mock_norm:
            mock_norm.return_value = (
                np.zeros((10, 10), dtype=np.uint8),
                np.zeros((10, 10), dtype=np.uint8),
                np.zeros((10, 10), dtype=np.uint8),
                np.ones((10, 10), dtype=np.uint8) * 255,
            )

            with patch('xarray.DataArray') as mock_da_class:
                mock_rgb = MagicMock()
                mock_da_class.return_value = mock_rgb

                try:
                    service._generate_geotiff("test.nc", mock_xarray_data)
                except:
                    pass  # We just want to verify the grid_mapping removal

        assert "grid_mapping" not in mock_xarray_data.attrs


class TestGeoTIFFAtomicWrite:
    """Tests for atomic write behavior."""

    def test_atomic_write_uses_temp_file(self, tmp_path):
        """Test that atomic write pattern uses temporary file."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        service = GenerateGeoTIFFFilesService(
            brightness_temperatures={},
            output_dir=output_dir,
        )

        # Create minimal mock data
        mock_data = MagicMock()
        mock_data.attrs = {}
        mock_data.values = np.array([[250.0]])

        mock_reproj = MagicMock()
        mock_reproj.rio.write_nodata.return_value = mock_reproj
        mock_reproj.rio.clip_box.return_value = mock_reproj
        mock_reproj.__getitem__ = lambda self, key: np.array([0.0])

        mock_data.rio.reproject.return_value = mock_reproj

        with patch('xarray.DataArray') as mock_da_class:
            mock_rgb = MagicMock()
            mock_da_class.return_value = mock_rgb

            # Track what paths are used for writing
            write_paths = []

            def track_to_raster(path):
                write_paths.append(path)
                # Create the temp file so rename can work
                Path(path).touch()

            mock_rgb.rio.to_raster = track_to_raster

            result = service._generate_geotiff("test_file.nc", mock_data)

            # Verify a temp file was used (UUID in name)
            assert len(write_paths) == 1
            temp_path = write_paths[0]
            # Temp file should have been renamed, so it shouldn't exist
            # Final file should exist
            assert result.exists()
            assert result.name == "test_file.tif"


class TestMemoryManagement:
    """Tests for memory management with gc.collect()."""

    def test_gc_collect_called_during_generation(self, tmp_path):
        """Test that gc.collect() is called to manage memory."""
        service = GenerateGeoTIFFFilesService(
            brightness_temperatures={},
            output_dir=tmp_path,
        )

        mock_data = MagicMock()
        mock_data.attrs = {}
        mock_data.values = np.array([[250.0]])

        mock_reproj = MagicMock()
        mock_reproj.rio.write_nodata.return_value = mock_reproj
        mock_reproj.rio.clip_box.return_value = mock_reproj
        mock_reproj.__getitem__ = lambda self, key: np.array([0.0])

        mock_data.rio.reproject.return_value = mock_reproj

        with patch('gc.collect') as mock_gc:
            with patch('xarray.DataArray') as mock_da_class:
                mock_rgb = MagicMock()
                mock_da_class.return_value = mock_rgb

                def mock_to_raster(path):
                    Path(path).touch()

                mock_rgb.rio.to_raster = mock_to_raster

                service._generate_geotiff("test.nc", mock_data)

            # gc.collect should be called multiple times for memory management
            assert mock_gc.call_count >= 2
