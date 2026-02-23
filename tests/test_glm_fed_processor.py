"""Tests for GLM FED processor zero-flash window handling."""

import os
import sys
import warnings
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest
import xarray as xr

import rioxarray  # noqa: F401  # ensures rio accessor is registered

from models.work_unit import WorkUnit
from processors.glm_fed_processor import GlmFedProcessor
from services.processing_steps import compute_flash_extent_density

BOUNDS = {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15}
RESOLUTION = 0.02


def _make_empty_glm_dataset():
    """Return a mock xr.Dataset whose flash arrays are empty."""
    mock_ds = MagicMock()
    mock_ds.__enter__ = MagicMock(return_value=mock_ds)
    mock_ds.__exit__ = MagicMock(return_value=False)
    mock_ds["flash_lat"].values = np.array([], dtype=np.float32)
    mock_ds["flash_lon"].values = np.array([], dtype=np.float32)
    return mock_ds


def _make_fed_data_array(bounds: dict, resolution: float = RESOLUTION) -> xr.DataArray:
    """Create a properly georeferenced all-NaN FED DataArray."""
    lon_bins = np.arange(bounds["minx"], bounds["maxx"] + resolution, resolution)
    lat_bins = np.arange(bounds["miny"], bounds["maxy"] + resolution, resolution)
    lon_centers = (lon_bins[:-1] + lon_bins[1:]) / 2
    lat_centers = (lat_bins[:-1] + lat_bins[1:]) / 2
    data = np.full((len(lat_centers), len(lon_centers)), np.nan, dtype=np.float64)
    fed = xr.DataArray(
        data,
        dims=["y", "x"],
        coords={"x": lon_centers, "y": lat_centers},
        name="Flash_Extent_Density",
    )
    fed.rio.write_crs("EPSG:4326", inplace=True)
    fed.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    return fed


class TestZeroFlashFedGrid:
    """compute_flash_extent_density behaves correctly when no flashes are present."""

    def test_zero_flash_returns_all_nan_dataarray(self, tmp_path):
        """Empty flash arrays yield an all-NaN DataArray named Flash_Extent_Density."""
        fake_files = [
            tmp_path
            / "OR_GLM-L2-LCFA_G19_s2026044120000_e2026044120200_c2026044120200.nc",
            tmp_path
            / "OR_GLM-L2-LCFA_G19_s2026044120200_e2026044120400_c2026044120400.nc",
        ]
        mock_ds = _make_empty_glm_dataset()

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            with warnings.catch_warnings():
                warnings.filterwarnings("error", category=RuntimeWarning)
                result = compute_flash_extent_density(fake_files, BOUNDS, RESOLUTION)

        assert isinstance(result, xr.DataArray)
        assert result.name == "Flash_Extent_Density"
        assert result.dims == ("y", "x")
        assert np.all(np.isnan(result.values))

    def test_zero_flash_shape_matches_bounds(self, tmp_path):
        """Grid shape matches the expected cell count derived from bounds + resolution."""
        fake_files = [
            tmp_path
            / "OR_GLM-L2-LCFA_G19_s2026044120000_e2026044120200_c2026044120200.nc",
        ]
        mock_ds = _make_empty_glm_dataset()

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            result = compute_flash_extent_density(fake_files, BOUNDS, RESOLUTION)

        lon_bins = np.arange(BOUNDS["minx"], BOUNDS["maxx"] + RESOLUTION, RESOLUTION)
        lat_bins = np.arange(BOUNDS["miny"], BOUNDS["maxy"] + RESOLUTION, RESOLUTION)
        expected_rows = len(lat_bins) - 1
        expected_cols = len(lon_bins) - 1
        assert result.shape == (expected_rows, expected_cols)


class TestZeroFlashWindowPipeline:
    """GlmFedProcessor completes and uploads tiles for a zero-flash window."""

    # Small bounds → tiny GeoTIFF, fast rio.to_raster call
    _SMALL_BOUNDS = {"minx": -80, "miny": -50, "maxx": -70, "maxy": -40}
    _SMALL_RESOLUTION = 1.0

    @pytest.mark.asyncio
    async def test_zero_flash_window_uploads_tiles(self, tmp_path):
        """upload_directory is called exactly once even when all flashes are absent."""
        config = MagicMock()
        config.TMP_DIR = str(tmp_path / "proc")

        mock_s3 = AsyncMock()
        mock_s3.upload_directory = AsyncMock()
        mock_s3.ensure_bucket_exists = AsyncMock()

        with patch(
            "processors.glm_fed_processor.create_s3_client", return_value=mock_s3
        ):
            processor = GlmFedProcessor(config)

        # A data directory with at least one matching GLM file name
        data_dir = tmp_path / "glm_window"
        data_dir.mkdir()
        (
            data_dir
            / "OR_GLM-L2-LCFA_G19_s2026044120000_e2026044120200_c2026044120200.nc"
        ).touch()

        # Fake tiles directory that run_gdal2tiles would return
        fake_tiles_dir = tmp_path / "fake_tiles"
        fake_tiles_dir.mkdir()

        work_unit = WorkUnit.create(
            image_id="20260213120000",
            source_uri="test://unused",
            data_source_id="goes19_glm",
            processor_id="glm_fed",
            output_prefix="glm_fed/tiles",
            bounds=self._SMALL_BOUNDS,
            band_id="glm_fed",
        )

        fed_data = _make_fed_data_array(self._SMALL_BOUNDS, self._SMALL_RESOLUTION)

        with patch(
            "processors.glm_fed_processor.compute_flash_extent_density",
            return_value=fed_data,
        ), patch(
            "processors.glm_fed_processor.run_gdal2tiles",
            return_value=fake_tiles_dir,
        ):
            await processor.process(str(data_dir), work_unit)

        mock_s3.upload_directory.assert_called_once()
