"""Tests for GLM FED/TOE processor zero-flash window handling."""

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
from services.processing_steps import compute_flash_extent_density, compute_glm_grids

BOUNDS = {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15}
RESOLUTION = 0.02


def _make_empty_glm_dataset():
    """Return a mock xr.Dataset whose flash arrays are empty.

    Uses side_effect on __getitem__ so each key gets its own .values,
    avoiding the MagicMock pitfall where a single return_value is shared.
    """
    _data = {
        "flash_lat": np.array([], dtype=np.float32),
        "flash_lon": np.array([], dtype=np.float32),
        "flash_energy": np.array([], dtype=np.float32),
        "flash_area": np.array([], dtype=np.float32),
    }
    mock_ds = MagicMock()
    mock_ds.__enter__ = MagicMock(return_value=mock_ds)
    mock_ds.__exit__ = MagicMock(return_value=False)

    def _getitem(key):
        child = MagicMock()
        child.values = _data[key]
        return child

    mock_ds.__getitem__.side_effect = _getitem
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
    """compute_glm_grids / compute_flash_extent_density behave correctly when no flashes present."""

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

    def test_compute_glm_grids_returns_both_arrays(self, tmp_path):
        """compute_glm_grids returns (fed, toe, mfa) tuple, all all-NaN when no flashes."""
        fake_files = [
            tmp_path
            / "OR_GLM-L2-LCFA_G19_s2026044120000_e2026044120200_c2026044120200.nc",
        ]
        mock_ds = _make_empty_glm_dataset()

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            fed, toe, mfa = compute_glm_grids(fake_files, BOUNDS, RESOLUTION)

        assert fed.name == "Flash_Extent_Density"
        assert toe.name == "Total_Optical_Energy"
        assert mfa.name == "Minimum_Flash_Area"
        assert fed.shape == toe.shape == mfa.shape
        assert np.all(np.isnan(fed.values))
        assert np.all(np.isnan(toe.values))
        assert np.all(np.isnan(mfa.values))


class TestZeroFlashWindowPipeline:
    """GlmFedProcessor completes and uploads tiles for a zero-flash window."""

    # Small bounds → tiny GeoTIFF, fast rio.to_raster call
    _SMALL_BOUNDS = {"minx": -80, "miny": -50, "maxx": -70, "maxy": -40}
    _SMALL_RESOLUTION = 1.0

    @pytest.mark.asyncio
    async def test_zero_flash_window_uploads_tiles(self, tmp_path):
        """upload_directory is called exactly once (FED only) when TOE and MFA are disabled."""
        config = MagicMock()
        config.TMP_DIR = str(tmp_path / "proc")
        config.ENABLE_GLM_TOE = False
        config.ENABLE_GLM_MFA = False

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
            "processors.glm_fed_processor.compute_glm_grids",
            return_value=(fed_data, fed_data, fed_data),
        ), patch(
            "processors.glm_fed_processor.run_gdal2tiles",
            return_value=fake_tiles_dir,
        ):
            await processor.process(str(data_dir), work_unit)

        mock_s3.upload_directory.assert_called_once()

    @pytest.mark.asyncio
    async def test_toe_enabled_uploads_twice(self, tmp_path):
        """upload_directory is called twice (FED + TOE) when ENABLE_GLM_TOE is True."""
        config = MagicMock()
        config.TMP_DIR = str(tmp_path / "proc")
        config.ENABLE_GLM_TOE = True
        config.ENABLE_GLM_MFA = False

        mock_s3 = AsyncMock()
        mock_s3.upload_directory = AsyncMock()
        mock_s3.ensure_bucket_exists = AsyncMock()

        with patch(
            "processors.glm_fed_processor.create_s3_client", return_value=mock_s3
        ):
            processor = GlmFedProcessor(config)

        data_dir = tmp_path / "glm_window"
        data_dir.mkdir()
        (
            data_dir
            / "OR_GLM-L2-LCFA_G19_s2026044120000_e2026044120200_c2026044120200.nc"
        ).touch()

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
            "processors.glm_fed_processor.compute_glm_grids",
            return_value=(fed_data, fed_data, fed_data),
        ), patch(
            "processors.glm_fed_processor.run_gdal2tiles",
            return_value=fake_tiles_dir,
        ):
            await processor.process(str(data_dir), work_unit)

        assert mock_s3.upload_directory.call_count == 2
