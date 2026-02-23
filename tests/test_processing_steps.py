"""Unit tests for compute_glm_grids in processing_steps."""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest
import xarray as xr

import rioxarray  # noqa: F401  # ensures rio accessor is registered

from services.processing_steps import compute_glm_grids

BOUNDS = {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15}
RESOLUTION = 1.0  # coarse grid for fast tests


def _make_glm_dataset(lats, lons, energies, areas=None):
    """Return a mock xr.Dataset with the given flash arrays.

    Uses side_effect on __getitem__ so each key gets its own .values,
    avoiding the MagicMock pitfall where a single return_value is shared.
    """
    if areas is None:
        areas = [100.0] * len(lats)
    _data = {
        "flash_lat": np.array(lats, dtype=np.float32),
        "flash_lon": np.array(lons, dtype=np.float32),
        "flash_energy": np.array(energies, dtype=np.float64),
        "flash_area": np.array(areas, dtype=np.float32),
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


class TestComputeGlmGrids:
    """compute_glm_grids correctness tests using synthetic flash data."""

    def test_returns_tuple_of_three_dataarrays(self, tmp_path):
        """Return value is a (fed, toe, mfa) tuple of xr.DataArrays."""
        fake_file = tmp_path / "OR_GLM-L2-LCFA_G19_s20260441.nc"
        mock_ds = _make_glm_dataset([], [], [])

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            result = compute_glm_grids([fake_file], BOUNDS, RESOLUTION)

        assert isinstance(result, tuple)
        assert len(result) == 3
        fed, toe, mfa = result
        assert isinstance(fed, xr.DataArray)
        assert isinstance(toe, xr.DataArray)
        assert isinstance(mfa, xr.DataArray)

    def test_fed_counts_flashes_per_cell(self, tmp_path):
        """FED grid value equals the number of flashes in each cell."""
        fake_file = tmp_path / "OR_GLM-L2-LCFA_G19_s20260441.nc"
        # Two flashes in the same 1° cell centred on (-45, -75) and one in (-30, -60)
        lats = [-44.5, -44.8, -29.5]
        lons = [-74.5, -74.8, -59.5]
        energies = [1e-10, 2e-10, 3e-10]
        mock_ds = _make_glm_dataset(lats, lons, energies)

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            fed, _, _ = compute_glm_grids([fake_file], BOUNDS, RESOLUTION)

        # Non-NaN cells should reflect flash counts
        valid = fed.values[~np.isnan(fed.values)]
        assert 2.0 in valid  # cell with two flashes
        assert 1.0 in valid  # cell with one flash

    def test_toe_sums_energy_per_cell(self, tmp_path):
        """TOE grid value equals the sum of flash_energy in each cell."""
        fake_file = tmp_path / "OR_GLM-L2-LCFA_G19_s20260441.nc"
        lats = [-44.5, -44.8, -29.5]
        lons = [-74.5, -74.8, -59.5]
        energies = [1e-10, 2e-10, 5e-10]
        mock_ds = _make_glm_dataset(lats, lons, energies)

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            _, toe, _ = compute_glm_grids([fake_file], BOUNDS, RESOLUTION)

        valid = sorted(toe.values[~np.isnan(toe.values)])
        assert len(valid) == 2
        assert np.isclose(
            valid[0], 3e-10, rtol=1e-5
        )  # sum of two flashes in first cell
        assert np.isclose(valid[1], 5e-10, rtol=1e-5)  # single flash in second cell

    def test_zero_flash_cells_are_nan(self, tmp_path):
        """Cells with no flashes are NaN in FED, TOE, and MFA."""
        fake_file = tmp_path / "OR_GLM-L2-LCFA_G19_s20260441.nc"
        mock_ds = _make_glm_dataset([], [], [])

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            fed, toe, mfa = compute_glm_grids([fake_file], BOUNDS, RESOLUTION)

        assert np.all(np.isnan(fed.values))
        assert np.all(np.isnan(toe.values))
        assert np.all(np.isnan(mfa.values))

    def test_fed_and_toe_share_same_shape(self, tmp_path):
        """FED, TOE, and MFA grids have identical spatial dimensions."""
        fake_file = tmp_path / "OR_GLM-L2-LCFA_G19_s20260441.nc"
        mock_ds = _make_glm_dataset([-45.0], [-75.0], [1e-10])

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            fed, toe, mfa = compute_glm_grids([fake_file], BOUNDS, RESOLUTION)

        assert fed.shape == toe.shape == mfa.shape
        np.testing.assert_array_equal(fed.coords["x"].values, toe.coords["x"].values)
        np.testing.assert_array_equal(fed.coords["y"].values, toe.coords["y"].values)

    def test_crs_is_epsg4326(self, tmp_path):
        """All three output arrays carry an EPSG:4326 CRS."""
        fake_file = tmp_path / "OR_GLM-L2-LCFA_G19_s20260441.nc"
        mock_ds = _make_glm_dataset([], [], [])

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            fed, toe, mfa = compute_glm_grids([fake_file], BOUNDS, RESOLUTION)

        assert fed.rio.crs is not None
        assert toe.rio.crs is not None
        assert mfa.rio.crs is not None
        assert fed.rio.crs.to_epsg() == 4326
        assert toe.rio.crs.to_epsg() == 4326
        assert mfa.rio.crs.to_epsg() == 4326

    def test_mfa_returns_minimum_area_per_cell(self, tmp_path):
        """MFA grid value equals the minimum flash_area in each cell."""
        fake_file = tmp_path / "OR_GLM-L2-LCFA_G19_s20260441.nc"
        # Two flashes in the same 1° cell: areas 200 and 50 km² → MFA should be 50 km²
        # One flash in another cell: area 300 km² → MFA should be 300 km²
        # Mock values are in m² (real unit) — code divides by 1e6 to get km²
        lats = [-44.5, -44.8, -29.5]
        lons = [-74.5, -74.8, -59.5]
        energies = [1e-10, 2e-10, 3e-10]
        areas = [200.0e6, 50.0e6, 300.0e6]
        mock_ds = _make_glm_dataset(lats, lons, energies, areas)

        with patch("services.processing_steps.xr.open_dataset", return_value=mock_ds):
            _, _, mfa = compute_glm_grids([fake_file], BOUNDS, RESOLUTION)

        valid = sorted(mfa.values[~np.isnan(mfa.values)])
        assert len(valid) == 2
        assert np.isclose(valid[0], 50.0, rtol=1e-5)  # min of 200 and 50
        assert np.isclose(valid[1], 300.0, rtol=1e-5)  # single flash
