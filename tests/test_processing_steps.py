"""Unit tests for compute_glm_grids and prewarp_to_mercator_grid in processing_steps."""

import math
import os
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest
import xarray as xr

import rioxarray  # noqa: F401  # ensures rio accessor is registered

from services.processing_steps import (
    _mercator_resolution_for_zoom,
    compute_glm_grids,
    prewarp_to_mercator_grid,
)

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


class TestMercatorResolutionForZoom:
    """Sanity checks for the zoom→resolution helper."""

    def test_z0_matches_canonical_constant(self):
        """At zoom 0 a 256-px tile spans the equatorial circumference."""
        # Well-known Web Mercator constant: 2πR / 256 ≈ 156543.03 m/px
        assert math.isclose(
            _mercator_resolution_for_zoom(0), 156543.03392804097, rel_tol=1e-9
        )

    def test_each_zoom_halves_pixel_size(self):
        """Each +1 zoom level halves the meter-per-pixel resolution."""
        for z in range(0, 18):
            assert math.isclose(
                _mercator_resolution_for_zoom(z) / 2,
                _mercator_resolution_for_zoom(z + 1),
                rel_tol=1e-12,
            )

    def test_z7_is_about_1223m(self):
        """Spot check: zoom 7 ≈ 1223 m/px (production max-native zoom for ECMWF TP)."""
        assert math.isclose(_mercator_resolution_for_zoom(7), 1222.99245, rel_tol=1e-5)


class TestPrewarpToMercatorGrid:
    """Tests for the prewarp_to_mercator_grid helper."""

    @pytest.fixture
    def tmp_output_dir(self, tmp_path):
        d = tmp_path / "out"
        d.mkdir()
        return d

    @pytest.fixture
    def mock_input(self, tmp_path):
        f = tmp_path / "input.tif"
        f.write_text("mock content")
        return f

    def test_command_uses_epsg_3857_near_resampling_and_dstalpha(
        self, mock_input, tmp_output_dir
    ):
        """gdalwarp command carries the flags needed to preserve alpha + cell edges."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="", stdout="")
            with patch.object(Path, "rename"):
                prewarp_to_mercator_grid(mock_input, tmp_output_dir, max_zoom=7)

        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "gdalwarp"
        assert "EPSG:3857" in cmd
        assert cmd[cmd.index("-r") + 1] == "near"
        assert "-dstalpha" in cmd
        assert str(mock_input) in cmd

    def test_tr_argument_equals_zoom_resolution(self, mock_input, tmp_output_dir):
        """The -tr flag uses the canonical Web Mercator resolution for max_zoom."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            with patch.object(Path, "rename"):
                prewarp_to_mercator_grid(mock_input, tmp_output_dir, max_zoom=7)

        cmd = mock_run.call_args[0][0]
        tr_idx = cmd.index("-tr")
        res_x = float(cmd[tr_idx + 1])
        res_y = float(cmd[tr_idx + 2])
        assert res_x == res_y
        assert math.isclose(res_x, _mercator_resolution_for_zoom(7), rel_tol=1e-12)

    def test_returns_expected_output_path(self, mock_input, tmp_output_dir):
        """Returns <output_dir>/<stem>_3857.tif on success."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            with patch.object(Path, "rename"):
                result = prewarp_to_mercator_grid(mock_input, tmp_output_dir, max_zoom=7)

        assert result == tmp_output_dir / f"{mock_input.stem}_3857.tif"

    def test_atomic_rename_from_tmp_to_final(self, mock_input, tmp_output_dir):
        """The function writes through a uuid-named tmp and atomically renames."""
        rename_calls = []
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            with patch.object(Path, "rename") as mock_rename:
                mock_rename.side_effect = lambda dest: rename_calls.append(dest)
                prewarp_to_mercator_grid(mock_input, tmp_output_dir, max_zoom=7)

        assert len(rename_calls) == 1
        assert rename_calls[0] == tmp_output_dir / f"{mock_input.stem}_3857.tif"

    def test_cleanup_on_nonzero_returncode(self, mock_input, tmp_output_dir):
        """When gdalwarp exits non-zero we unlink the tmp file and raise RuntimeError."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="gdalwarp boom")
            with patch.object(Path, "unlink") as mock_unlink:
                with pytest.raises(RuntimeError, match="gdalwarp pre-warp failed"):
                    prewarp_to_mercator_grid(mock_input, tmp_output_dir, max_zoom=7)

        mock_unlink.assert_called_once_with(missing_ok=True)

    def test_cleanup_on_timeout(self, mock_input, tmp_output_dir):
        """Timeout is re-raised as RuntimeError and the tmp file is unlinked."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(
                cmd="gdalwarp", timeout=600
            )
            with patch.object(Path, "unlink") as mock_unlink:
                with pytest.raises(RuntimeError, match="timed out"):
                    prewarp_to_mercator_grid(mock_input, tmp_output_dir, max_zoom=7)

        mock_unlink.assert_called_once_with(missing_ok=True)

    @pytest.mark.skipif(
        shutil.which("gdalwarp") is None, reason="gdalwarp binary not available"
    )
    def test_integration_real_gdalwarp_writes_epsg_3857(self, tmp_path):
        """End-to-end with real gdalwarp on a tiny rasterio-written input."""
        import rasterio
        from rasterio.transform import from_origin

        src = tmp_path / "in.tif"
        data = np.full((4, 8, 8), 255, dtype=np.uint8)
        data[3, :2, :] = 0  # row band-4 alpha=0 on first 2 rows → transparent
        with rasterio.open(
            src, "w", driver="GTiff", count=4, height=8, width=8, dtype="uint8",
            crs="EPSG:4326", transform=from_origin(-1.0, 1.0, 0.25, 0.25),
        ) as dst:
            dst.write(data)
            dst.colorinterp = [
                rasterio.enums.ColorInterp.red,
                rasterio.enums.ColorInterp.green,
                rasterio.enums.ColorInterp.blue,
                rasterio.enums.ColorInterp.alpha,
            ]

        out_dir = tmp_path / "out"
        out_dir.mkdir()
        result = prewarp_to_mercator_grid(src, out_dir, max_zoom=5)

        assert result == out_dir / "in_3857.tif"
        assert result.exists()
        with rasterio.open(result) as ds:
            assert ds.crs.to_epsg() == 3857
            assert math.isclose(
                ds.res[0], _mercator_resolution_for_zoom(5), rel_tol=1e-6
            )
            assert math.isclose(ds.res[1], ds.res[0], rel_tol=1e-12)
