"""Unit tests for prewarp_to_mercator_grid and friends in processing_steps."""

import math
import os
import shutil
import subprocess
import sys
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest
import rioxarray  # noqa: F401  # registers the .rio accessor
import xarray as xr

from services.processing_steps import (
    _mercator_resolution_for_zoom,
    compute_brightness_temperature,
    mask_outside_range,
    prewarp_to_mercator_grid,
)


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
        import rasterio  # pylint: disable=import-outside-toplevel
        from rasterio.transform import (  # pylint: disable=import-outside-toplevel
            from_origin,
        )

        src = tmp_path / "in.tif"
        data = np.full((4, 8, 8), 255, dtype=np.uint8)
        data[3, :2, :] = 0  # row band-4 alpha=0 on first 2 rows → transparent
        with rasterio.open(
            src,
            "w",
            driver="GTiff",
            count=4,
            height=8,
            width=8,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(-1.0, 1.0, 0.25, 0.25),
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


class TestMaskOutsideRange:
    """In-place NaN masking, the shared building block of the science steps.

    It replaces ``xr.where((v >= low) & (v <= high), v, np.nan)``, so the
    semantics it must reproduce exactly are: inclusive bounds, and NaN left
    alone (NaN compares False against both bounds, and is already NaN).
    """

    def test_values_inside_range_survive_untouched(self):
        values = np.array([150.0, 200.0, 349.9], dtype=np.float32)
        expected = values.copy()
        mask_outside_range(values, 150.0, 350.0)
        np.testing.assert_array_equal(values, expected)

    def test_values_below_low_become_nan(self):
        values = np.array([149.999, 0.0, -1e30], dtype=np.float32)
        mask_outside_range(values, 150.0, 350.0)
        assert np.isnan(values).all()

    def test_values_above_high_become_nan(self):
        values = np.array([350.001, 1e30, np.inf], dtype=np.float32)
        mask_outside_range(values, 150.0, 350.0)
        assert np.isnan(values).all()

    def test_bounds_are_inclusive(self):
        """A value exactly on either bound is kept, matching (v >= low) & (v <= high)."""
        values = np.array([150.0, 350.0], dtype=np.float32)
        mask_outside_range(values, 150.0, 350.0)
        np.testing.assert_array_equal(values, [150.0, 350.0])

    def test_incoming_nan_stays_nan(self):
        """NaN compares False against both bounds, so it must survive as NaN."""
        values = np.array([np.nan, 200.0], dtype=np.float32)
        mask_outside_range(values, 150.0, 350.0)
        assert np.isnan(values[0])
        assert values[1] == 200.0

    def test_mutates_in_place_and_returns_nothing(self):
        """The caller relies on the buffer being reused, not replaced."""
        values = np.array([10.0, 200.0], dtype=np.float32)
        buffer_id = values.__array_interface__["data"][0]

        assert mask_outside_range(values, 150.0, 350.0) is None
        assert values.__array_interface__["data"][0] == buffer_id
        assert np.isnan(values[0])

    def test_preserves_dtype(self):
        """float32 input must not be promoted by the NaN assignment."""
        values = np.array([10.0, 200.0], dtype=np.float32)
        mask_outside_range(values, 150.0, 350.0)
        assert values.dtype == np.float32


# Real GOES-19 ABI band 13 calibration constants, so the fixture lands in the
# physical 150-350 K window the way production data does.
_FK1 = 10860.400390625
_FK2 = 1395.18994140625
_BC1 = 0.07480999827384949
_BC2 = 0.999750018119812


def _planck_reference(radiance: np.ndarray) -> np.ndarray:
    """Straight transcription of the documented formula, for comparison.

    T = (fk2 / ln((fk1 / radiance) + 1) - bc1) / bc2, with non-positive
    radiance floored at 1e-10 and out-of-range results set to NaN.
    """
    safe = np.where(radiance <= 0, 1e-10, radiance)
    temperature = (_FK2 / np.log((_FK1 / safe) + 1.0) - _BC1) / _BC2
    return np.where(
        (temperature >= 150) & (temperature <= 350), temperature, np.nan
    ).astype(radiance.dtype)


def _radiance_dataset(values: np.ndarray) -> xr.Dataset:
    """Minimal georeferenced dataset shaped like the output of georeferencing."""
    radiance = xr.DataArray(
        values,
        dims=("y", "x"),
        coords={
            "y": np.arange(values.shape[0], dtype=float) * 2000.0,
            "x": np.arange(values.shape[1], dtype=float) * 2000.0,
        },
        name="Rad",
    )
    dataset = radiance.to_dataset(name="Rad")
    for name, constant in (
        ("planck_fk1", _FK1),
        ("planck_fk2", _FK2),
        ("planck_bc1", _BC1),
        ("planck_bc2", _BC2),
    ):
        dataset[name] = xr.DataArray(np.float32(constant))
    dataset.rio.write_crs(
        "+proj=geos +h=35786023 +a=6378137 +b=6356752.31414 +lon_0=-75 +sweep=x",
        inplace=True,
    )
    return dataset


class TestComputeBrightnessTemperature:
    """The inverse Planck step runs in place over a single buffer.

    These tests pin the two properties that rewrite could silently break: the
    numeric result, and the fact that the source dataset is never written into
    (``ComputeBrightnessTemperaturesService`` keeps every input Dataset alive in
    a dict while several conversions run concurrently).
    """

    def test_matches_the_documented_planck_formula(self):
        radiance = np.array([[6.5, 20.0, 60.0], [90.0, 120.0, 141.5]], dtype=np.float32)

        result = compute_brightness_temperature(_radiance_dataset(radiance))

        np.testing.assert_array_equal(result.values, _planck_reference(radiance))
        assert not np.isnan(result.values).any(), "fixture should be fully in range"

    def test_non_positive_radiance_never_reaches_the_logarithm(self):
        """The 1e-10 floor exists so 0 and negative radiance don't hit fk1/0 or log(<0).

        Without it, 0 raises a divide-by-zero and -5 makes the logarithm invalid.
        """
        radiance = np.array([[0.0, -5.0, 60.0]], dtype=np.float32)

        with np.errstate(divide="raise", invalid="raise"):
            values = compute_brightness_temperature(_radiance_dataset(radiance)).values

        # Floored to 1e-10 the Planck chain yields ~43 K, under the 150 K floor.
        assert np.isnan(values[0, 0])
        assert np.isnan(values[0, 1])
        assert not np.isnan(values[0, 2])

    def test_non_physical_temperatures_become_nan(self):
        """Radiance far outside the calibrated span maps outside 150-350 K."""
        radiance = np.array([[1e-30, 1e30, 60.0]], dtype=np.float32)

        values = compute_brightness_temperature(_radiance_dataset(radiance)).values

        assert np.isnan(values[0, 0]), "tiny radiance -> T > 350 K"
        assert np.isnan(values[0, 1]), "huge radiance -> T < 150 K"
        assert not np.isnan(values[0, 2])

    def test_extreme_radiance_does_not_emit_runtime_warnings(self):
        """Raw numpy replaced xarray here, and xarray used to swallow fp warnings.

        Radiance large enough to collapse ln(fk1/L + 1) to 0 divides by zero.
        The inf is masked out anyway, so worker logs must stay clean.
        """
        radiance = np.array([[1e30, 60.0]], dtype=np.float32)

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            result = compute_brightness_temperature(_radiance_dataset(radiance))

        assert np.isnan(result.values[0, 0])

    def test_incoming_nan_propagates(self):
        radiance = np.array([[np.nan, 60.0]], dtype=np.float32)

        values = compute_brightness_temperature(_radiance_dataset(radiance)).values

        assert np.isnan(values[0, 0])
        assert not np.isnan(values[0, 1])

    def test_does_not_write_into_the_source_radiance(self):
        """The in-place chain must run on its own buffer, not on dataset['Rad']."""
        radiance = np.array([[6.5, 0.0, 60.0], [-1.0, 120.0, np.nan]], dtype=np.float32)
        dataset = _radiance_dataset(radiance.copy())
        before = dataset["Rad"].values.copy()

        result = compute_brightness_temperature(dataset)

        np.testing.assert_array_equal(dataset["Rad"].values, before)
        assert not np.shares_memory(result.values, dataset["Rad"].values)

    def test_preserves_dtype_dims_coords_and_crs(self):
        radiance = np.array([[6.5, 60.0], [90.0, 120.0]], dtype=np.float32)
        dataset = _radiance_dataset(radiance)

        result = compute_brightness_temperature(dataset)

        assert result.dtype == np.float32
        assert result.dims == ("y", "x")
        np.testing.assert_array_equal(result.coords["x"].values, [0.0, 2000.0])
        assert result.rio.crs == dataset.rio.crs
