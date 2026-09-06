"""Tests for the windowed GEOS -> EPSG:4326 reprojection helpers.

Structural tests on a synthetic geostationary grid: they lock the *grid*
(shape, transform, CRS) against what ``reproject`` + ``clip_box`` produce, and
lock that ``tolerance=0`` reaches GDAL. They make no claim about cell values —
a synthetic field does not bound reprojection error reliably (a smooth sinusoid
understates it, a hard-edged field overstates it), so the bit-identity claim was
verified against a real ABI C13 outside the suite.
"""

import numpy as np
import pytest
import rioxarray  # noqa: F401  # registers the .rio accessor
import xarray as xr
from pyproj import CRS
from rioxarray.raster_array import RasterArray

from exceptions import UnprocessableInputError
from services.geo_reprojection import compute_target_window, reproject_to_bounds

BOUNDS = {"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0}

_GEOS_ATTRS = {
    "grid_mapping_name": "geostationary",
    "perspective_point_height": 35786023.0,
    "semi_major_axis": 6378137.0,
    "semi_minor_axis": 6356752.31414,
    "longitude_of_projection_origin": -75.0,
    "latitude_of_projection_origin": 0.0,
    "sweep_angle_axis": "x",
}


def _synthetic_geos_array(size: int = 256, half_angle: float = 0.09) -> xr.DataArray:
    """A georeferenced GEOS grid wide enough to cover the project bounds."""
    sat_h = _GEOS_ATTRS["perspective_point_height"]
    x_rad = np.linspace(-half_angle, half_angle, size, dtype=np.float64)
    y_rad = np.linspace(half_angle, -half_angle, size, dtype=np.float64)
    rows, cols = np.meshgrid(
        np.arange(size, dtype=np.float32),
        np.arange(size, dtype=np.float32),
        indexing="ij",
    )
    array = xr.DataArray(
        rows * size + cols,
        dims=("y", "x"),
        coords={"y": y_rad * sat_h, "x": x_rad * sat_h},
        name="synthetic",
    )
    array.rio.write_crs(CRS.from_cf(_GEOS_ATTRS).to_string(), inplace=True)
    array.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    return array


def _reference_full_then_clip(source: xr.DataArray, bounds: dict) -> xr.DataArray:
    """The pre-change path: warp everything, then clip."""
    reprojected = source.rio.reproject("EPSG:4326")
    reprojected.rio.write_nodata(np.nan, inplace=True)
    return reprojected.rio.clip_box(**bounds)


def test_window_grid_matches_full_reproject_then_clip():
    """The windowed grid must be the same grid clip_box would have kept."""
    source = _synthetic_geos_array()

    transform, shape = compute_target_window(source, BOUNDS)
    reference = _reference_full_then_clip(source, BOUNDS)

    assert shape == reference.shape
    # Composing the affine directly vs. recalculating it from the clipped
    # coordinates differs only by float64 rounding (~1e-14 deg, sub-nanometre).
    assert transform.a == pytest.approx(reference.rio.transform().a, rel=1e-12)
    assert transform.e == pytest.approx(reference.rio.transform().e, rel=1e-12)
    assert transform.c == pytest.approx(reference.rio.transform().c, abs=1e-9)
    assert transform.f == pytest.approx(reference.rio.transform().f, abs=1e-9)


def test_reproject_to_bounds_preserves_crs_and_shape():
    source = _synthetic_geos_array()

    result = reproject_to_bounds(source, BOUNDS)

    assert result.rio.crs.to_epsg() == 4326
    assert result.shape == compute_target_window(source, BOUNDS)[1]
    # The window is the intersection of the box with the source extent, so it
    # can fall short of the box (this synthetic disk ends west of maxx) but must
    # never overshoot it by more than the one cell clip_box keeps for touching.
    xmin, ymin, xmax, ymax = result.rio.bounds()
    res_x, res_y = result.rio.resolution()
    assert xmin >= BOUNDS["minx"] - abs(res_x)
    assert xmax <= BOUNDS["maxx"] + abs(res_x)
    assert ymin >= BOUNDS["miny"] - abs(res_y)
    assert ymax <= BOUNDS["maxy"] + abs(res_y)
    assert result.rio.bounds() == pytest.approx(
        _reference_full_then_clip(source, BOUNDS).rio.bounds(), abs=1e-9
    )


def test_reproject_to_bounds_disables_the_approximate_transformer(monkeypatch):
    """``tolerance=0`` must reach GDAL: without it the windowed warp is wrong.

    GDAL fits a polynomial approximation of the transformer per warp chunk
    (default threshold 0.125 destination pixels). The fit depends on the warped
    extent, so a window and a full disk disagree — measured at 95-99% of cells
    matching on real data. This locks the kwarg that turns it off.
    """
    captured: dict = {}
    original = RasterArray.reproject

    def spy(self, dst_crs, **kwargs):
        captured.update(kwargs)
        return original(self, dst_crs, **kwargs)

    monkeypatch.setattr(RasterArray, "reproject", spy)
    reproject_to_bounds(_synthetic_geos_array(), BOUNDS)

    assert captured["tolerance"] == 0


def test_reproject_to_bounds_forwards_kwargs(monkeypatch):
    """Caller kwargs (e.g. GLM's finite nodata sentinel) reach rio.reproject."""
    captured: dict = {}
    original = RasterArray.reproject

    def spy(self, dst_crs, **kwargs):
        captured.update(kwargs)
        return original(self, dst_crs, **kwargs)

    monkeypatch.setattr(RasterArray, "reproject", spy)
    reproject_to_bounds(_synthetic_geos_array(), BOUNDS, nodata=-9999.0)

    assert captured["nodata"] == -9999.0


def test_bounds_wider_than_source_clamp_to_the_full_grid():
    """A box that swallows the source yields the whole destination grid."""
    source = _synthetic_geos_array()
    whole_world = {"minx": -180.0, "miny": -90.0, "maxx": 180.0, "maxy": 90.0}

    _, shape = compute_target_window(source, whole_world)

    assert shape == source.rio.reproject("EPSG:4326").shape


def test_bounds_outside_the_source_raise_unprocessable_input():
    """Fail fast, and as a non-retryable data-shape error."""
    source = _synthetic_geos_array()
    elsewhere = {"minx": 100.0, "miny": 10.0, "maxx": 120.0, "maxy": 30.0}

    with pytest.raises(UnprocessableInputError, match="do not intersect"):
        compute_target_window(source, elsewhere)
