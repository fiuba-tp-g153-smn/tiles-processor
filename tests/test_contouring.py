"""Tests for the synthetic-field contour extraction service."""

import json
import os
import sys

import numpy as np
import pytest
import xarray as xr

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from services.contouring import (  # noqa: E402  pylint: disable=wrong-import-position
    BARB_ZOOM_STRIDES,
    extract_barbs,
    extract_barbs_tiled,
    extract_isolines,
    smooth_field,
    write_geojson,
)


def _paraboloid(grid_step: float = 0.5) -> xr.DataArray:
    """Build a paraboloid centered at (0,0): z = 1000 + (x^2 + y^2) / 4.

    On a 41×41 grid covering [-10, 10]² in 0.5° steps, z spans [1000, 1050]
    so multiples-of-5 contours land at 11 levels (1000, 1005, …, 1050).
    """
    x = np.arange(-10.0, 10.0 + grid_step, grid_step)
    y = np.arange(-10.0, 10.0 + grid_step, grid_step)
    xx, yy = np.meshgrid(x, y)
    z = 1000.0 + (xx**2 + yy**2) / 4.0
    return xr.DataArray(z, dims=("y", "x"), coords={"x": x, "y": y})


class TestExtractIsolines:
    def test_returns_features_at_expected_levels(self):
        """Interior multiples of `step` produce LineString features.

        With z spanning [1000, 1050] exactly, the boundary levels 1000 (a single
        point) and 1050 (degenerate corners) yield no LineStrings; the strictly
        interior levels 1005..1045 (every 5 hPa) all produce a closed contour.
        """
        da = _paraboloid()
        features = extract_isolines(
            da,
            step=5.0,
            simplify_tolerance=0.0,
            value_property="pressure_hpa",
        )

        levels = sorted({f["properties"]["pressure_hpa"] for f in features})
        assert levels == [1005.0 + 5.0 * i for i in range(9)]

    def test_features_have_linestring_geometry_and_property(self):
        da = _paraboloid()
        features = extract_isolines(
            da,
            step=5.0,
            simplify_tolerance=0.0,
            value_property="pressure_hpa",
        )

        assert features, "expected at least one feature"
        for feat in features:
            assert feat["type"] == "Feature"
            assert feat["geometry"]["type"] == "LineString"
            assert isinstance(feat["properties"]["pressure_hpa"], float)
            # Each segment must have ≥ 2 (lon, lat) points
            assert len(feat["geometry"]["coordinates"]) >= 2

    def test_contour_values_match_synthetic_field(self):
        """Spot-check: 1010 hPa contour lies on r = 2·sqrt(10) ≈ 6.32°."""
        da = _paraboloid()
        features = extract_isolines(
            da, step=5.0, simplify_tolerance=0.0, value_property="pressure_hpa"
        )
        ring = next(f for f in features if f["properties"]["pressure_hpa"] == 1010.0)
        coords = np.asarray(ring["geometry"]["coordinates"])
        radii = np.hypot(coords[:, 0], coords[:, 1])
        expected_r = 2.0 * np.sqrt(10.0)
        # Contour resolution ~ grid spacing (0.5°); allow a generous tolerance.
        assert np.allclose(radii, expected_r, atol=0.4)

    def test_simplify_reduces_vertex_count(self):
        """Higher tolerance must yield ≤ vertices for the same level."""
        da = _paraboloid()

        def vertex_count(features, level):
            return sum(
                len(f["geometry"]["coordinates"])
                for f in features
                if f["properties"]["pressure_hpa"] == level
            )

        precise = extract_isolines(
            da, step=5.0, simplify_tolerance=0.0, value_property="pressure_hpa"
        )
        coarse = extract_isolines(
            da, step=5.0, simplify_tolerance=2.0, value_property="pressure_hpa"
        )

        precise_vertices = vertex_count(precise, 1010.0)
        coarse_vertices = vertex_count(coarse, 1010.0)
        assert coarse_vertices < precise_vertices
        assert coarse_vertices >= 4  # still a closed-ish polygon

    def test_uniform_field_returns_empty(self):
        """A constant field has no isolines (vmin == vmax)."""
        x = np.arange(-5.0, 5.5, 0.5)
        y = np.arange(-5.0, 5.5, 0.5)
        z = np.full((len(y), len(x)), 1013.0)
        da = xr.DataArray(z, dims=("y", "x"), coords={"x": x, "y": y})

        assert (
            extract_isolines(
                da, step=5.0, simplify_tolerance=0.0, value_property="pressure_hpa"
            )
            == []
        )

    def test_all_nan_returns_empty(self):
        x = np.arange(-5.0, 5.5, 0.5)
        y = np.arange(-5.0, 5.5, 0.5)
        z = np.full((len(y), len(x)), np.nan)
        da = xr.DataArray(z, dims=("y", "x"), coords={"x": x, "y": y})

        assert (
            extract_isolines(
                da, step=5.0, simplify_tolerance=0.0, value_property="pressure_hpa"
            )
            == []
        )


class TestSmoothField:
    def test_reduces_high_frequency_noise(self):
        """Smoothing decreases the std-dev of a noisy field."""
        rng = np.random.default_rng(seed=42)
        base = _paraboloid()
        noise = rng.normal(0.0, 5.0, size=base.shape).astype(np.float32)
        noisy = base + noise

        smoothed = smooth_field(noisy, sigma=2.0)

        # Compare against base: smoothing should bring the noisy field closer.
        assert np.std(smoothed.values - base.values) < np.std(
            noisy.values - base.values
        )

    def test_preserves_nan_locations(self):
        da = _paraboloid().copy()
        da.values[0, 0] = np.nan
        da.values[5, 7] = np.nan

        smoothed = smooth_field(da, sigma=1.0)

        assert np.isnan(smoothed.values[0, 0])
        assert np.isnan(smoothed.values[5, 7])
        # Non-NaN cells stay finite
        assert np.isfinite(smoothed.values[10, 10])

    def test_preserves_coords(self):
        da = _paraboloid()
        smoothed = smooth_field(da, sigma=1.0)
        np.testing.assert_array_equal(smoothed["x"].values, da["x"].values)
        np.testing.assert_array_equal(smoothed["y"].values, da["y"].values)


class TestWriteGeoJSON:
    def test_writes_rfc7946_compliant_feature_collection(self, tmp_path):
        features = [
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
                "properties": {"pressure_hpa": 1015.0},
            }
        ]
        out = tmp_path / "out.json"
        write_geojson(features, out)

        payload = json.loads(out.read_text())
        assert payload["type"] == "FeatureCollection"
        assert payload["features"] == features
        # RFC 7946 deprecates the `crs` member; it must NOT be present.
        assert "crs" not in payload

    def test_empty_features_emits_empty_collection(self, tmp_path):
        out = tmp_path / "empty.json"
        write_geojson([], out)
        payload = json.loads(out.read_text())
        assert payload == {"type": "FeatureCollection", "features": []}


def _wind_grid(n: int = 160):
    """Finite u/v wind field on a curvilinear lon/lat mesh over Argentina.

    Large enough (n>150) that the coarsest stride (150) still yields barbs, so
    every zoom in BARB_ZOOM_STRIDES produces at least one tile.
    """
    lon, lat = np.meshgrid(np.linspace(-75.0, -55.0, n), np.linspace(-40.0, -20.0, n))
    u = np.full((n, n), 12.0, dtype=np.float64)
    v = np.full((n, n), -7.0, dtype=np.float64)
    return u, v, lon, lat


class TestExtractBarbsTiled:
    """Barb tiling is capped at z8; z10/z12 (redundant stride-9 re-tiling) gone."""

    def test_strides_capped_at_z8(self):
        """The redundant high-zoom barb tilesets must no longer be configured."""
        assert set(BARB_ZOOM_STRIDES) == {2, 4, 6, 8}
        assert 10 not in BARB_ZOOM_STRIDES and 12 not in BARB_ZOOM_STRIDES

    def test_tiled_emits_only_capped_zooms(self):
        """extract_barbs_tiled must bucket features only into zooms {2,4,6,8}."""
        u, v, lon, lat = _wind_grid()
        tiled = extract_barbs_tiled(u, v, lon, lat)

        zooms = {zoom for (zoom, _tx, _ty) in tiled}
        assert zooms == {2, 4, 6, 8}
        assert max(zooms) == 8  # no z10/z12 write storm

    def test_z8_preserves_full_stride9_point_set(self):
        """No barb DATA is lost: the z8 tiles together hold every stride-9 point.

        z8/z10/z12 all used stride 9 — identical points. Dropping z10/z12 only
        removes redundant re-tiling, so the union of all z8 tile features must
        still equal the complete stride-9 barb set.
        """
        u, v, lon, lat = _wind_grid()
        tiled = extract_barbs_tiled(u, v, lon, lat)

        z8_count = sum(
            len(feats) for (zoom, _tx, _ty), feats in tiled.items() if zoom == 8
        )
        expected = len(extract_barbs(u, v, lon, lat, stride=BARB_ZOOM_STRIDES[8]))
        assert z8_count == expected
        assert expected > 0  # sanity: the fixture actually produced barbs


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
