"""Tests for GFS field loading — mainly the coordinate conventions.

Uses a real 10x10 degree GRIB subset fetched from NOMADS (`tests/fixtures`), so
these exercise the actual cfgrib/rioxarray path rather than a synthetic array.
"""

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from services.gfs_field import clip, coordinate_mesh, load_field, to_geographic

FIXTURE = Path(__file__).parent / "fixtures" / "gfs_sample_f003.grib2"
# The fixture covers 290-300E / -40..-30, i.e. -70..-60 in project longitudes.
BOUNDS = {"minx": -70.0, "miny": -40.0, "maxx": -60.0, "maxy": -30.0}


def _synthetic_0_360() -> xr.DataArray:
    """A field on a 0-360 longitude axis with south-to-north latitudes."""
    return xr.DataArray(
        np.arange(12, dtype="float32").reshape(3, 4),
        dims=("latitude", "longitude"),
        coords={
            "latitude": [-60.0, -40.0, -20.0],
            "longitude": [250.0, 280.0, 310.0, 340.0],
        },
    )


class TestToGeographic:
    """The conversions that stand between a GRIB subset and an empty output."""

    def test_shifts_longitudes_into_minus_180_to_180(self):
        """Clipping a 250..330 axis to minx=-110 would select nothing."""
        result = to_geographic(_synthetic_0_360())
        # 250/280/310/340 E become -110/-80/-50/-20.
        assert list(result["x"].values) == [-110.0, -80.0, -50.0, -20.0]

    def test_longitudes_come_out_sorted(self):
        """Shifting alone leaves the axis unordered, which breaks clip_box."""
        xs = to_geographic(_synthetic_0_360())["x"].values
        assert list(xs) == sorted(xs)

    def test_values_follow_their_longitude(self):
        """The shift must move data with coordinates, not just relabel the axis."""
        source = _synthetic_0_360()
        expected = float(source.sel(longitude=250.0).isel(latitude=2))
        result = to_geographic(source)
        assert float(result.sel(x=-110.0).sel(y=-20.0)) == expected

    def test_latitudes_are_flipped_to_north_up(self):
        ys = to_geographic(_synthetic_0_360())["y"].values
        assert list(ys) == sorted(ys, reverse=True)

    def test_dims_are_renamed_for_rioxarray(self):
        assert to_geographic(_synthetic_0_360()).dims == ("y", "x")

    def test_crs_is_set(self):
        assert to_geographic(_synthetic_0_360()).rio.crs.to_epsg() == 4326

    def test_already_signed_longitudes_are_left_alone(self):
        field = _synthetic_0_360().assign_coords(
            longitude=[-110.0, -80.0, -50.0, -20.0]
        )
        assert float(to_geographic(field)["x"].min()) == -110.0


class TestLoadFieldFromRealGrib:
    def test_reads_a_single_level_variable(self):
        field = load_field(FIXTURE, "mslet", BOUNDS)
        assert field.dims == ("y", "x")
        assert field.shape == (41, 41)

    def test_pressure_values_are_physically_plausible(self):
        hpa = load_field(FIXTURE, "mslet", BOUNDS) / 100.0
        assert 870.0 < float(hpa.min()) < float(hpa.max()) < 1090.0

    @pytest.mark.parametrize("level", [1000, 500, 250])
    def test_reads_each_isobaric_level(self, level):
        field = load_field(FIXTURE, "gh", BOUNDS, level_hpa=level)
        assert field.shape == (41, 41)

    def test_geopotential_height_increases_with_altitude(self):
        """A basic sanity check that level selection is not silently transposed."""
        h1000 = float(load_field(FIXTURE, "gh", BOUNDS, level_hpa=1000).mean())
        h500 = float(load_field(FIXTURE, "gh", BOUNDS, level_hpa=500).mean())
        h250 = float(load_field(FIXTURE, "gh", BOUNDS, level_hpa=250).mean())
        assert h1000 < h500 < h250

    def test_thickness_is_in_the_expected_range(self):
        h500 = load_field(FIXTURE, "gh", BOUNDS, level_hpa=500)
        h1000 = load_field(FIXTURE, "gh", BOUNDS, level_hpa=1000)
        thickness = h500 - h1000
        assert 4800.0 < float(thickness.min()) < float(thickness.max()) < 6000.0

    def test_temperature_is_in_kelvin(self):
        kelvin = load_field(FIXTURE, "t", BOUNDS, level_hpa=500)
        assert 200.0 < float(kelvin.mean()) < 280.0

    def test_output_longitudes_are_signed(self):
        """The fixture is stored as 290-300E and must come back as -70..-60."""
        field = load_field(FIXTURE, "mslet", BOUNDS)
        assert float(field["x"].min()) == pytest.approx(-70.0)
        assert float(field["x"].max()) == pytest.approx(-60.0)

    @pytest.mark.parametrize("short_name", ["u", "v"])
    def test_reads_wind_components(self, short_name):
        assert load_field(FIXTURE, short_name, BOUNDS, level_hpa=250).shape == (41, 41)


class TestClip:
    def test_narrows_to_the_requested_box(self):
        field = load_field(FIXTURE, "mslet", BOUNDS)
        narrowed = clip(
            field, {"minx": -68.0, "miny": -38.0, "maxx": -64.0, "maxy": -34.0}
        )
        assert narrowed.shape[0] < field.shape[0]
        assert float(narrowed["x"].min()) >= -68.5


class TestCoordinateMesh:
    def test_matches_the_field_shape(self):
        field = load_field(FIXTURE, "mslet", BOUNDS)
        lon_2d, lat_2d = coordinate_mesh(field)
        assert lon_2d.shape == lat_2d.shape == field.shape

    def test_rows_vary_by_latitude_and_columns_by_longitude(self):
        field = load_field(FIXTURE, "mslet", BOUNDS)
        lon_2d, lat_2d = coordinate_mesh(field)
        assert lon_2d[0, 0] != lon_2d[0, 1]
        assert lat_2d[0, 0] != lat_2d[1, 0]
