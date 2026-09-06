"""Equivalence guards for the optimized radar polar->cartesian mapping.

``_compute_cartesian_mapping`` was rewritten to broadcast 1-D offsets instead of
materializing two ``np.meshgrid`` grids, and the RGBA gather in
``_polar_to_geotiff_with_mapping`` was collapsed from four fancy-indexing passes
into one flat ``take``. Both are pure performance changes, so these tests pin the
output against reference implementations that mirror the previous formulation.
"""

import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest

from processors.radar_processor import RadarProcessor, _nearest_azimuth_indices

# Geometries spanning what the SINARAME corpus produces: both grid sizes the
# clamp allows, 360/480/720 rays, azimuths that wrap across 0/360 and azimuths
# stored out of order, and a radar far from the equator (small cos(lat)).
GEOMETRIES = {
    "grid2000_360rays": (np.arange(1000) * 240.0, np.arange(360) * 1.0, -31.44, -64.19),
    "grid2000_720rays": (np.arange(1000) * 480.0, np.arange(720) * 0.5, -34.60, -58.40),
    "grid1000_360rays": (np.arange(200) * 1000.0, np.arange(360) * 1.0, -27.45, -55.90),
    "wrapped_azimuths": (
        np.arange(994) * 300.0,
        (np.arange(360) * 1.0 + 359.5) % 360,
        -38.00,
        -68.00,
    ),
    "unsorted_azimuths": (
        np.arange(500) * 500.0,
        np.random.default_rng(0).permutation(np.arange(360) * 1.0),
        -24.70,
        -65.40,
    ),
    "far_south": (np.arange(800) * 375.0, np.arange(480) * 0.75, -54.80, -68.30),
}


def _radar_processor(tmp_path) -> RadarProcessor:
    config = MagicMock()
    config.TMP_DIR = str(tmp_path)
    with patch("processors.radar_processor.create_s3_client", return_value=AsyncMock()):
        return RadarProcessor(config)


def _reference_mapping(bounds, grid_size, max_range_m, ranges, azimuths, lat, lon):
    """Pre-optimization mapping: two meshgrid grids, sqrt of squares, int64."""
    min_lon, max_lon, min_lat, max_lat = bounds
    lons = np.linspace(min_lon, max_lon, grid_size)
    lats = np.linspace(min_lat, max_lat, grid_size)
    lon_grid, lat_grid = np.meshgrid(lons, lats)

    dx = (lon_grid - lon) * 111000 * np.cos(np.radians(lat))
    dy = (lat_grid - lat) * 111000

    r_grid = np.sqrt(dx**2 + dy**2)
    theta_grid = np.degrees(np.arctan2(dx, dy)) % 360

    range_idx = np.clip(np.searchsorted(ranges, r_grid.ravel()), 0, len(ranges) - 1)
    azimuth_idx = _nearest_azimuth_indices(azimuths, theta_grid.ravel())
    outside_range = r_grid.ravel() > max_range_m
    return range_idx, azimuth_idx, outside_range


@pytest.mark.parametrize("name", sorted(GEOMETRIES))
def test_cartesian_mapping_matches_meshgrid_reference(name, tmp_path):
    """Broadcasting the 1-D offsets reproduces the meshgrid indices exactly."""
    ranges, azimuths, lat, lon = GEOMETRIES[name]
    processor = _radar_processor(tmp_path)

    range_idx, azimuth_idx, outside_range, bounds, grid_size = (
        processor._compute_cartesian_mapping(ranges, azimuths, lat, lon)
    )
    expected = _reference_mapping(
        bounds, grid_size, ranges.max(), ranges, azimuths, lat, lon
    )

    assert np.array_equal(range_idx, expected[0])
    assert np.array_equal(azimuth_idx, expected[1])
    assert np.array_equal(outside_range, expected[2])


@pytest.mark.parametrize("name", sorted(GEOMETRIES))
def test_cartesian_mapping_indices_are_int32(name, tmp_path):
    """Indices stay int32: two 4M-element arrays at half the previous width."""
    ranges, azimuths, lat, lon = GEOMETRIES[name]
    processor = _radar_processor(tmp_path)

    range_idx, azimuth_idx, _, _, _ = processor._compute_cartesian_mapping(
        ranges, azimuths, lat, lon
    )

    assert range_idx.dtype == np.int32
    assert azimuth_idx.dtype == np.int32
