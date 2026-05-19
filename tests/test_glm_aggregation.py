"""Tests for the GLM aggregation + reprojection service.

The aggregation test uses real CG_GLM-L2-GLMF sample files shipped under
``./data/glm_h5/``. If those files are missing (e.g. CI without the data
mount), the test is skipped rather than failed.
"""

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from services.glm_aggregation import aggregate_glm_window, reproject_to_latlon


SAMPLE_DIR = Path("data/glm_h5")
SAMPLE_FILES = sorted(SAMPLE_DIR.glob("CG_GLM-L2-GLMF-M3_*.nc"))[:3]


def _require_sample_files():
    if len(SAMPLE_FILES) < 3:
        pytest.skip(
            f"need >=3 CG_GLM-L2-GLMF sample files under {SAMPLE_DIR}, "
            f"found {len(SAMPLE_FILES)}"
        )


def test_aggregate_empty_files_raises():
    with pytest.raises(ValueError, match="at least one file"):
        aggregate_glm_window(
            [], datetime(2026, 3, 2, 14, 0), datetime(2026, 3, 2, 14, 3), 3
        )


def test_aggregate_glm_window_returns_expected_variables():
    _require_sample_files()

    aggregated = aggregate_glm_window(
        SAMPLE_FILES,
        window_start=datetime(2026, 3, 2, 14, 0),
        window_end=datetime(2026, 3, 2, 14, 3),
        accum_minutes=3,
    )

    for var in ("flash_extent_density", "total_energy", "minimum_flash_area"):
        assert var in aggregated.data_vars, f"missing aggregated var: {var}"

    assert "goes_imager_projection" in aggregated.variables
    assert aggregated.sizes["time"] == 1
    assert aggregated.sizes["x"] == 5424
    assert aggregated.sizes["y"] == 5424


def test_aggregate_fed_sums_across_minutes():
    """FED is extensive — the aggregated value must be >= the value of any single minute."""
    _require_sample_files()

    aggregated = aggregate_glm_window(
        SAMPLE_FILES,
        window_start=datetime(2026, 3, 2, 14, 0),
        window_end=datetime(2026, 3, 2, 14, 3),
        accum_minutes=3,
    )
    fed_agg_max = float(np.nanmax(aggregated["flash_extent_density"].values))
    assert fed_agg_max > 0, "expected lightning activity in sample window"


def test_aggregate_emits_nan_for_empty_fed_toe_cells():
    """FED/TOE empty cells must arrive as NaN, not 0 — otherwise tiles render opaque."""
    _require_sample_files()

    aggregated = aggregate_glm_window(
        SAMPLE_FILES,
        window_start=datetime(2026, 3, 2, 14, 0),
        window_end=datetime(2026, 3, 2, 14, 3),
        accum_minutes=3,
    )

    for var in ("flash_extent_density", "total_energy"):
        values = aggregated[var].values
        assert (values == 0).sum() == 0, f"{var} still has zero-valued empty cells"
        assert np.isnan(
            values
        ).any(), f"{var} should have NaN where no lightning was observed"

    # MFA's behavior is unchanged — glmtools.aggregate already returns NaN for it.
    assert np.isnan(aggregated["minimum_flash_area"].values).any()


def test_reproject_carries_nan_nodata():
    """Reprojection must produce a NaN-nodata raster, not a 0-filled one."""
    _require_sample_files()

    aggregated = aggregate_glm_window(
        SAMPLE_FILES,
        window_start=datetime(2026, 3, 2, 14, 0),
        window_end=datetime(2026, 3, 2, 14, 3),
        accum_minutes=3,
    )
    reprojected = reproject_to_latlon(
        aggregated,
        var_name="flash_extent_density",
        bounds={"minx": -75.0, "maxx": -50.0, "miny": -40.0, "maxy": -20.0},
        resolution_deg=0.1,
    )

    assert np.isnan(reprojected.rio.nodata)
    assert (reprojected.values == 0).sum() == 0
    assert np.isnan(reprojected.values).any()


def test_reproject_to_latlon_clips_to_bounds():
    _require_sample_files()

    aggregated = aggregate_glm_window(
        SAMPLE_FILES,
        window_start=datetime(2026, 3, 2, 14, 0),
        window_end=datetime(2026, 3, 2, 14, 3),
        accum_minutes=3,
    )
    bounds = {"minx": -75.0, "maxx": -50.0, "miny": -40.0, "maxy": -20.0}
    reprojected = reproject_to_latlon(
        aggregated,
        var_name="flash_extent_density",
        bounds=bounds,
        resolution_deg=0.1,
    )

    assert reprojected.rio.crs.to_epsg() == 4326
    xmin, ymin, xmax, ymax = reprojected.rio.bounds()
    # rio.clip_box keeps cells touching the box → bounds extend slightly outside
    # the requested box; allow one pixel of slack on each side.
    slack = 0.2
    assert bounds["minx"] - slack <= xmin <= bounds["minx"] + slack
    assert bounds["maxx"] - slack <= xmax <= bounds["maxx"] + slack
    assert bounds["miny"] - slack <= ymin <= bounds["miny"] + slack
    assert bounds["maxy"] - slack <= ymax <= bounds["maxy"] + slack
    assert "time" not in reprojected.dims
