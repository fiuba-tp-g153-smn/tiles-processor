"""Tests for the ECMWF mean sea level pressure processor."""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
import xarray as xr

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from models.work_unit import (
    WorkUnit,
)  # noqa: E402  pylint: disable=wrong-import-position
from processors.ecmwf_mslp_processor import (  # noqa: E402  pylint: disable=wrong-import-position
    EcmwfMslpProcessor,
)


def _make_config(tmp_path: Path):
    config = MagicMock()
    config.TMP_DIR = str(tmp_path)
    config.ECMWF_MSLP_SMOOTHING_SIGMA = 1.0
    config.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE = 0.1
    return config


def _make_work_unit(forecast_time: datetime, hour_center: int) -> WorkUnit:
    center_time = forecast_time.replace(hour=forecast_time.hour + hour_center)
    image_id = center_time.strftime("%Y%m%dT%H%MZ")
    payload = {
        "grib_path": "grib/models/ecmwf/mean_sea_level_pressure/20260413T1200Z.grib",
        "forecast_time": forecast_time.isoformat(),
        "center_time": center_time.isoformat(),
        "hour_center": hour_center,
    }
    return WorkUnit.create(
        image_id=image_id,
        source_uri=json.dumps(payload),
        data_source_id="ecmwf_mslp_period",
        processor_id="ecmwf_mslp_processor",
        output_prefix="tiles/models/ecmwf/mean_sea_level_pressure/20260413T1200Z",
        bounds={"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        band_id="ecmwf_mslp",
    )


def _make_clipped_field() -> xr.DataArray:
    """Small synthetic pressure field already in hPa with EPSG:4326 axes."""
    x = np.linspace(-110.0, -30.0, 9)
    y = np.linspace(-60.0, -15.0, 5)
    z = np.full((len(y), len(x)), 1010.0)
    z += np.linspace(-5.0, 5.0, len(x))[None, :]
    return xr.DataArray(z, dims=("y", "x"), coords={"x": x, "y": y})


@pytest.mark.asyncio
async def test_uploads_cog_and_geojson_with_expected_s3_keys(tmp_path):
    """Both COG and GeoJSON must land at the documented S3 paths."""
    config = _make_config(tmp_path)
    forecast_time = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    work_unit = _make_work_unit(forecast_time, hour_center=3)

    mock_s3 = AsyncMock()
    mock_s3.upload_file = AsyncMock(return_value=True)

    with patch(
        "processors.ecmwf_mslp_processor.create_s3_client", return_value=mock_s3
    ):
        processor = EcmwfMslpProcessor(config)

    grib_file = tmp_path / "input.grib"
    grib_file.write_bytes(b"fake grib payload")

    fake_clipped = _make_clipped_field()
    fake_cog = tmp_path / "fake.tif"
    fake_cog.write_bytes(b"cog")
    fake_geojson = tmp_path / "fake.json"
    fake_geojson.write_text("{}")

    processor._load_and_prepare = MagicMock(return_value=fake_clipped)
    processor._generate_outputs = MagicMock(return_value=(fake_cog, fake_geojson))

    await processor.process(str(grib_file), work_unit)

    assert mock_s3.upload_file.await_count == 2
    cog_call, geojson_call = mock_s3.upload_file.await_args_list

    forecast_ts = "20260413T1200Z"
    expected_cog_key = (
        f"cog/models/ecmwf/mean_sea_level_pressure/{forecast_ts}/"
        f"{work_unit.image_id}.tif"
    )
    expected_geojson_key = (
        f"geojson/models/ecmwf/mean_sea_level_pressure/{forecast_ts}/"
        f"{work_unit.image_id}.json"
    )
    assert cog_call.args[0] == expected_cog_key
    assert geojson_call.args[0] == expected_geojson_key


@pytest.mark.asyncio
async def test_load_and_prepare_converts_pa_to_hpa(tmp_path):
    """`_load_and_prepare` must divide the GRIB Pa values by 100 to yield hPa."""
    config = _make_config(tmp_path)
    mock_s3 = AsyncMock()
    with patch(
        "processors.ecmwf_mslp_processor.create_s3_client", return_value=mock_s3
    ):
        processor = EcmwfMslpProcessor(config)

    # Build a fake Dataset with `msl` along (step, latitude, longitude),
    # in Pa, returning the same plane regardless of the requested step.
    lats = np.linspace(-15.0, -60.0, 5)  # decreasing latitude (typical GRIB order)
    lons = np.linspace(-110.0, -30.0, 9)
    steps = [np.timedelta64(h, "h") for h in (3, 6, 9)]
    pa_values = np.full((len(steps), len(lats), len(lons)), 101300.0)  # 1013 hPa
    msl_da = xr.DataArray(
        pa_values,
        dims=("step", "latitude", "longitude"),
        coords={"step": steps, "latitude": lats, "longitude": lons},
        name="msl",
    )
    fake_ds = xr.Dataset({"msl": msl_da})

    bounds = {"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0}

    with patch("xarray.open_dataset", return_value=fake_ds):
        clipped = processor._load_and_prepare(
            grib_path=tmp_path / "input.grib",
            hour_center=3,
            bounds=bounds,
        )

    # All cells must be 1013 hPa (or NaN where reproject created edges).
    finite = clipped.values[np.isfinite(clipped.values)]
    assert finite.size > 0
    np.testing.assert_allclose(finite, 1013.0, atol=1e-6)
    assert clipped.attrs["units"] == "hPa"


@pytest.mark.asyncio
async def test_failed_uploads_do_not_raise(tmp_path):
    """An upload returning False is logged but does not crash the pipeline."""
    config = _make_config(tmp_path)
    forecast_time = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    work_unit = _make_work_unit(forecast_time, hour_center=3)

    mock_s3 = AsyncMock()
    mock_s3.upload_file = AsyncMock(return_value=False)

    with patch(
        "processors.ecmwf_mslp_processor.create_s3_client", return_value=mock_s3
    ):
        processor = EcmwfMslpProcessor(config)

    grib_file = tmp_path / "input.grib"
    grib_file.write_bytes(b"fake")

    processor._load_and_prepare = MagicMock(return_value=_make_clipped_field())
    fake_cog = tmp_path / "c.tif"
    fake_cog.write_bytes(b"")
    fake_geojson = tmp_path / "c.json"
    fake_geojson.write_text("{}")
    processor._generate_outputs = MagicMock(return_value=(fake_cog, fake_geojson))

    await processor.process(str(grib_file), work_unit)
    assert mock_s3.upload_file.await_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
