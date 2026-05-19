"""Tests for the folder-based GlmFedProcessor (CG_GLM-L2-GLMF inputs)."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
import rioxarray  # noqa: F401  # registers .rio accessor
import xarray as xr

from models.work_unit import WorkUnit
from processors.glm_fed_processor import GlmFedProcessor


SMALL_BOUNDS = {"minx": -80.0, "maxx": -78.0, "miny": -50.0, "maxy": -48.0}


def _fake_reprojected_array(bounds: dict) -> xr.DataArray:
    """Return a tiny EPSG:4326 DataArray that survives the colorize/COG/GeoTIFF chain."""
    lon = np.array([bounds["minx"], bounds["maxx"]], dtype=np.float64)
    lat = np.array([bounds["maxy"], bounds["miny"]], dtype=np.float64)
    data = np.array([[10.0, 50.0], [np.nan, 100.0]], dtype=np.float32)
    da = xr.DataArray(
        data,
        dims=("y", "x"),
        coords={"x": lon, "y": lat},
        name="aggregated_glm_var",
    )
    da.rio.write_crs("EPSG:4326", inplace=True)
    da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    return da


def _make_work_unit(window_start: datetime) -> WorkUnit:
    manifest = json.dumps(
        {
            "window_start": window_start.isoformat(),
            "files": ["/data/glm_h5/file1.nc", "/data/glm_h5/file2.nc"],
        }
    )
    return WorkUnit.create(
        image_id="20260611400000",
        source_uri=manifest,
        data_source_id="glm_folder",
        processor_id="glm_fed",
        output_prefix="tiles/glm_fed",
        bounds=SMALL_BOUNDS,
        band_id="glm_folder_fed",
    )


def _make_config(tmp_path: Path, *, enable_toe: bool, enable_mfa: bool) -> MagicMock:
    config = MagicMock()
    config.TMP_DIR = str(tmp_path / "proc")
    config.ENABLE_GLM_TOE = enable_toe
    config.ENABLE_GLM_MFA = enable_mfa
    config.GLM_ACCUM_MINUTES = 10
    config.GLM_RESOLUTION_DEG = 1.0
    return config


def _make_processor(config: MagicMock) -> GlmFedProcessor:
    mock_s3 = AsyncMock()
    mock_s3.upload_directory = AsyncMock()
    mock_s3.ensure_bucket_exists = AsyncMock()
    mock_s3.upload_file = AsyncMock(return_value=True)
    with patch("processors.glm_fed_processor.create_s3_client", return_value=mock_s3):
        processor = GlmFedProcessor(config)
    processor.test_s3 = mock_s3  # expose for assertions
    return processor


def _populate_data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "glm_window"
    data_dir.mkdir()
    (data_dir / "CG_GLM-L2-GLMF-M3_G19_s20260611400000_e20260611401000_c0.nc").touch()
    return data_dir


@pytest.mark.asyncio
async def test_fed_only_uploads_once(tmp_path):
    """With TOE/MFA disabled, only FED's tiles+COG should hit S3."""
    config = _make_config(tmp_path, enable_toe=False, enable_mfa=False)
    processor = _make_processor(config)
    data_dir = _populate_data_dir(tmp_path)
    work_unit = _make_work_unit(datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc))

    fake_tiles_dir = tmp_path / "fake_tiles"
    fake_tiles_dir.mkdir()

    with patch(
        "processors.glm_fed_processor.aggregate_glm_window",
        return_value=MagicMock(close=lambda: None),
    ), patch(
        "processors.glm_fed_processor.reproject_to_latlon",
        return_value=_fake_reprojected_array(SMALL_BOUNDS),
    ), patch(
        "processors.glm_fed_processor.run_gdal2tiles",
        return_value=fake_tiles_dir,
    ), patch(
        "processors.glm_fed_processor.fill_missing_tiles",
        return_value=0,
    ):
        await processor.process(str(data_dir), work_unit)

    assert processor.test_s3.upload_directory.await_count == 1
    assert processor.test_s3.upload_file.await_count == 1  # COG


@pytest.mark.asyncio
async def test_toe_and_mfa_enabled_uploads_three_times(tmp_path):
    config = _make_config(tmp_path, enable_toe=True, enable_mfa=True)
    processor = _make_processor(config)
    data_dir = _populate_data_dir(tmp_path)
    work_unit = _make_work_unit(datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc))

    fake_tiles_dir = tmp_path / "fake_tiles"
    fake_tiles_dir.mkdir()

    with patch(
        "processors.glm_fed_processor.aggregate_glm_window",
        return_value=MagicMock(close=lambda: None),
    ), patch(
        "processors.glm_fed_processor.reproject_to_latlon",
        side_effect=lambda *_a, **_kw: _fake_reprojected_array(SMALL_BOUNDS),
    ), patch(
        "processors.glm_fed_processor.run_gdal2tiles",
        return_value=fake_tiles_dir,
    ), patch(
        "processors.glm_fed_processor.fill_missing_tiles",
        return_value=0,
    ):
        await processor.process(str(data_dir), work_unit)

    assert processor.test_s3.upload_directory.await_count == 3
    assert processor.test_s3.upload_file.await_count == 3


@pytest.mark.asyncio
async def test_missing_glm_files_raises(tmp_path):
    config = _make_config(tmp_path, enable_toe=False, enable_mfa=False)
    processor = _make_processor(config)
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    work_unit = _make_work_unit(datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc))

    with pytest.raises(FileNotFoundError, match="CG_GLM-L2-GLMF"):
        await processor.process(str(empty_dir), work_unit)


@pytest.mark.asyncio
async def test_aggregate_called_with_window_from_manifest(tmp_path):
    """The aggregation window must come from the JSON manifest + config.GLM_ACCUM_MINUTES."""
    config = _make_config(tmp_path, enable_toe=False, enable_mfa=False)
    processor = _make_processor(config)
    data_dir = _populate_data_dir(tmp_path)
    window_start = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc)
    work_unit = _make_work_unit(window_start)

    fake_tiles_dir = tmp_path / "fake_tiles"
    fake_tiles_dir.mkdir()

    aggregate_mock = MagicMock(return_value=MagicMock(close=lambda: None))
    with patch(
        "processors.glm_fed_processor.aggregate_glm_window", aggregate_mock
    ), patch(
        "processors.glm_fed_processor.reproject_to_latlon",
        return_value=_fake_reprojected_array(SMALL_BOUNDS),
    ), patch(
        "processors.glm_fed_processor.run_gdal2tiles",
        return_value=fake_tiles_dir,
    ), patch(
        "processors.glm_fed_processor.fill_missing_tiles",
        return_value=0,
    ):
        await processor.process(str(data_dir), work_unit)

    call_args = aggregate_mock.call_args
    files_arg, win_start_arg, win_end_arg, accum_arg = call_args.args
    assert [Path(f).name for f in files_arg] == [
        "CG_GLM-L2-GLMF-M3_G19_s20260611400000_e20260611401000_c0.nc"
    ]
    assert win_start_arg == window_start
    assert win_end_arg == datetime(2026, 3, 2, 14, 10, tzinfo=timezone.utc)
    assert accum_arg == 10


def test_log_clip_preserves_nan_and_takes_log10():
    """_log_clip should clamp to [vmin, vmax] and apply base-10 log; NaN stays NaN."""
    # pylint: disable=import-outside-toplevel,protected-access
    from processors.glm_fed_processor import _log_clip

    da = xr.DataArray(
        np.array([0.5, 1.0, 10.0, 1000.0, np.nan], dtype=np.float32),
        dims=("x",),
    )
    out = _log_clip(da, vmin=1.0, vmax=100.0)
    values = out.values
    assert np.isclose(values[0], 0.0)  # 0.5 clipped to 1 → log10(1) = 0
    assert np.isclose(values[1], 0.0)  # 1 → log10(1) = 0
    assert np.isclose(values[2], 1.0)  # 10 → log10(10) = 1
    assert np.isclose(values[3], 2.0)  # 1000 clipped to 100 → log10(100) = 2
    assert np.isnan(values[4])
