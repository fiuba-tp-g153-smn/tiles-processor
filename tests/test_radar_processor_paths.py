"""Tests for radar S3 key layout generation in RadarProcessor."""

import sys
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import numpy as np
import pytest

from exceptions import UnprocessableInputError
from models.work_unit import WorkUnit
from processors.radar_processor import RadarProcessor


def _radar_processor(tmp_path) -> RadarProcessor:
    config = MagicMock()
    config.TMP_DIR = str(tmp_path)
    with patch("processors.radar_processor.create_s3_client", return_value=AsyncMock()):
        return RadarProcessor(config)


def test_read_radar_skips_incompatible_sweep_geometry(tmp_path):
    """pyart's 'changes between sweeps' ValueError → UnprocessableInputError (skip)."""
    processor = _radar_processor(tmp_path)
    fake_pyart = MagicMock()
    fake_pyart.aux_io.read_sinarame_h5.side_effect = ValueError(
        "range start changes between sweeps"
    )

    with patch.dict(sys.modules, {"pyart": fake_pyart}):
        with pytest.raises(UnprocessableInputError, match="sweep range geometry"):
            processor._read_radar(Path("RMA11_KDP_20260114T170040Z.H5"))


def test_read_radar_reraises_unrelated_valueerror(tmp_path):
    """A different ValueError is a real error — it must NOT be swallowed as a skip."""
    processor = _radar_processor(tmp_path)
    fake_pyart = MagicMock()
    fake_pyart.aux_io.read_sinarame_h5.side_effect = ValueError(
        "totally unrelated boom"
    )

    with patch.dict(sys.modules, {"pyart": fake_pyart}):
        with pytest.raises(ValueError) as excinfo:
            processor._read_radar(Path("RMA11_DBZH_x.H5"))

    assert not isinstance(excinfo.value, UnprocessableInputError)
    assert "unrelated boom" in str(excinfo.value)


@pytest.mark.asyncio
async def test_radar_upload_paths_split_elevation_and_timestamp(tmp_path):
    """Radar tiles and COG keys must use .../elevN/<timestamp>/ layout."""
    config = MagicMock()
    config.TMP_DIR = str(tmp_path)

    mock_s3 = AsyncMock()
    mock_s3.upload_file = AsyncMock(return_value=True)

    with patch("processors.radar_processor.create_s3_client", return_value=mock_s3):
        processor = RadarProcessor(config)

    # Keep the test focused on path construction by mocking heavy processing steps.
    fake_radar = MagicMock()
    fake_radar.nsweeps = 1
    fake_radar.fixed_angle = {"data": [0.5]}

    processor._read_radar = MagicMock(return_value=fake_radar)
    processor._get_field_name = MagicMock(return_value="reflectivity")
    processor._extract_polar_data = MagicMock(
        return_value={
            "data": np.ma.array([[1.0]], mask=[[False]]),
            "ranges": np.array([0.0, 1000.0]),
            "azimuths": np.array([0.0]),
            "radar_lat": -34.0,
            "radar_lon": -58.0,
        }
    )
    processor._compute_cartesian_mapping = MagicMock(
        return_value=(
            np.array([0], dtype=int),
            np.array([0], dtype=int),
            np.array([False]),
            (-58.1, -57.9, -34.1, -33.9),
            1,
        )
    )
    processor._save_polar_cog = MagicMock()
    processor._polar_to_geotiff_with_mapping = MagicMock()
    processor._generate_tiles = MagicMock()
    processor._upload_tiles = AsyncMock()

    radar_file = tmp_path / "RMA1_0315_01_DBZH_20260114T170328Z.H5"
    radar_file.write_bytes(b"fake")

    work_unit = WorkUnit.create(
        image_id="RMA1_DBZH_20260114T170328Z",
        source_uri=str(radar_file),
        data_source_id="radar_DBZH",
        processor_id="radar",
        output_prefix="tiles/radar",
        bounds={"minx": -70, "miny": -40, "maxx": -50, "maxy": -20},
        band_id="radar_DBZH",
    )

    with patch("processors.radar_processor.get_palette", return_value=MagicMock()):
        await processor.process(str(radar_file), work_unit)

    processor._upload_tiles.assert_awaited_once()
    uploaded_tiles_prefix = processor._upload_tiles.await_args.args[1]
    assert uploaded_tiles_prefix == "tiles/radar/RMA1/DBZH/elev0/20260114T170328Z"

    mock_s3.upload_file.assert_awaited_once()
    uploaded_cog_key = mock_s3.upload_file.await_args.args[0]
    assert uploaded_cog_key == "cog/radar/RMA1/DBZH/elev0/20260114T170328Z.tif"
