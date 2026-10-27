"""Tests for IntaRadarProcessor: scan resolution and the long-range .azi path.

The PyART reads need wradlib (pyart.aux_io.read_rainbow_wrl imports it lazily),
which the Docker test image carries but a bare virtualenv may not; those tests
skip rather than fail without it.
"""

import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from models.work_unit import WorkUnit
from processors.inta_radar_processor import IntaRadarProcessor

INTA_FIXTURES = Path(__file__).parent / "fixtures" / "inta"
ANG_AZI = INTA_FIXTURES / "ANG" / "2026091501072200dBZ.azi"
PER_AZI = INTA_FIXTURES / "PER" / "2026091501074200dBZ.azi"


def _processor(tmp_path, s3_client=None) -> IntaRadarProcessor:
    config = MagicMock()
    config.TMP_DIR = str(tmp_path)
    config.INTA_ZOOM.spec = "4-9"
    with patch(
        "processors.radar_processor.create_s3_client",
        return_value=s3_client or AsyncMock(),
    ):
        return IntaRadarProcessor(config)


def test_azi_resolves_to_the_long_range_product(tmp_path):
    scan = _processor(tmp_path)._resolve_scan(str(ANG_AZI))

    assert scan.radar_id == "ANG"
    assert scan.timestamp == "20260915T010722Z"
    assert scan.product_config.product_id == "dbzh-450km"
    assert scan.product_config.s3_tiles_prefix == "tiles/radar/inta"
    # Same moment as the 240 km volume: palette and PyART field are shared.
    assert scan.variable_id == "DBZH"


def test_vol_with_the_same_variable_stays_dbzh(tmp_path, rainbow_header):
    volume = tmp_path / "2026091501000300dBZ.vol"
    volume.write_bytes(rainbow_header(radar_id="ANG"))

    scan = _processor(tmp_path)._resolve_scan(str(volume))

    assert scan.radar_id == "ANG"
    assert scan.product_config.product_id == "dbzh"


@pytest.mark.parametrize(
    "path,radar_id,max_range_km",
    [(ANG_AZI, "ANG", 478.5), (PER_AZI, "PER", 399.5)],
)
def test_pyart_reads_the_azi_as_a_single_long_range_sweep(
    tmp_path, path, radar_id, max_range_km
):
    pytest.importorskip("wradlib")
    radar = _processor(tmp_path)._read_radar(path)

    assert radar.nsweeps == 1
    assert radar.range["data"].max() / 1000 == pytest.approx(max_range_km)
    assert "reflectivity" in radar.fields
    assert radar_id == _processor(tmp_path)._resolve_scan(str(path)).radar_id


@pytest.mark.asyncio
async def test_azi_is_published_as_elev0_of_dbzh_450km(tmp_path):
    """Read, map and COG for real; only tiling and uploads are mocked.

    Anguil's PPI carries 361 rays (the first azimuth repeats as the antenna
    closes the turn); the nearest-ray mapping must take that in its stride.
    """
    pytest.importorskip("wradlib")
    s3_client = AsyncMock()
    s3_client.upload_file = AsyncMock(return_value=True)
    processor = _processor(tmp_path, s3_client)
    processor._generate_tiles = MagicMock()
    processor._upload_tiles = AsyncMock()
    real_mapping = processor._compute_cartesian_mapping
    mappings = []

    def spy_mapping(*args):
        mappings.append(real_mapping(*args))
        return mappings[-1]

    processor._compute_cartesian_mapping = spy_mapping

    # The worker's copy keeps the .azi extension (see the repository).
    downloaded = tmp_path / "download" / "scan.azi"
    downloaded.parent.mkdir()
    shutil.copy(ANG_AZI, downloaded)
    work_unit = WorkUnit.create(
        image_id="ANG_dbzh-450km_20260915T010722Z",
        source_uri=str(ANG_AZI),
        data_source_id="radar_inta_dbzh-450km",
        processor_id="radar_inta",
        output_prefix="tiles/radar/inta",
        bounds={"minx": -70, "miny": -45, "maxx": -55, "maxy": -30},
        band_id="radar_inta_dbzh-450km",
    )

    await processor.process(str(downloaded), work_unit)

    # One sweep: elevations 1 and 2 are skipped, not attempted.
    assert len(mappings) == 1
    processor._upload_tiles.assert_awaited_once()
    assert processor._upload_tiles.await_args.args[1] == (
        "tiles/radar/inta/ANG/dbzh-450km/elev0/20260915T010722Z"
    )
    s3_client.upload_file.assert_awaited_once()
    assert s3_client.upload_file.await_args.args[0] == (
        "cog/radar/inta/ANG/dbzh-450km/elev0/20260915T010722Z.tif"
    )

    # The grid spans the scan's own range (~478.5 km), not the 240 km volume's.
    _, _, outside_range, bounds, grid_size = mappings[0]
    min_lon, max_lon, min_lat, max_lat = bounds
    assert (max_lat - min_lat) / 2 * 111.0 == pytest.approx(478.5, abs=1.0)
    assert min_lon < -63.99 < max_lon
    assert grid_size == 1000
    assert not outside_range.all()
