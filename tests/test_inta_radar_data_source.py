"""Tests for IntaRadarDataSource using a mock IntaRadarFileRepository."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from data_sources.base import DiscoveryConfig
from data_sources.inta_radar import IntaRadarDataSource
from models.radar_config import RadarStationFilter, get_inta_product_config

DBZH = get_inta_product_config("dBZ")
ZDR = get_inta_product_config("ZDR")


def make_repo(header_factory, files, radar_id="PAR", **header_kwargs):
    repo = AsyncMock()
    repo.list_files = AsyncMock(return_value=files)
    repo.read_header = AsyncMock(
        return_value=header_factory(radar_id=radar_id, **header_kwargs)
    )
    repo.download = AsyncMock(side_effect=lambda src, dest: dest.with_suffix(".vol"))
    return repo


def make_discovery_config(existing=None, in_progress=None) -> DiscoveryConfig:
    return DiscoveryConfig(
        current_time=datetime(2026, 5, 21, 15, 10, tzinfo=timezone.utc),
        existing_tilesets=existing or set(),
        in_progress_images=in_progress or set(),
        bounds={},
    )


@pytest.mark.asyncio
async def test_discovers_only_its_own_variable(rainbow_header):
    files = [
        "/data/2026052115100400dBZ.vol",
        "/data/2026052115100400ZDR.vol",
        "/data/2026052115100400KDP.vol",
    ]
    source = IntaRadarDataSource(DBZH, "dBZ", make_repo(rainbow_header, files))
    images = await source.discover_images(make_discovery_config())
    assert [img.image_id for img in images] == ["PAR_dbzh_20260521T151004Z"]


@pytest.mark.asyncio
async def test_uncorrected_variant_is_not_mistaken_for_the_corrected_one(
    rainbow_header,
):
    # dBuZ is the unfiltered reflectivity: a substring match would swallow it.
    files = ["/data/2026052115100400dBuZ.vol"]
    source = IntaRadarDataSource(DBZH, "dBZ", make_repo(rainbow_header, files))
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_station_comes_from_the_header_not_the_filename(rainbow_header):
    # The SMN's sample files carry no radar token at all, so the id can only
    # come from inside the file.
    files = ["/data/2026091501500300dBZ.vol"]
    source = IntaRadarDataSource(
        DBZH, "dBZ", make_repo(rainbow_header, files, radar_id="ANG")
    )
    images = await source.discover_images(make_discovery_config())
    assert [img.image_id for img in images] == ["ANG_dbzh_20260915T015003Z"]


@pytest.mark.asyncio
async def test_short_range_scans_are_filtered_out(rainbow_header):
    # Anguil and Pergamino interleave 120 km volumes with the 240 km ones in the
    # same folder. Publishing both under one product would make the layer's
    # footprint jump between two extents as the user scrubs through time.
    files = ["/data/2026091501442900dBZ.vol"]
    source = IntaRadarDataSource(
        DBZH, "dBZ", make_repo(rainbow_header, files, stop_range="120")
    )
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_image_id_matches_the_sinarame_layout(rainbow_header):
    # The producer dedups INTA against the same tileset layout as SINARAME, so
    # the id has to keep the {radar}_{product}_{timestamp} shape.
    files = ["/data/2026052115100400dBZ.vol"]
    source = IntaRadarDataSource(DBZH, "dBZ", make_repo(rainbow_header, files))
    existing = {"PAR_dbzh_20260521T151004Z"}
    assert await source.discover_images(make_discovery_config(existing=existing)) == []


@pytest.mark.asyncio
async def test_filters_in_progress(rainbow_header):
    files = ["/data/2026052115100400dBZ.vol"]
    source = IntaRadarDataSource(DBZH, "dBZ", make_repo(rainbow_header, files))
    in_progress = {"PAR_dbzh_20260521T151004Z"}
    images = await source.discover_images(
        make_discovery_config(in_progress=in_progress)
    )
    assert images == []


@pytest.mark.asyncio
async def test_unreadable_header_skips_only_that_file(rainbow_header):
    files = ["/data/2026052115100400dBZ.vol", "/data/2026052115200400dBZ.vol"]
    repo = make_repo(rainbow_header, files)
    repo.read_header = AsyncMock(
        side_effect=[b"not a rainbow volume", rainbow_header()]
    )
    source = IntaRadarDataSource(DBZH, "dBZ", repo)
    assert len(await source.discover_images(make_discovery_config())) == 1


@pytest.mark.asyncio
async def test_ignores_foreign_filenames(rainbow_header):
    files = [
        "/data/RMA1_0315_01_DBZH_20260114T170328Z.H5",
        "/data/2026091501472600dBZ.azi",
        "/data/notes.vol",
    ]
    source = IntaRadarDataSource(DBZH, "dBZ", make_repo(rainbow_header, files))
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_station_filter_applies(rainbow_header):
    files = ["/data/2026052115100400dBZ.vol"]
    source = IntaRadarDataSource(
        DBZH,
        "dBZ",
        make_repo(rainbow_header, files, radar_id="PAR"),
        station_filter=RadarStationFilter("whitelist", frozenset({"ANG"})),
    )
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_target_images_caps_per_station(rainbow_header):
    files = [f"/data/202605211{h}100400dBZ.vol" for h in range(5)]
    source = IntaRadarDataSource(
        DBZH, "dBZ", make_repo(rainbow_header, files), target_images=2
    )
    images = await source.discover_images(make_discovery_config())
    assert len(images) == 2
    # Newest first: the cap must keep the most recent scans.
    assert images[0].image_id == "PAR_dbzh_20260521T141004Z"


@pytest.mark.asyncio
async def test_source_and_processor_ids_are_distinct_from_sinarame(rainbow_header):
    source = IntaRadarDataSource(ZDR, "ZDR", make_repo(rainbow_header, []))
    assert source.source_id == "radar_inta_zdr"
    assert source.processor_id == "radar_inta"


@pytest.mark.asyncio
async def test_output_prefix_is_the_inta_namespace(rainbow_header):
    source = IntaRadarDataSource(
        DBZH, "dBZ", make_repo(rainbow_header, ["/d/2026052115100400dBZ.vol"])
    )
    images = await source.discover_images(make_discovery_config())
    assert images[0].output_prefix == "tiles/radar/inta"
