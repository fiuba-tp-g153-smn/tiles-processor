"""Tests for IntaRadarDataSource using a mock IntaRadarFileRepository."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from data_sources.base import DiscoveryConfig
from data_sources.inta_radar import IntaRadarDataSource
from models.listing_window import ListingWindow
from models.radar_config import RadarStationFilter, get_inta_scan_spec

DBZH = get_inta_scan_spec("dBZ", ".vol")
ZDR = get_inta_scan_spec("ZDR", ".vol")
DBZH_450KM = get_inta_scan_spec("dBZ", ".azi")


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
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files))
    images = await source.discover_images(make_discovery_config())
    assert [img.image_id for img in images] == ["PAR_dbzh_20260521T151004Z"]


@pytest.mark.asyncio
async def test_uncorrected_variant_is_not_mistaken_for_the_corrected_one(
    rainbow_header,
):
    # dBuZ is the unfiltered reflectivity: a substring match would swallow it.
    files = ["/data/2026052115100400dBuZ.vol"]
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files))
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_station_comes_from_the_header_not_the_filename(rainbow_header):
    # The SMN's sample files carry no radar token at all, so the id can only
    # come from inside the file.
    files = ["/data/2026091501500300dBZ.vol"]
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files, radar_id="ANG"))
    images = await source.discover_images(make_discovery_config())
    assert [img.image_id for img in images] == ["ANG_dbzh_20260915T015003Z"]


@pytest.mark.asyncio
async def test_short_range_scans_are_filtered_out(rainbow_header):
    # Anguil and Pergamino interleave 120 km volumes with the 240 km ones in the
    # same folder. Publishing both under one product would make the layer's
    # footprint jump between two extents as the user scrubs through time.
    files = ["/data/2026091501442900dBZ.vol"]
    source = IntaRadarDataSource(
        DBZH, make_repo(rainbow_header, files, stop_range="120")
    )
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_image_id_matches_the_sinarame_layout(rainbow_header):
    # The producer dedups INTA against the same tileset layout as SINARAME, so
    # the id has to keep the {radar}_{product}_{timestamp} shape.
    files = ["/data/2026052115100400dBZ.vol"]
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files))
    existing = {"PAR_dbzh_20260521T151004Z"}
    assert await source.discover_images(make_discovery_config(existing=existing)) == []


@pytest.mark.asyncio
async def test_filters_in_progress(rainbow_header):
    files = ["/data/2026052115100400dBZ.vol"]
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files))
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
    source = IntaRadarDataSource(DBZH, repo)
    assert len(await source.discover_images(make_discovery_config())) == 1


@pytest.mark.asyncio
async def test_ignores_foreign_filenames(rainbow_header):
    files = [
        "/data/RMA1_0315_01_DBZH_20260114T170328Z.H5",
        "/data/2026091501472600dBZ.azi",
        "/data/notes.vol",
    ]
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files))
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_station_filter_applies(rainbow_header):
    files = ["/data/2026052115100400dBZ.vol"]
    source = IntaRadarDataSource(
        DBZH,
        make_repo(rainbow_header, files, radar_id="PAR"),
        station_filter=RadarStationFilter("whitelist", frozenset({"ANG"})),
    )
    assert await source.discover_images(make_discovery_config()) == []


@pytest.mark.asyncio
async def test_target_images_caps_per_station(rainbow_header):
    files = [f"/data/202605211{h}100400dBZ.vol" for h in range(5)]
    source = IntaRadarDataSource(
        DBZH, make_repo(rainbow_header, files), target_images=2
    )
    images = await source.discover_images(make_discovery_config())
    assert len(images) == 2
    # Newest first: the cap must keep the most recent scans.
    assert images[0].image_id == "PAR_dbzh_20260521T141004Z"


@pytest.mark.asyncio
async def test_source_and_processor_ids_are_distinct_from_sinarame(rainbow_header):
    source = IntaRadarDataSource(ZDR, make_repo(rainbow_header, []))
    assert source.source_id == "radar_inta_zdr"
    assert source.processor_id == "radar_inta"


@pytest.mark.asyncio
async def test_output_prefix_is_the_inta_namespace(rainbow_header):
    source = IntaRadarDataSource(
        DBZH, make_repo(rainbow_header, ["/d/2026052115100400dBZ.vol"])
    )
    images = await source.discover_images(make_discovery_config())
    assert images[0].output_prefix == "tiles/radar/inta"


@pytest.mark.asyncio
async def test_scans_older_than_max_age_are_skipped(rainbow_header):
    # Discovery runs at 15:10; the default window is 150 min (back to 12:40).
    files = [
        "/data/2026052112400000dBZ.vol",
        "/data/2026052112395900dBZ.vol",
        "/data/2026052015100400dBZ.vol",
    ]
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files))
    images = await source.discover_images(make_discovery_config())
    assert [img.image_id for img in images] == ["PAR_dbzh_20260521T124000Z"]


@pytest.mark.asyncio
async def test_old_scans_never_have_their_header_read(rainbow_header):
    # The age cut is what keeps an S3 backlog from costing a GET per tick.
    files = ["/data/2026052115000400dBZ.vol", "/data/2026052109000400dBZ.vol"]
    repo = make_repo(rainbow_header, files)
    source = IntaRadarDataSource(DBZH, repo)
    await source.discover_images(make_discovery_config())
    repo.read_header.assert_awaited_once_with("/data/2026052115000400dBZ.vol")


@pytest.mark.asyncio
async def test_max_age_is_configurable(rainbow_header):
    files = ["/data/2026052115000400dBZ.vol", "/data/2026052114300400dBZ.vol"]
    source = IntaRadarDataSource(
        DBZH, make_repo(rainbow_header, files), max_age_minutes=20
    )
    images = await source.discover_images(make_discovery_config())
    assert [img.image_id for img in images] == ["PAR_dbzh_20260521T150004Z"]


@pytest.mark.asyncio
async def test_impossible_date_skips_only_that_file(rainbow_header):
    files = ["/data/2026132115000400dBZ.vol", "/data/2026052115000400dBZ.vol"]
    source = IntaRadarDataSource(DBZH, make_repo(rainbow_header, files))
    images = await source.discover_images(make_discovery_config())
    assert [img.image_id for img in images] == ["PAR_dbzh_20260521T150004Z"]


@pytest.mark.asyncio
async def test_asks_repository_for_the_max_age_window(rainbow_header):
    repo = make_repo(rainbow_header, [])
    source = IntaRadarDataSource(DBZH, repo, max_age_minutes=20)
    await source.discover_images(make_discovery_config())
    repo.list_files.assert_awaited_once_with(
        ListingWindow(
            datetime(2026, 5, 21, 14, 50, tzinfo=timezone.utc),
            datetime(2026, 5, 21, 15, 10, tzinfo=timezone.utc),
        )
    )


@pytest.mark.asyncio
async def test_old_scans_never_get_a_header_read(rainbow_header):
    """Each header read is a ranged GET in S3 mode; history must not pay one."""
    files = ["/data/2026052109000400dBZ.vol", "/data/2026052115000400dBZ.vol"]
    repo = make_repo(rainbow_header, files)
    source = IntaRadarDataSource(DBZH, repo)
    await source.discover_images(make_discovery_config())
    repo.read_header.assert_awaited_once_with("/data/2026052115000400dBZ.vol")


def make_mixed_repo(header_factory, headers_by_uri):
    """Repo whose files each carry their own header (range, station)."""
    repo = AsyncMock()
    repo.list_files = AsyncMock(return_value=sorted(headers_by_uri))
    repo.read_header = AsyncMock(
        side_effect=lambda uri: header_factory(**headers_by_uri[uri])
    )
    return repo


MIXED_FOLDER = {
    "/data/ANG/2026091501000300dBZ.vol": {"radar_id": "ANG", "stop_range": "240"},
    "/data/ANG/2026091501072200dBZ.azi": {"radar_id": "ANG", "stop_range": "480"},
    "/data/PER/2026091501074200dBZ.azi": {"radar_id": "PER", "stop_range": "400"},
    "/data/PAR/2026091501071000dBZ.azi": {"radar_id": "PAR", "stop_range": "480"},
}


def mixed_discovery_config() -> DiscoveryConfig:
    return DiscoveryConfig(
        current_time=datetime(2026, 9, 15, 1, 30, tzinfo=timezone.utc),
        existing_tilesets=set(),
        in_progress_images=set(),
        bounds={},
    )


@pytest.mark.asyncio
async def test_long_range_source_discovers_only_azi_ppis(rainbow_header):
    source = IntaRadarDataSource(
        DBZH_450KM, make_mixed_repo(rainbow_header, MIXED_FOLDER)
    )
    images = await source.discover_images(mixed_discovery_config())
    # No station is special-cased: every INTA radar publishes its .azi PPIs.
    assert sorted(img.image_id for img in images) == [
        "ANG_dbzh-450km_20260915T010722Z",
        "PAR_dbzh-450km_20260915T010710Z",
        "PER_dbzh-450km_20260915T010742Z",
    ]
    assert {img.output_prefix for img in images} == {"tiles/radar/inta"}


@pytest.mark.asyncio
async def test_volume_source_never_picks_up_azi_ppis(rainbow_header):
    # Both share the dBZ token: an .azi published as dbzh would make the
    # layer's footprint jump between 240 and 480 km.
    source = IntaRadarDataSource(DBZH, make_mixed_repo(rainbow_header, MIXED_FOLDER))
    images = await source.discover_images(mixed_discovery_config())
    assert [img.image_id for img in images] == ["ANG_dbzh_20260915T010003Z"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_range", ["240", "120"])
async def test_azi_not_beyond_240_km_is_not_long_range(rainbow_header, stop_range):
    files = {"/data/2026091501072200dBZ.azi": {"stop_range": stop_range}}
    source = IntaRadarDataSource(DBZH_450KM, make_mixed_repo(rainbow_header, files))
    assert await source.discover_images(mixed_discovery_config()) == []


@pytest.mark.asyncio
async def test_long_range_volume_is_not_published_as_dbzh(rainbow_header):
    files = {"/data/2026091501000300dBZ.vol": {"stop_range": "480"}}
    source = IntaRadarDataSource(DBZH, make_mixed_repo(rainbow_header, files))
    assert await source.discover_images(mixed_discovery_config()) == []


def test_long_range_source_id_matches_the_settings_toggle(rainbow_header):
    # The producer enables a source by stripping "radar_inta_" off its id.
    source = IntaRadarDataSource(DBZH_450KM, make_repo(rainbow_header, []))
    assert source.source_id == "radar_inta_dbzh-450km"
    assert source.product_config.product_id == "dbzh-450km"
