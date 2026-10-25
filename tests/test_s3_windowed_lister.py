"""Tests for S3WindowedKeyLister and the S3 radar repositories that use it."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from data_sources.inta_radar_repository import S3IntaRadarFileRepository
from data_sources.radar_repository import S3RadarFileRepository
from data_sources.s3_windowed_lister import S3WindowedKeyLister
from models.listing_window import ListingWindow

TICK = ListingWindow(
    datetime(2026, 10, 25, 11, 30, tzinfo=timezone.utc),
    datetime(2026, 10, 25, 14, 0, tzinfo=timezone.utc),
)
NEXT_TICK = ListingWindow(
    datetime(2026, 10, 25, 11, 35, tzinfo=timezone.utc),
    datetime(2026, 10, 25, 14, 5, tzinfo=timezone.utc),
)


def make_s3(keys=None) -> AsyncMock:
    s3_client = AsyncMock()
    s3_client.list_files.return_value = keys or ["radar/a.H5"]
    return s3_client


@pytest.mark.asyncio
async def test_same_window_is_listed_once():
    s3_client = make_s3()
    lister = S3WindowedKeyLister(s3_client, "radar/")

    first = await lister.list_keys(TICK)
    second = await lister.list_keys(TICK)

    assert first == second == ["radar/a.H5"]
    s3_client.list_files.assert_awaited_once_with("radar/", file_pattern="")


@pytest.mark.asyncio
async def test_new_window_lists_again():
    s3_client = make_s3()
    lister = S3WindowedKeyLister(s3_client, "radar/")

    await lister.list_keys(TICK)
    await lister.list_keys(NEXT_TICK)

    assert s3_client.list_files.await_count == 2


@pytest.mark.asyncio
async def test_no_window_is_never_cached():
    s3_client = make_s3()
    lister = S3WindowedKeyLister(s3_client, "radar/")

    await lister.list_keys()
    await lister.list_keys()

    assert s3_client.list_files.await_count == 2


@pytest.mark.asyncio
async def test_concurrent_callers_share_one_listing():
    s3_client = make_s3()
    lister = S3WindowedKeyLister(s3_client, "radar/")

    await asyncio.gather(*(lister.list_keys(TICK) for _ in range(6)))

    s3_client.list_files.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_listing_is_retried_by_the_next_caller():
    s3_client = make_s3()
    s3_client.list_files.side_effect = [OSError("gateway down"), ["radar/a.H5"]]
    lister = S3WindowedKeyLister(s3_client, "radar/")

    with pytest.raises(OSError):
        await lister.list_keys(TICK)
    assert await lister.list_keys(TICK) == ["radar/a.H5"]


@pytest.mark.asyncio
async def test_callers_cannot_mutate_the_shared_listing():
    lister = S3WindowedKeyLister(make_s3(), "radar/")

    (await lister.list_keys(TICK)).clear()

    assert await lister.list_keys(TICK) == ["radar/a.H5"]


@pytest.mark.asyncio
async def test_template_lists_only_the_window_partitions():
    s3_client = make_s3()
    s3_client.list_files.side_effect = lambda prefix, file_pattern: [f"{prefix}x.H5"]
    lister = S3WindowedKeyLister(s3_client, "radar/", "{yyyy}/{mm}/{dd}/{HH}/")

    keys = await lister.list_keys(TICK)

    assert keys == [f"radar/2026/10/25/{hour}/x.H5" for hour in (11, 12, 13, 14)]


@pytest.mark.asyncio
async def test_wildcard_segment_expands_to_existing_folders():
    s3_client = make_s3()
    s3_client.list_prefixes.return_value = ["radar/RMA1/", "radar/RMA2/"]
    s3_client.list_files.side_effect = lambda prefix, file_pattern: [f"{prefix}x.H5"]
    lister = S3WindowedKeyLister(s3_client, "radar/", "*/{yyyy}{mm}{dd}/")

    keys = await lister.list_keys(TICK)

    s3_client.list_prefixes.assert_awaited_once_with("radar/", "/")
    assert keys == ["radar/RMA1/20261025/x.H5", "radar/RMA2/20261025/x.H5"]


@pytest.mark.asyncio
async def test_wildcard_folders_are_listed_once_across_days():
    s3_client = make_s3()
    s3_client.list_prefixes.return_value = ["RMA1/"]
    s3_client.list_files.side_effect = lambda prefix, file_pattern: [f"{prefix}x.H5"]
    lister = S3WindowedKeyLister(s3_client, "", "*/{yyyy}{mm}{dd}/")
    across_midnight = ListingWindow(
        datetime(2026, 10, 24, 23, 0, tzinfo=timezone.utc),
        datetime(2026, 10, 25, 1, 0, tzinfo=timezone.utc),
    )

    keys = await lister.list_keys(across_midnight)

    s3_client.list_prefixes.assert_awaited_once_with("", "/")
    assert keys == ["RMA1/20261024/x.H5", "RMA1/20261025/x.H5"]


@pytest.mark.asyncio
async def test_sinarame_repository_shares_listing_and_keeps_h5_only():
    s3_client = make_s3(["radar/b.H5", "radar/notes.txt", "radar/a.h5"])
    repo = S3RadarFileRepository(s3_client, prefix="radar/")

    for _ in range(6):
        files = await repo.list_files(TICK)

    assert files == ["radar/a.h5", "radar/b.H5"]
    s3_client.list_files.assert_awaited_once()


@pytest.mark.asyncio
async def test_inta_repository_shares_listing_and_keeps_vol_only():
    s3_client = make_s3(["inta/x.azi", "inta/2026102514000400dBZ.vol"])
    repo = S3IntaRadarFileRepository(s3_client, prefix="inta/")

    for _ in range(4):
        files = await repo.list_files(TICK)

    assert files == ["inta/2026102514000400dBZ.vol"]
    s3_client.list_files.assert_awaited_once()
