"""Tests for S3WindowedKeyLister and the S3 radar repositories that use it."""

import asyncio
import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

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
    lister = S3WindowedKeyLister(s3_client, "radar/", key_suffix=".h5")

    first = await lister.list_keys(TICK)
    second = await lister.list_keys(TICK)

    assert first == second == ["radar/a.H5"]
    s3_client.list_files.assert_awaited_once_with("radar/", file_pattern="")


@pytest.mark.asyncio
async def test_new_window_lists_again():
    s3_client = make_s3()
    lister = S3WindowedKeyLister(s3_client, "radar/", key_suffix=".h5")

    await lister.list_keys(TICK)
    await lister.list_keys(NEXT_TICK)

    assert s3_client.list_files.await_count == 2


@pytest.mark.asyncio
async def test_no_window_is_never_cached():
    s3_client = make_s3()
    lister = S3WindowedKeyLister(s3_client, "radar/", key_suffix=".h5")

    await lister.list_keys()
    await lister.list_keys()

    assert s3_client.list_files.await_count == 2


@pytest.mark.asyncio
async def test_concurrent_callers_share_one_listing():
    s3_client = make_s3()
    lister = S3WindowedKeyLister(s3_client, "radar/", key_suffix=".h5")

    await asyncio.gather(*(lister.list_keys(TICK) for _ in range(6)))

    s3_client.list_files.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_listing_is_retried_by_the_next_caller():
    s3_client = make_s3()
    s3_client.list_files.side_effect = [OSError("gateway down"), ["radar/a.H5"]]
    lister = S3WindowedKeyLister(s3_client, "radar/", key_suffix=".h5")

    with pytest.raises(OSError):
        await lister.list_keys(TICK)
    assert await lister.list_keys(TICK) == ["radar/a.H5"]


@pytest.mark.asyncio
async def test_callers_cannot_mutate_the_shared_listing():
    lister = S3WindowedKeyLister(make_s3(), "radar/", key_suffix=".h5")

    (await lister.list_keys(TICK)).clear()

    assert await lister.list_keys(TICK) == ["radar/a.H5"]


@pytest.mark.asyncio
async def test_template_lists_only_the_window_partitions():
    s3_client = make_s3()
    s3_client.list_files.side_effect = lambda prefix, file_pattern: [f"{prefix}x.H5"]
    lister = S3WindowedKeyLister(
        s3_client, "radar/", "{yyyy}/{mm}/{dd}/{HH}/", key_suffix=".h5"
    )

    keys = await lister.list_keys(TICK)

    assert keys == [f"radar/2026/10/25/{hour}/x.H5" for hour in (11, 12, 13, 14)]


@pytest.mark.asyncio
async def test_wildcard_segment_expands_to_existing_folders():
    s3_client = make_s3()
    s3_client.list_prefixes.return_value = ["radar/RMA1/", "radar/RMA2/"]
    s3_client.list_files.side_effect = lambda prefix, file_pattern: [f"{prefix}x.H5"]
    lister = S3WindowedKeyLister(
        s3_client, "radar/", "*/{yyyy}{mm}{dd}/", key_suffix=".h5"
    )

    keys = await lister.list_keys(TICK)

    s3_client.list_prefixes.assert_awaited_once_with("radar/", "/")
    assert keys == ["radar/RMA1/20261025/x.H5", "radar/RMA2/20261025/x.H5"]


@pytest.mark.asyncio
async def test_wildcard_folders_are_listed_once_across_days():
    s3_client = make_s3()
    s3_client.list_prefixes.return_value = ["RMA1/"]
    s3_client.list_files.side_effect = lambda prefix, file_pattern: [f"{prefix}x.H5"]
    lister = S3WindowedKeyLister(s3_client, "", "*/{yyyy}{mm}{dd}/", key_suffix=".h5")
    across_midnight = ListingWindow(
        datetime(2026, 10, 24, 23, 0, tzinfo=timezone.utc),
        datetime(2026, 10, 25, 1, 0, tzinfo=timezone.utc),
    )

    keys = await lister.list_keys(across_midnight)

    s3_client.list_prefixes.assert_awaited_once_with("", "/")
    assert keys == ["RMA1/20261024/x.H5", "RMA1/20261025/x.H5"]


TEMPLATE = "{yyyy}/{mm}/{dd}/{HH}/"
FULL_LISTING = ["radar/old/RMA1_a.H5", "radar/RMA1_b.h5"]


def template_s3(partition_keys, full_keys=None) -> AsyncMock:
    """Partition prefixes return ``partition_keys``; the root returns the full listing."""
    s3_client = make_s3()
    s3_client.list_files.side_effect = lambda prefix, file_pattern: (
        list(full_keys or FULL_LISTING) if prefix == "radar/" else list(partition_keys)
    )
    return s3_client


def listed_prefixes(s3_client: AsyncMock) -> list[str]:
    return [c.args[0] for c in s3_client.list_files.await_args_list]


@pytest.mark.asyncio
async def test_empty_template_listing_falls_back_to_whole_prefix():
    s3_client = template_s3([])
    lister = S3WindowedKeyLister(s3_client, "radar/", TEMPLATE, key_suffix=".h5")

    keys = await lister.list_keys(TICK)

    assert keys == sorted(FULL_LISTING)
    assert listed_prefixes(s3_client)[-1] == "radar/"
    assert len(listed_prefixes(s3_client)) == 5  # 4 hour folders, then the root


@pytest.mark.asyncio
async def test_fallback_listing_is_shared_for_the_window():
    s3_client = template_s3([])
    lister = S3WindowedKeyLister(s3_client, "radar/", TEMPLATE, key_suffix=".h5")

    await lister.list_keys(TICK)
    calls = s3_client.list_files.await_count
    assert await lister.list_keys(TICK) == sorted(FULL_LISTING)

    assert s3_client.list_files.await_count == calls


@pytest.mark.asyncio
async def test_template_with_only_other_file_types_falls_back():
    """INTA folders also hold rendered .png previews; those are not inputs."""
    s3_client = template_s3(["inta/x.png"], full_keys=["radar/2026102514000400dBZ.vol"])
    lister = S3WindowedKeyLister(s3_client, "radar/", TEMPLATE, key_suffix=".vol")

    assert await lister.list_keys(TICK) == ["radar/2026102514000400dBZ.vol"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2"),
        EndpointConnectionError(endpoint_url="http://s3.smn"),
        OSError("connection reset"),
    ],
)
async def test_template_listing_error_falls_back_to_whole_prefix(error):
    s3_client = template_s3([])
    s3_client.list_prefixes.side_effect = error
    lister = S3WindowedKeyLister(
        s3_client, "radar/", "*/{yyyy}{mm}{dd}/", key_suffix=".h5"
    )

    assert await lister.list_keys(TICK) == sorted(FULL_LISTING)
    assert listed_prefixes(s3_client) == ["radar/"]


@pytest.mark.asyncio
async def test_failed_fallback_raises_and_the_next_caller_retries():
    s3_client = make_s3()
    s3_client.list_prefixes.side_effect = OSError("gateway down")
    s3_client.list_files.side_effect = OSError("gateway down")
    lister = S3WindowedKeyLister(
        s3_client, "radar/", "*/{yyyy}{mm}{dd}/", key_suffix=".h5"
    )

    with pytest.raises(OSError):
        await lister.list_keys(TICK)
    with pytest.raises(OSError):
        await lister.list_keys(TICK)

    assert s3_client.list_prefixes.await_count == 2
    assert s3_client.list_files.await_count == 2


@pytest.mark.asyncio
async def test_template_with_files_never_lists_the_whole_prefix():
    s3_client = template_s3(["radar/2026/10/25/13/RMA1_c.H5"])
    lister = S3WindowedKeyLister(s3_client, "radar/", TEMPLATE, key_suffix=".h5")

    await lister.list_keys(TICK)

    assert "radar/" not in listed_prefixes(s3_client)


@pytest.mark.asyncio
async def test_fallback_is_logged_as_a_warning(caplog):
    lister = S3WindowedKeyLister(template_s3([]), "radar/", TEMPLATE, key_suffix=".h5")

    with caplog.at_level(logging.WARNING, logger="data_sources.s3_windowed_lister"):
        await lister.list_keys(TICK)

    assert TEMPLATE in caplog.text
    assert "found no .h5 files" in caplog.text
    assert "listing the whole prefix instead" in caplog.text


@pytest.mark.asyncio
async def test_cancellation_is_not_turned_into_a_fallback():
    """The source timeout cancels discovery; that must not start a full listing."""
    s3_client = template_s3([])
    s3_client.list_files.side_effect = asyncio.CancelledError
    lister = S3WindowedKeyLister(s3_client, "radar/", TEMPLATE, key_suffix=".h5")

    with pytest.raises(asyncio.CancelledError):
        await lister.list_keys(TICK)

    assert "radar/" not in listed_prefixes(s3_client)


@pytest.mark.asyncio
async def test_sinarame_repository_shares_listing_and_keeps_h5_only():
    s3_client = make_s3(["radar/b.H5", "radar/notes.txt", "radar/a.h5"])
    repo = S3RadarFileRepository(s3_client, prefix="radar/")

    for _ in range(6):
        files = await repo.list_files(TICK)

    assert files == ["radar/a.h5", "radar/b.H5"]
    s3_client.list_files.assert_awaited_once()


@pytest.mark.asyncio
async def test_inta_repository_shares_listing_and_keeps_vol_and_azi():
    s3_client = make_s3(
        ["inta/x.png", "inta/2026102514000400dBZ.vol", "inta/2026102514072200dBZ.azi"]
    )
    repo = S3IntaRadarFileRepository(s3_client, prefix="inta/")

    for _ in range(5):
        files = await repo.list_files(TICK)

    assert files == ["inta/2026102514000400dBZ.vol", "inta/2026102514072200dBZ.azi"]
    s3_client.list_files.assert_awaited_once()


@pytest.mark.asyncio
async def test_several_suffixes_are_kept_in_any_case():
    s3_client = make_s3(["inta/a.vol", "inta/b.AZI", "inta/c.png", "inta/d.VOL"])
    lister = S3WindowedKeyLister(s3_client, "inta/", key_suffix=(".vol", ".azi"))

    assert await lister.list_keys(None) == ["inta/a.vol", "inta/b.AZI", "inta/d.VOL"]
