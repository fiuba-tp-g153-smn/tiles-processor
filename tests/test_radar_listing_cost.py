"""Per-tick S3 listing cost of SINARAME discovery against a fake bucket.

The real ``S3Client`` runs on top of a fake ``list_objects_v2`` paginator that
pages like S3 (1000 entries per page, CommonPrefixes with a delimiter) and counts
the pages it serves. One tick = every SINARAME product source discovering once,
as the producer does. Run with ``-s`` to print the comparison table.
"""

import bisect
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from clients.s3_client import S3Client
from data_sources.base import DiscoveryConfig
from data_sources.radar import RadarDataSource
from data_sources.radar_repository import RadarFileRepository, S3RadarFileRepository
from models.listing_window import ListingWindow
from models.radar_config import RADAR_PRODUCT_CONFIGS

RADARS = [f"RMA{n}" for n in range(1, 21)]
SCAN_EVERY = timedelta(minutes=5)
NOW = datetime(2026, 10, 25, 14, 0, tzinfo=timezone.utc)
PAGE_SIZE = 1000

LAYOUTS = {
    # name: (key builder, key template that matches it)
    "per-radar folder": (lambda r, t, f: f"radar/{r}/{f}", None),
    "per-radar/day": (
        lambda r, t, f: f"radar/{r}/{t:%Y%m%d}/{f}",
        "*/{yyyy}{mm}{dd}/",
    ),
    "day/hour": (
        lambda r, t, f: f"radar/{t:%Y/%m/%d/%H}/{f}",
        "{yyyy}/{mm}/{dd}/{HH}/",
    ),
}


class FakeS3:
    """Sorted keys behind a ``list_objects_v2`` paginator that counts pages."""

    def __init__(self, keys: list[str]) -> None:
        self._keys = sorted(keys)
        self.pages = 0
        self.entries = 0

    def get_paginator(self, operation: str) -> "FakeS3":
        assert operation == "list_objects_v2"
        return self

    async def paginate(self, Bucket, Prefix="", Delimiter=None):  # noqa: N803
        entries = self._entries(Prefix, Delimiter)
        for start in range(0, max(len(entries), 1), PAGE_SIZE):
            chunk = entries[start : start + PAGE_SIZE]
            self.pages += 1
            self.entries += len(chunk)
            yield {
                "Contents": [{"Key": k} for kind, k in chunk if kind == "key"],
                "CommonPrefixes": [{"Prefix": p} for kind, p in chunk if kind == "cp"],
            }

    def _entries(self, prefix: str, delimiter: str | None) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        index = bisect.bisect_left(self._keys, prefix)
        while index < len(self._keys) and self._keys[index].startswith(prefix):
            rest = self._keys[index][len(prefix) :]
            if delimiter and delimiter in rest:
                common = prefix + rest.split(delimiter)[0] + delimiter
                if not out or out[-1] != ("cp", common):
                    out.append(("cp", common))
            else:
                out.append(("key", self._keys[index]))
            index += 1
        return out


class WindowIgnoringRepository(RadarFileRepository):
    """Reproduces the behaviour before shared listings: a full LIST per call."""

    def __init__(self, inner: S3RadarFileRepository) -> None:
        self._inner = inner

    async def list_files(self, window: ListingWindow | None = None) -> list[str]:
        return await self._inner.list_files()

    async def download(self, source_uri, dest_path):
        raise NotImplementedError


def build_bucket(layout: str, days: int, now: datetime) -> FakeS3:
    """``days`` of scans every 5 min for every radar and product, up to ``now``."""
    build_key, _ = LAYOUTS[layout]
    keys = []
    moment = now - timedelta(days=days) + SCAN_EVERY
    while moment <= now:
        stamp = f"{moment:%Y%m%dT%H%M%SZ}"
        for radar in RADARS:
            for cfg in RADAR_PRODUCT_CONFIGS.values():
                name = f"{radar}_0315_{cfg.subvolume}_{cfg.variable}_{stamp}.H5"
                keys.append(build_key(radar, moment, name))
        moment += SCAN_EVERY
    return FakeS3(keys)


def s3_client_over(fake: FakeS3) -> S3Client:
    client = S3Client(bucket_name="radar-input")
    client._get_client = AsyncMock(
        return_value=fake
    )  # pylint: disable=protected-access
    return client


async def run_tick(repository: RadarFileRepository, now: datetime) -> set[str]:
    config = DiscoveryConfig(
        current_time=now, existing_tilesets=set(), in_progress_images=set(), bounds={}
    )
    found: set[str] = set()
    for cfg in RADAR_PRODUCT_CONFIGS.values():
        # No per-radar cap, so the whole 150-min window is compared.
        source = RadarDataSource(
            cfg, repository, target_images=1000, max_age_minutes=150
        )
        found.update(img.image_id for img in await source.discover_images(config))
    return found


async def measure(layout: str, days: int, strategy: str, now: datetime = NOW):
    fake = build_bucket(layout, days, now)
    template = LAYOUTS[layout][1] if strategy == "shared + template" else None
    repository: RadarFileRepository = S3RadarFileRepository(
        s3_client_over(fake), prefix="radar/", key_template=template
    )
    if strategy == "before":
        repository = WindowIgnoringRepository(repository)
    found = await run_tick(repository, now)
    return fake.pages, fake.entries, found


@pytest.mark.asyncio
@pytest.mark.parametrize("layout", list(LAYOUTS))
async def test_listing_cost_per_tick(layout, days=1):
    before = await measure(layout, days, "before")
    shared = await measure(layout, days, "shared")
    template = await measure(layout, days, "shared + template")

    for name, (pages, entries, found) in {
        "before": before,
        "shared": shared,
        "shared + template": template,
    }.items():
        print(
            f"\n{layout:>16} | {days}d history | {name:<17} | "
            f"{pages:>4} LIST pages | {entries:>7} entries | {len(found)} images"
        )

    products = len(RADAR_PRODUCT_CONFIGS)
    assert before[0] == products * shared[0]
    if LAYOUTS[layout][1] is not None:
        assert template[1] < shared[1]
    # Every strategy publishes exactly the same work: 11:30..14:00 is 31 scans.
    assert before[2] == shared[2] == template[2]
    assert len(before[2]) == len(RADARS) * products * 31


@pytest.mark.asyncio
@pytest.mark.parametrize("layout", ["per-radar/day", "day/hour"])
async def test_template_cost_does_not_grow_with_history(layout):
    one_day = await measure(layout, 1, "shared + template")
    three_days = await measure(layout, 3, "shared + template")
    shared_three_days = await measure(layout, 3, "shared")

    print(
        f"\n{layout:>16} | 3d history | shared {shared_three_days[0]} pages, "
        f"shared + template {three_days[0]} pages"
    )
    assert three_days[:2] == one_day[:2]
    assert three_days[2] == shared_three_days[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("layout", ["per-radar/day", "day/hour"])
async def test_template_keeps_scans_across_midnight(layout):
    just_after_midnight = datetime(2026, 10, 25, 1, 0, tzinfo=timezone.utc)

    shared = await measure(layout, 1, "shared", just_after_midnight)
    template = await measure(layout, 1, "shared + template", just_after_midnight)

    assert template[2] == shared[2]
    assert any("20261024T23" in image_id for image_id in template[2])
