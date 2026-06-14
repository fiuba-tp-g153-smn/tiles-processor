"""Tests for WrfDataSource using a mock WrfFileRepository.

Mirrors tests/test_radar_data_source.py: the discovery cap is count-based
(newest TARGET_RUNS init_tags) over the post-dedup candidate set, so older runs
drain backward over successive cycles instead of being permanently ignored.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from data_sources.base import DiscoveryConfig
from data_sources.wrf import WrfDataSource
from models.wrf_config import WRF_PRODUCT_CONFIGS, parse_wrf_filename

COLMAX_CONFIG = WRF_PRODUCT_CONFIGS["Colmax"]  # skip_f000=False (F000 kept)
PRECIP_CONFIG = WRF_PRODUCT_CONFIGS["Precipitacion1h"]  # skip_f000=True (F000 dropped)

# init_tags in chronological (== lexicographic) order, oldest first.
RUNS_OLD_TO_NEW = [
    "20260530_000000",
    "20260530_120000",
    "20260531_000000",
    "20260531_120000",
    "20260601_000000",
]


def wrf_file(init_tag: str, fnum: int) -> str:
    """Build a FIELD2D source URI: WRF_ARG4K.FCST_L0_FIELD2D.01H.<INIT>.F<NNN>.M000.nc."""
    return f"/data/WRF_ARG4K.FCST_L0_FIELD2D.01H.{init_tag}.F{fnum:03d}.M000.nc"


def image_id(product_id: str, init_tag: str, fnum: int) -> str:
    """The image_id WrfDataSource derives for a (product, run, step)."""
    return f"{product_id}_{init_tag}_F{fnum:03d}"


def init_tags_of(images) -> set[str]:
    """Set of init_tags present in a discovery result (parsed from source_uri)."""
    return {parse_wrf_filename(Path(img.source_uri).name)["init_tag"] for img in images}


def fnums_of(images) -> set[int]:
    """Set of forecast-step numbers present in a discovery result."""
    return {parse_wrf_filename(Path(img.source_uri).name)["fnum"] for img in images}


def make_repo(files: list[str]) -> AsyncMock:
    repo = AsyncMock()
    repo.list_files = AsyncMock(return_value=files)
    repo.download = AsyncMock(side_effect=lambda src, dest: dest)
    return repo


def make_discovery_config(existing=None, in_progress=None) -> DiscoveryConfig:
    return DiscoveryConfig(
        current_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        existing_tilesets=existing or set(),
        in_progress_images=in_progress or set(),
        bounds={},
    )


@pytest.mark.asyncio
async def test_caps_to_newest_three_runs():
    """5 runs × 3 steps → only the 3 newest runs' steps; the 2 oldest excluded."""
    files = [wrf_file(run, f) for run in RUNS_OLD_TO_NEW for f in range(3)]
    source = WrfDataSource(COLMAX_CONFIG, make_repo(files))

    images = await source.discover_images(make_discovery_config())

    assert init_tags_of(images) == set(RUNS_OLD_TO_NEW[-3:])
    assert len(images) == 3 * 3  # 3 runs × 3 steps
    assert RUNS_OLD_TO_NEW[0] not in init_tags_of(images)
    assert RUNS_OLD_TO_NEW[1] not in init_tags_of(images)


@pytest.mark.asyncio
async def test_dedup_lets_older_runs_drain_backward():
    """Newest 3 runs already processed → the next-oldest runs surface (paced)."""
    files = [wrf_file(run, f) for run in RUNS_OLD_TO_NEW for f in range(3)]
    existing = {
        image_id("Colmax", run, f) for run in RUNS_OLD_TO_NEW[-3:] for f in range(3)
    }
    source = WrfDataSource(COLMAX_CONFIG, make_repo(files))

    images = await source.discover_images(make_discovery_config(existing=existing))

    assert init_tags_of(images) == set(RUNS_OLD_TO_NEW[:2])
    assert len(images) == 2 * 3


@pytest.mark.asyncio
async def test_skip_f000_excludes_init_hour_for_accumulation_product():
    """A skip_f000=True product (pp01H) drops F000 but keeps later steps."""
    files = [wrf_file("20260601_000000", 0), wrf_file("20260601_000000", 1)]
    source = WrfDataSource(PRECIP_CONFIG, make_repo(files))

    images = await source.discover_images(make_discovery_config())

    assert fnums_of(images) == {1}


@pytest.mark.asyncio
async def test_f000_included_for_non_accumulation_product():
    """A skip_f000=False product keeps F000 as the first frame."""
    files = [wrf_file("20260601_000000", 0), wrf_file("20260601_000000", 1)]
    source = WrfDataSource(COLMAX_CONFIG, make_repo(files))

    images = await source.discover_images(make_discovery_config())

    assert fnums_of(images) == {0, 1}


@pytest.mark.asyncio
async def test_in_progress_steps_excluded():
    """Steps already in progress are not re-emitted."""
    files = [wrf_file("20260601_000000", f) for f in range(3)]
    in_progress = {image_id("Colmax", "20260601_000000", 1)}
    source = WrfDataSource(COLMAX_CONFIG, make_repo(files))

    images = await source.discover_images(
        make_discovery_config(in_progress=in_progress)
    )

    assert fnums_of(images) == {0, 2}


@pytest.mark.asyncio
async def test_single_run_returns_all_steps():
    """A single run (< TARGET_RUNS) returns every non-skipped step."""
    files = [wrf_file("20260601_000000", f) for f in range(5)]
    source = WrfDataSource(COLMAX_CONFIG, make_repo(files))

    images = await source.discover_images(make_discovery_config())

    assert len(images) == 5
    assert init_tags_of(images) == {"20260601_000000"}


@pytest.mark.asyncio
async def test_fewer_than_target_runs_returns_all():
    """With fewer than TARGET_RUNS runs present, all of them pass through."""
    files = [wrf_file(run, f) for run in RUNS_OLD_TO_NEW[:2] for f in range(3)]
    source = WrfDataSource(COLMAX_CONFIG, make_repo(files))

    images = await source.discover_images(make_discovery_config())

    assert init_tags_of(images) == set(RUNS_OLD_TO_NEW[:2])
    assert len(images) == 2 * 3


@pytest.mark.asyncio
async def test_empty_repo_returns_empty():
    source = WrfDataSource(COLMAX_CONFIG, make_repo([]))

    images = await source.discover_images(make_discovery_config())

    assert images == []


@pytest.mark.asyncio
async def test_invalid_filenames_are_skipped():
    """Non-WRF / malformed names are skipped, not raised."""
    files = [
        wrf_file("20260601_000000", 0),
        "/data/not_a_wrf_file.nc",
        "/data/WRF_ARG4K.bad.nc",
    ]
    source = WrfDataSource(COLMAX_CONFIG, make_repo(files))

    images = await source.discover_images(make_discovery_config())

    assert len(images) == 1


@pytest.mark.asyncio
async def test_download_delegates_to_repository(tmp_path):
    repo = AsyncMock()
    expected = tmp_path / "out.nc"
    repo.download = AsyncMock(return_value=expected)
    source = WrfDataSource(COLMAX_CONFIG, repo)

    result = await source.download("/data/file.nc", tmp_path / "out")

    repo.download.assert_called_once_with("/data/file.nc", tmp_path / "out")
    assert result == expected
