"""Tests for GlmFolderDataSource."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from data_sources.base import DiscoveryConfig
from data_sources.glm_folder import GlmFolderDataSource
from data_sources.glm_folder_repository import GlmFolderFileRepository
from models.band_config import BandConfig


GLM_FED_CONFIG = BandConfig(
    band_id="glm_fed",
    file_pattern="CG_GLM-L2-GLMF",
    vmin=0.0,
    vmax=128.0,
    palette_name="GLM_FED_PALETTE",
    s3_tiles_prefix="tiles/glm_fed",
    s3_cog_prefix="cog/glm_fed",
    product_name="Flash_Extent_Density",
)


class StubRepository(GlmFolderFileRepository):
    """In-memory fake whose list/download surface is fully controlled by tests."""

    def __init__(self, files):
        self._files = list(files)
        self.download_calls: list[tuple[list[str], Path]] = []

    async def list_files(self):
        return list(self._files)

    async def download_to_dir(self, source_uris, dest_dir):
        self.download_calls.append((list(source_uris), dest_dir))
        return dest_dir


def _filename(start: datetime, end: datetime) -> str:
    """Build a CG_GLM-L2-GLMF filename whose start/end tokens decode to ``start``/``end``."""

    def _fmt(dt: datetime) -> str:
        doy = dt.timetuple().tm_yday
        decisec = dt.microsecond // 100_000
        return (
            f"{dt.year:04d}"
            f"{doy:03d}"
            f"{dt.hour:02d}"
            f"{dt.minute:02d}"
            f"{dt.second:02d}"
            f"{decisec}"
        )

    return (
        f"/data/glm_h5/CG_GLM-L2-GLMF-M3_G19_s{_fmt(start)}_e{_fmt(end)}"
        f"_c{_fmt(end)}.nc"
    )


def _ten_consecutive_minutes(window_start: datetime) -> list[str]:
    return [
        _filename(
            window_start + timedelta(minutes=i), window_start + timedelta(minutes=i + 1)
        )
        for i in range(10)
    ]


def _discovery_config(current_time: datetime, *, existing=None, in_progress=None):
    return DiscoveryConfig(
        current_time=current_time,
        existing_tilesets=set(existing or ()),
        in_progress_images=set(in_progress or ()),
        bounds={"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
    )


def _make_source(repository, **overrides):
    kwargs = dict(
        band_config=GLM_FED_CONFIG,
        repository=repository,
        accum_minutes=10,
        produce_every_minutes=10,
        safety_lag_seconds=0,
    )
    kwargs.update(overrides)
    return GlmFolderDataSource(**kwargs)


@pytest.mark.asyncio
async def test_emits_one_image_per_complete_window():
    window_start = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc)
    repo = StubRepository(_ten_consecutive_minutes(window_start))
    source = _make_source(repo)

    images = await source.discover_images(
        _discovery_config(current_time=window_start + timedelta(minutes=11))
    )

    assert len(images) == 1
    info = images[0]
    assert info.processor_id == "glm_fed"
    assert info.data_source_id == "glm_folder"
    assert info.output_prefix == "tiles/glm_fed"
    assert info.image_id == "20260611400000"

    manifest = json.loads(info.source_uri)
    assert manifest["window_start"] == window_start.isoformat()
    assert len(manifest["files"]) == 10
    assert manifest["files"] == sorted(manifest["files"])


@pytest.mark.asyncio
async def test_skips_incomplete_window():
    window_start = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc)
    only_seven = _ten_consecutive_minutes(window_start)[:7]
    source = _make_source(StubRepository(only_seven))

    images = await source.discover_images(
        _discovery_config(current_time=window_start + timedelta(minutes=11))
    )

    assert images == []


@pytest.mark.asyncio
async def test_skips_window_whose_end_is_in_the_future():
    window_start = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc)
    repo = StubRepository(_ten_consecutive_minutes(window_start))
    source = _make_source(repo, safety_lag_seconds=60)

    # current_time is during the window — should not emit
    images = await source.discover_images(
        _discovery_config(current_time=window_start + timedelta(minutes=5))
    )

    assert images == []


@pytest.mark.asyncio
async def test_dedups_against_existing_tilesets():
    window_start = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc)
    repo = StubRepository(_ten_consecutive_minutes(window_start))
    source = _make_source(repo)

    images = await source.discover_images(
        _discovery_config(
            current_time=window_start + timedelta(minutes=11),
            existing={"20260611400000"},
        )
    )

    assert images == []


@pytest.mark.asyncio
async def test_dedups_against_in_progress_images():
    window_start = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc)
    repo = StubRepository(_ten_consecutive_minutes(window_start))
    source = _make_source(repo)

    images = await source.discover_images(
        _discovery_config(
            current_time=window_start + timedelta(minutes=11),
            in_progress={"20260611400000"},
        )
    )

    assert images == []


@pytest.mark.asyncio
async def test_overlapping_windows_with_short_produce_period():
    """5-min accumulation every 2 min → file at 14:03 belongs to 14:00 AND 14:02."""
    files: list[str] = []
    for minute in range(0, 15):
        start = datetime(2026, 3, 2, 14, minute, tzinfo=timezone.utc)
        files.append(_filename(start, start + timedelta(minutes=1)))

    source = _make_source(
        StubRepository(files),
        accum_minutes=5,
        produce_every_minutes=2,
    )

    images = await source.discover_images(
        _discovery_config(
            current_time=datetime(2026, 3, 2, 14, 30, tzinfo=timezone.utc)
        )
    )

    starts = {json.loads(info.source_uri)["window_start"] for info in images}
    # We should at minimum see the 14:00, 14:02, 14:04, 14:06, 14:08, 14:10 anchors
    # because each has 5 minute-files covering [anchor, anchor+5).
    assert "2026-03-02T14:00:00+00:00" in starts
    assert "2026-03-02T14:02:00+00:00" in starts


@pytest.mark.asyncio
async def test_download_parses_manifest_and_delegates_to_repository(tmp_path):
    window_start = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc)
    files = _ten_consecutive_minutes(window_start)
    repo = StubRepository(files)
    source = _make_source(repo)

    manifest = json.dumps({"window_start": window_start.isoformat(), "files": files})
    dest = tmp_path / "work_dir"
    result = await source.download(manifest, dest)

    assert result == dest
    assert len(repo.download_calls) == 1
    called_files, called_dest = repo.download_calls[0]
    assert called_files == files
    assert called_dest == dest


def test_constructor_validates_periods():
    repo = StubRepository([])
    with pytest.raises(ValueError, match="accum_minutes"):
        GlmFolderDataSource(
            GLM_FED_CONFIG, repo, accum_minutes=0, produce_every_minutes=10
        )
    with pytest.raises(ValueError, match="produce_every_minutes"):
        GlmFolderDataSource(
            GLM_FED_CONFIG, repo, accum_minutes=10, produce_every_minutes=0
        )
