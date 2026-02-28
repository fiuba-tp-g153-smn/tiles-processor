"""Tests for RadarDataSource using a mock RadarFileRepository."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from datetime import datetime, timezone

from data_sources.base import DiscoveryConfig
from data_sources.radar import RadarDataSource
from models.radar_config import RADAR_PRODUCT_CONFIGS

DBZH_CONFIG = RADAR_PRODUCT_CONFIGS["DBZH"]


def make_repo(files: list[str]) -> AsyncMock:
    repo = AsyncMock()
    repo.list_files = AsyncMock(return_value=files)
    repo.download = AsyncMock(side_effect=lambda src, dest: dest.with_suffix(".H5"))
    return repo


def make_discovery_config(existing=None, in_progress=None) -> DiscoveryConfig:
    return DiscoveryConfig(
        current_time=datetime(2026, 1, 14, 17, 0, tzinfo=timezone.utc),
        existing_tilesets=existing or set(),
        in_progress_images=in_progress or set(),
        bounds={},
    )


@pytest.mark.asyncio
async def test_discover_images_returns_matching_files():
    files = [
        "/data/RMA1_0315_01_DBZH_20260114T170000Z.H5",
        "/data/RMA2_0315_01_DBZH_20260114T170500Z.H5",
    ]
    source = RadarDataSource(DBZH_CONFIG, make_repo(files))
    images = await source.discover_images(make_discovery_config())
    assert len(images) == 2
    assert all("DBZH" in img.image_id for img in images)


@pytest.mark.asyncio
async def test_discover_images_filters_wrong_product():
    files = [
        "/data/RMA1_0315_01_DBZH_20260114T170000Z.H5",
        "/data/RMA1_0315_02_VRAD_20260114T170000Z.H5",
    ]
    source = RadarDataSource(DBZH_CONFIG, make_repo(files))
    images = await source.discover_images(make_discovery_config())
    assert len(images) == 1
    assert "DBZH" in images[0].image_id


@pytest.mark.asyncio
async def test_discover_images_filters_already_processed():
    files = ["/data/RMA1_0315_01_DBZH_20260114T170000Z.H5"]
    source = RadarDataSource(DBZH_CONFIG, make_repo(files))
    existing = {"RMA1_DBZH_20260114T170000Z"}
    images = await source.discover_images(make_discovery_config(existing=existing))
    assert images == []


@pytest.mark.asyncio
async def test_discover_images_filters_in_progress():
    files = ["/data/RMA1_0315_01_DBZH_20260114T170000Z.H5"]
    source = RadarDataSource(DBZH_CONFIG, make_repo(files))
    in_progress = {"RMA1_DBZH_20260114T170000Z"}
    images = await source.discover_images(
        make_discovery_config(in_progress=in_progress)
    )
    assert images == []


@pytest.mark.asyncio
async def test_discover_images_empty_repo():
    source = RadarDataSource(DBZH_CONFIG, make_repo([]))
    images = await source.discover_images(make_discovery_config())
    assert images == []


@pytest.mark.asyncio
async def test_discover_images_respects_target_limit():
    # Generate 20 files — should return at most TARGET_IMAGES (14)
    files = [f"/data/RMA1_0315_01_DBZH_20260114T{i:06d}Z.H5" for i in range(20)]
    source = RadarDataSource(DBZH_CONFIG, make_repo(files))
    images = await source.discover_images(make_discovery_config())
    assert len(images) <= RadarDataSource.TARGET_IMAGES


@pytest.mark.asyncio
async def test_download_delegates_to_repository(tmp_path):
    repo = AsyncMock()
    expected = tmp_path / "out.H5"
    repo.download = AsyncMock(return_value=expected)
    source = RadarDataSource(DBZH_CONFIG, repo)
    result = await source.download("/data/file.H5", tmp_path / "out")
    repo.download.assert_called_once_with("/data/file.H5", tmp_path / "out")
    assert result == expected
