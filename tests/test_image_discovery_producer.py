"""Tests for ImageDiscoveryProducer duplicate prevention."""

import sys
import os
from datetime import datetime, UTC, timedelta
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest

from clients.progress_tracker import ProgressTracker
from data_sources import DataSourceRegistry
from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from models.band_config import BAND_CONFIGS, BandConfig
from producer.image_discovery_producer import ImageDiscoveryProducer
from config import Config


@pytest.fixture
def mock_config():
    config = MagicMock(spec=Config)
    config.S3_TILES_DATA_BUCKET_NAME = "test-bucket"
    config.S3_TILES_DATA_ENDPOINT = "mock:9000"
    config.S3_TILES_DATA_RW_ACCESS_KEY = "user"
    config.S3_TILES_DATA_RW_SECRET_KEY = "pass"
    config.S3_TILES_DATA_SECURE = False
    config.ENABLE_BAND_13 = True
    config.ENABLE_BAND_9 = True
    config.ENABLE_BAND_2 = True
    config.ENABLE_RADAR = False
    config.get_bounds.return_value = {
        "minx": -90.0,
        "miny": -60.0,
        "maxx": -30.0,
        "maxy": -15.0,
    }
    return config


@pytest.fixture
def progress_tracker(tmp_path):
    db_path = tmp_path / "progress_tracker.db"
    return ProgressTracker(db_path, ttl=timedelta(minutes=20))


class FakeDataSource(DataSource):
    """Fake data source that returns a fixed set of images."""

    def __init__(self, band_config: BandConfig, images: list[ImageInfo]):
        self._band_config = band_config
        self._images = images

    @property
    def source_id(self) -> str:
        return f"goes19_abi_{self._band_config.band_id}"

    @property
    def processor_id(self) -> str:
        return f"goes_{self._band_config.band_id}"

    @property
    def band_config(self) -> BandConfig:
        return self._band_config

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        new = []
        for img in self._images:
            stem = Path(img.image_id).stem
            if stem in config.existing_tilesets:
                continue
            if img.image_id in config.in_progress_images:
                continue
            new.append(img)
        return new

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        return dest_path


def _make_images(band_config: BandConfig, count: int) -> list[ImageInfo]:
    """Generate fake ImageInfo list for a band."""
    images = []
    for i in range(count):
        filename = f"OR_ABI-L1b-RadF-M6{band_config.file_pattern}_s2025038{i:04d}.nc"
        images.append(
            ImageInfo(
                image_id=filename,
                source_uri=f"ABI-L1b-RadF/2025/038/12/{filename}",
                data_source_id=f"goes19_abi_{band_config.band_id}",
                processor_id=f"goes_{band_config.band_id}",
                output_prefix=band_config.s3_prefix,
            )
        )
    return images


class TestDuplicatePrevention:
    """Verify that consecutive discovery cycles do not publish duplicate work units."""

    @pytest.mark.asyncio
    async def test_second_run_publishes_zero_duplicates(
        self, mock_config, progress_tracker
    ):
        """Two back-to-back runs with identical NOAA state must not duplicate."""
        registry = DataSourceRegistry()
        mq_client = MagicMock()

        bands = ["band_2", "band_9", "band_13"]

        for band_id in bands:
            band_config = BAND_CONFIGS[band_id]
            images = _make_images(band_config, count=26)
            registry.register(FakeDataSource(band_config, images))

        producer = ImageDiscoveryProducer.__new__(ImageDiscoveryProducer)
        producer._config = mock_config
        producer._mq_client = mq_client
        producer._progress_tracker = progress_tracker
        producer._data_source_registry = registry
        producer._s3_client = AsyncMock()
        producer._s3_client.list_prefixes = AsyncMock(return_value=[])

        # First run: should publish 78 work units (26 per band)
        first_count = await producer.discover_and_publish()
        assert first_count == 78
        assert mq_client.publish.call_count == 78

        mq_client.reset_mock()

        # Second run (same state): should publish 0
        second_count = await producer.discover_and_publish()
        assert second_count == 0
        assert mq_client.publish.call_count == 0

    @pytest.mark.asyncio
    async def test_completed_image_not_republished_when_tiles_exist(
        self, mock_config, progress_tracker
    ):
        """After a worker completes an image, the producer must not republish it."""
        band_config = BAND_CONFIGS["band_2"]
        images = _make_images(band_config, count=3)

        registry = DataSourceRegistry()
        registry.register(FakeDataSource(band_config, images))

        mq_client = MagicMock()

        producer = ImageDiscoveryProducer.__new__(ImageDiscoveryProducer)
        producer._config = mock_config
        producer._config.ENABLE_BAND_13 = False
        producer._config.ENABLE_BAND_9 = False
        producer._mq_client = mq_client
        producer._progress_tracker = progress_tracker
        producer._data_source_registry = registry
        producer._s3_client = AsyncMock()
        producer._s3_client.list_prefixes = AsyncMock(return_value=[])

        # First run: publish all 3
        count = await producer.discover_and_publish()
        assert count == 3
        mq_client.reset_mock()

        # Simulate worker completing the first image:
        # mark_completed removes from SQLite
        progress_tracker.mark_completed(images[0].image_id, "band_2")
        # And its tiles now exist in S3
        stem = Path(images[0].image_id).stem
        tileset_prefix = f"band_2/tiles/{stem}/"
        producer._s3_client.list_prefixes = AsyncMock(return_value=[tileset_prefix])

        # Second run: images[0] covered by S3, images[1-2] still in-progress
        count = await producer.discover_and_publish()
        assert count == 0
        assert mq_client.publish.call_count == 0

    @pytest.mark.asyncio
    async def test_new_image_published_alongside_in_progress(
        self, mock_config, progress_tracker
    ):
        """A new image arriving while others are in-progress is published once."""
        band_config = BAND_CONFIGS["band_2"]
        initial_images = _make_images(band_config, count=2)

        registry = DataSourceRegistry()
        source = FakeDataSource(band_config, initial_images)
        registry.register(source)

        mq_client = MagicMock()

        producer = ImageDiscoveryProducer.__new__(ImageDiscoveryProducer)
        producer._config = mock_config
        producer._config.ENABLE_BAND_13 = False
        producer._config.ENABLE_BAND_9 = False
        producer._mq_client = mq_client
        producer._progress_tracker = progress_tracker
        producer._data_source_registry = registry
        producer._s3_client = AsyncMock()
        producer._s3_client.list_prefixes = AsyncMock(return_value=[])

        # First run: publish 2 images
        count = await producer.discover_and_publish()
        assert count == 2
        mq_client.reset_mock()

        # A new image arrives in NOAA S3
        new_image = ImageInfo(
            image_id="OR_ABI-L1b-RadF-M6C02_G19_s2025038NEW0.nc",
            source_uri="ABI-L1b-RadF/2025/038/12/OR_ABI-L1b-RadF-M6C02_G19_s2025038NEW0.nc",
            data_source_id="goes19_abi_band_2",
            processor_id="goes_band_2",
            output_prefix=band_config.s3_prefix,
        )
        source._images = initial_images + [new_image]

        # Second run: only the new image should be published
        count = await producer.discover_and_publish()
        assert count == 1
        assert mq_client.publish.call_count == 1

        published_unit = mq_client.publish.call_args[0][0]
        assert published_unit.image_id == new_image.image_id
