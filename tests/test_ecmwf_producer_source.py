"""Tests for ECMWF producer availability-gated discovery."""

import os
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest

from data_sources.base import DiscoveryConfig
from data_sources.ecmwf_producer_source import EcmwfProducerDataSource
from models.ecmwf_config import ECMWF_TP_CONFIG


def _config(now: datetime) -> DiscoveryConfig:
    return DiscoveryConfig(
        current_time=now,
        existing_tilesets=set(),
        in_progress_images=set(),
        bounds={},
    )


def _source(
    latest: datetime | None = None, grib_cached: bool = False
) -> EcmwfProducerDataSource:
    """Source whose repository reports `latest` and whose GRIBs are missing."""
    repository = MagicMock()
    repository.latest_available_run = AsyncMock(return_value=latest)
    repository.fetch = AsyncMock()
    s3 = MagicMock()
    s3.head_exists = AsyncMock(return_value=grib_cached)
    return EcmwfProducerDataSource(
        product_config=ECMWF_TP_CONFIG, s3_client=s3, repository=repository
    )


@pytest.mark.asyncio
async def test_discover_emits_only_runs_at_or_before_latest():
    """Candidates newer than the latest published run are never enqueued."""
    now = datetime(2026, 2, 17, 13, 0, tzinfo=UTC)
    latest = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)  # 17T12 candidate is unpublished
    source = _source(latest=latest)

    images = await source.discover_images(_config(now))

    ids = {img.image_id for img in images}
    assert ids == {"20260217T0000Z", "20260216T1200Z"}
    assert "20260217T1200Z" not in ids  # not yet published → no SKIP-loop unit


@pytest.mark.asyncio
async def test_discover_skips_cached_run_via_head():
    """A published-but-already-cached run is skipped via head_exists (no LIST)."""
    now = datetime(2026, 2, 17, 13, 0, tzinfo=UTC)
    latest = datetime(2026, 2, 17, 12, 0, tzinfo=UTC)  # all 3 candidates published
    source = _source(latest=latest)
    cached_key = f"{ECMWF_TP_CONFIG.grib_prefix}/20260217T1200Z.grib"
    source._s3_client.head_exists = AsyncMock(side_effect=lambda key: key == cached_key)

    images = await source.discover_images(_config(now))

    ids = {img.image_id for img in images}
    assert "20260217T1200Z" not in ids  # cached → skipped
    assert ids == {"20260217T0000Z", "20260216T1200Z"}


@pytest.mark.asyncio
async def test_discover_emits_nothing_when_availability_unknown():
    """If the backend can't establish a latest run, discovery emits nothing."""
    now = datetime(2026, 2, 17, 13, 0, tzinfo=UTC)

    assert await _source(latest=None).discover_images(_config(now)) == []


@pytest.mark.asyncio
async def test_discover_is_backend_agnostic_about_availability():
    """Discovery reads availability off the repository, whichever backend it is."""
    now = datetime(2026, 2, 17, 13, 0, tzinfo=UTC)
    source = _source(latest=datetime(2026, 2, 17, 12, 0, tzinfo=UTC))

    await source.discover_images(_config(now))

    source._repository.latest_available_run.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_download_delegates_to_the_repository(tmp_path):
    """The source forces the .grib suffix and hands the fetch to the backend."""
    forecast = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)
    source = _source()
    target = (tmp_path / "run").with_suffix(".grib")

    async def fake_fetch(_forecast_time, dest):
        dest.write_bytes(b"GRIB")
        return dest

    source._repository.fetch = AsyncMock(side_effect=fake_fetch)

    result = await source.download(forecast.isoformat(), tmp_path / "run")

    assert result == target
    assert result.read_bytes() == b"GRIB"
    source._repository.fetch.assert_awaited_once_with(forecast, target)


@pytest.mark.asyncio
async def test_download_without_a_repository_fails_loudly(tmp_path):
    """A half-built source must not look like a transient download failure."""
    source = EcmwfProducerDataSource(product_config=ECMWF_TP_CONFIG, s3_client=None)

    with pytest.raises(RuntimeError, match="EcmwfGribRepository"):
        await source.download(
            datetime(2026, 2, 17, tzinfo=UTC).isoformat(), tmp_path / "run"
        )
