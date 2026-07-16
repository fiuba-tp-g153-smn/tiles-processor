"""Tests for ECMWF producer availability-gated discovery."""

import os
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest

from data_sources.base import DiscoveryConfig
from data_sources.ecmwf_producer_source import (
    EcmwfProducerDataSource,
    ForecastNotAvailableError,
    TransientDownloadError,
    _STEPS,
)
from models.ecmwf_config import ECMWF_TP_CONFIG


def _config(now: datetime) -> DiscoveryConfig:
    return DiscoveryConfig(
        current_time=now,
        existing_tilesets=set(),
        in_progress_images=set(),
        bounds={},
    )


def _source(grib_cached: bool = False) -> EcmwfProducerDataSource:
    """Source whose s3_client.head_exists reports GRIBs as missing by default."""
    source = EcmwfProducerDataSource(product_config=ECMWF_TP_CONFIG, s3_client=None)
    s3 = MagicMock()
    s3.head_exists = AsyncMock(return_value=grib_cached)
    source._s3_client = s3
    return source


@pytest.mark.asyncio
async def test_discover_emits_only_runs_at_or_before_latest():
    """Candidates newer than the latest published run are never enqueued."""
    source = _source()
    now = datetime(2026, 2, 17, 13, 0, tzinfo=UTC)
    latest = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)  # 17T12 candidate is unpublished
    source._latest_available_run = MagicMock(return_value=latest)

    images = await source.discover_images(_config(now))

    ids = {img.image_id for img in images}
    assert ids == {"20260217T0000Z", "20260216T1200Z"}
    assert "20260217T1200Z" not in ids  # not yet published → no SKIP-loop unit


@pytest.mark.asyncio
async def test_discover_skips_cached_run_via_head():
    """A published-but-already-cached run is skipped via head_exists (no LIST)."""
    source = _source()
    now = datetime(2026, 2, 17, 13, 0, tzinfo=UTC)
    latest = datetime(2026, 2, 17, 12, 0, tzinfo=UTC)  # all 3 candidates published
    source._latest_available_run = MagicMock(return_value=latest)
    cached_key = f"{ECMWF_TP_CONFIG.grib_prefix}/20260217T1200Z.grib"
    source._s3_client.head_exists = AsyncMock(side_effect=lambda key: key == cached_key)

    images = await source.discover_images(_config(now))

    ids = {img.image_id for img in images}
    assert "20260217T1200Z" not in ids  # cached → skipped
    assert ids == {"20260217T0000Z", "20260216T1200Z"}


@pytest.mark.asyncio
async def test_discover_emits_nothing_when_availability_unknown():
    """If latest() can't be established, discovery is fail-safe (emits nothing)."""
    source = _source()
    now = datetime(2026, 2, 17, 13, 0, tzinfo=UTC)
    source._latest_available_run = MagicMock(return_value=None)

    assert await source.discover_images(_config(now)) == []


def test_latest_available_run_uses_last_step_and_normalizes_to_utc():
    """latest() is queried for the final step and its naive result is made UTC-aware."""
    source = _source()
    source._sources = ("ecmwf",)
    fake_client = MagicMock()
    fake_client.latest.return_value = datetime(2026, 2, 17, 0, 0)  # naive

    with patch(
        "data_sources.ecmwf_producer_source.Client", return_value=fake_client
    ) as client_cls:
        result = source._latest_available_run()

    assert result == datetime(2026, 2, 17, 0, 0, tzinfo=UTC)
    client_cls.assert_called_once_with(source="ecmwf")
    kwargs = fake_client.latest.call_args.kwargs
    assert kwargs["type"] == "fc"
    assert kwargs["step"] == _STEPS[-1]
    assert kwargs["param"] == [ECMWF_TP_CONFIG.parameter]


def test_latest_available_run_falls_back_across_mirrors_and_returns_none():
    """Every mirror failing (network error) returns None, after trying each in order."""
    source = _source()
    source._sources = ("ecmwf", "azure", "aws")
    fake_client = MagicMock()
    fake_client.latest.side_effect = ValueError("Cannot establish latest date")

    with patch(
        "data_sources.ecmwf_producer_source.Client", return_value=fake_client
    ) as client_cls:
        assert source._latest_available_run() is None

    # One client per mirror was tried.
    assert client_cls.call_count == 3


@pytest.mark.asyncio
async def test_download_falls_back_to_next_mirror_on_transient(tmp_path):
    """A 503 on the first mirror falls through to the next, which succeeds."""
    source = _source()
    source._sources = ("ecmwf", "azure")
    forecast = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)

    tried = []

    def fake_retrieve(mirror, _forecast_time, target):
        tried.append(mirror)
        if mirror == "ecmwf":
            raise TransientDownloadError("503")
        target.write_bytes(b"GRIB")

    source._retrieve_from_mirror = fake_retrieve

    result = await source.download(forecast.isoformat(), tmp_path / "run")

    assert tried == ["ecmwf", "azure"]
    assert result == (tmp_path / "run").with_suffix(".grib")
    assert result.read_bytes() == b"GRIB"


@pytest.mark.asyncio
async def test_download_requeues_when_all_mirrors_transiently_fail(tmp_path):
    """Every mirror 503 → TransientDownloadError (requeue, not skip)."""
    source = _source()
    source._sources = ("ecmwf", "azure", "aws")
    forecast = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)

    def fake_retrieve(_mirror, _forecast_time, _target):
        raise TransientDownloadError("503")

    source._retrieve_from_mirror = fake_retrieve

    with pytest.raises(TransientDownloadError):
        await source.download(forecast.isoformat(), tmp_path / "run")


@pytest.mark.asyncio
async def test_download_skips_when_no_mirror_has_data_yet(tmp_path):
    """Every mirror 404 (and none transient) → ForecastNotAvailableError (skip)."""
    source = _source()
    source._sources = ("ecmwf", "azure")
    forecast = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)

    def fake_retrieve(_mirror, _forecast_time, _target):
        raise ForecastNotAvailableError("404")

    source._retrieve_from_mirror = fake_retrieve

    with pytest.raises(ForecastNotAvailableError):
        await source.download(forecast.isoformat(), tmp_path / "run")
