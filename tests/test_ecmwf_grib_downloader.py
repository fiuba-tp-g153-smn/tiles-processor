"""Tests for the inline EcmwfGribDownloader's per-stage metrics + HEAD existence."""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest

from models.ecmwf_config import ECMWF_TP_CONFIG
from models.work_unit import WorkUnit
from worker.ecmwf_grib_downloader import EcmwfGribDownloader


def _work_unit() -> WorkUnit:
    return WorkUnit.create(
        image_id="20260217T0000Z",
        source_uri="2026-02-17T00:00:00+00:00",
        data_source_id="ecmwf_tp_producer",
        processor_id="ecmwf_tp_grib_downloader",
        output_prefix="grib/models/ecmwf",
        bounds={},
        band_id="ecmwf_tp_producer",
    )


def _downloader(s3) -> EcmwfGribDownloader:
    return EcmwfGribDownloader(product_config=ECMWF_TP_CONFIG, s3_client=s3, bounds={})


@pytest.mark.asyncio
async def test_process_records_upload_stage_timing():
    """The GRIB PUT time is recorded under the 'upload' (Subida) stage."""
    upload_s = 0.05
    s3 = AsyncMock()
    s3.head_exists = AsyncMock(return_value=False)  # GRIB + all COGs missing

    async def _slow_upload(_key, _path):
        await asyncio.sleep(upload_s)
        return True

    s3.upload_file = AsyncMock(side_effect=_slow_upload)
    collector = MagicMock()

    await _downloader(s3).process("/tmp/x.grib", _work_unit(), MagicMock(), collector)

    collector.set_stage_timings.assert_called_once()
    stages = collector.set_stage_timings.call_args.args[0]
    assert stages["upload"] >= upload_s * 0.8  # ≈ the PUT duration
    assert "list" in stages and "enqueue" in stages  # sibling stages present


@pytest.mark.asyncio
async def test_process_upload_is_zero_when_grib_already_cached():
    """Idempotent retry: GRIB HEAD hits → PUT skipped, upload stage is 0.0."""
    s3 = AsyncMock()
    # GRIB key (…​.grib) exists; the 47 COG candidates (.tif) do not.
    s3.head_exists = AsyncMock(side_effect=lambda key: key.endswith(".grib"))
    s3.upload_file = AsyncMock(return_value=True)
    collector = MagicMock()

    await _downloader(s3).process("/tmp/x.grib", _work_unit(), MagicMock(), collector)

    stages = collector.set_stage_timings.call_args.args[0]
    assert stages["upload"] == 0.0
    s3.upload_file.assert_not_called()


@pytest.mark.asyncio
async def test_process_skips_enqueue_for_existing_cog_periods():
    """Periods whose COG already exists (HEAD hit) are not re-enqueued."""
    forecast_ts = "20260217T0000Z"
    # Pretend the T+6 COG (20260217T0600Z) already exists; everything else missing.
    existing_cog = f"{ECMWF_TP_CONFIG.cog_prefix}/{forecast_ts}/20260217T0600Z.tif"
    s3 = AsyncMock()

    async def _head(key):
        return key == existing_cog  # GRIB missing → uploaded; one COG present

    s3.head_exists = AsyncMock(side_effect=_head)
    s3.upload_file = AsyncMock(return_value=True)
    mq = MagicMock()

    await _downloader(s3).process("/tmp/x.grib", _work_unit(), mq)

    enqueued_ids = {call.args[0].image_id for call in mq.publish.call_args_list}
    assert "20260217T0600Z" not in enqueued_ids  # existing period skipped
    assert "20260217T0900Z" in enqueued_ids  # a missing period still enqueued


@pytest.mark.asyncio
async def test_process_is_metrics_noop_without_collector():
    """No collector (default) must not raise; processing still proceeds."""
    s3 = AsyncMock()
    s3.head_exists = AsyncMock(return_value=False)
    s3.upload_file = AsyncMock(return_value=True)

    await _downloader(s3).process("/tmp/x.grib", _work_unit(), MagicMock())

    s3.upload_file.assert_awaited_once()
