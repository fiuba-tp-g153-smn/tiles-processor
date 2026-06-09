import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from models.job_metrics import JobOutcome
from models.work_unit import WorkUnit
from worker.job_metrics_context import JobMetricsContext


def _work_unit():
    return WorkUnit.create(
        image_id="20260521320209",
        source_uri="s3://bucket/key",
        data_source_id="goes19_abi_band_13",
        processor_id="goes_band_13",
        output_prefix="tiles/band_13",
        bounds={"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        band_id="band_13",
    )


def test_build_requires_outcome():
    ctx = JobMetricsContext(_work_unit(), worker_host="worker-light1")
    assert ctx.has_outcome is False
    with pytest.raises(ValueError):
        ctx.build()


def test_build_populates_timings_and_label():
    ctx = JobMetricsContext(_work_unit(), worker_host="worker-light1")
    ctx.set_download_seconds(1.5)
    ctx.set_process_seconds(40.0)
    ctx.set_stage_timings({"georef": 3.2})
    ctx.mark_outcome(JobOutcome.SUCCESS)

    metrics = ctx.build()
    assert metrics.outcome == "success"
    assert metrics.download_s == 1.5
    assert metrics.process_s == 40.0
    assert metrics.stage_timings == {"georef": 3.2}
    assert metrics.total_s is not None and metrics.total_s >= 0
    assert metrics.job_type == "goes19_abi_band_13"
    assert "GOES ABI" in metrics.product_label
    assert metrics.worker_host == "worker-light1"  # injected worker id round-trips


def test_failure_outcome_records_error_message():
    ctx = JobMetricsContext(_work_unit(), worker_host="worker-light1")
    ctx.mark_outcome(JobOutcome.DLQ, "boom")
    metrics = ctx.build()
    assert metrics.outcome == "dlq"
    assert metrics.error_message == "boom"
