import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.metrics_repository import MetricsRepository
from clients.progress_tracker import ProgressTracker
from db.migrate import run_migrations
from models.job_metrics import JobMetrics

# FastAPI/uvicorn are optional deps; skip cleanly if not installed.
fastapi_testclient = pytest.importorskip("fastapi.testclient")

from dashboard.server import create_app


def _seed(db_path):
    repo = MetricsRepository(db_path)
    for i, total in enumerate([40, 42, 120]):
        repo.record(
            JobMetrics(
                work_unit_id="w",
                image_id=f"i{i}",
                data_source_id="goes19_abi_band_13",
                processor_id="goes_band_13",
                band_id="band_13",
                job_type="goes19_abi_band_13",
                product_label="GOES ABI band_13 · Cloud Tops",
                image_timestamp=f"2026052132020{i}",
                outcome="success",
                worker_host="worker1",
                started_at=f"2026-06-04T0{i}:00:00+00:00",
                finished_at=f"2026-06-04T0{i}:00:44+00:00",
                total_s=total,
                download_s=1.5,
                process_s=total - 1.5,
                stage_timings={"georef": 3.2, "tiling": total * 0.4},
            )
        )
    repo.record(
        JobMetrics(
            work_unit_id="w",
            image_id="ix",
            data_source_id="radar_DBZH",
            processor_id="radar",
            band_id="radar",
            job_type="radar_DBZH",
            product_label="Radar RMA12 DBZH",
            image_timestamp="20260114T170328Z",
            outcome="dlq",
            worker_host="worker2",
            started_at="2026-06-04T05:00:00+00:00",
            finished_at="2026-06-04T05:00:03+00:00",
            error_message="boom",
            retry_count=3,
        )
    )


@pytest.fixture()
def client(tmp_path):
    db = tmp_path / "metrics.db"
    # Alembic owns the schema; create both DBs the dashboard opens.
    run_migrations(db, tmp_path / "progress_tracker.db")
    _seed(db)
    cfg = SimpleNamespace(
        METRICS_DB_PATH=str(db),
        LOG_LEVEL="INFO",
        DASHBOARD_PORT=8090,
        TMP_DIR=str(tmp_path),
        # Point RabbitMQ at a closed port so the live probe fails fast and
        # degrades to n/a (exercises graceful degradation).
        RABBITMQ_HOST="127.0.0.1",
        RABBITMQ_PORT=1,
        RABBITMQ_USER="guest",
        RABBITMQ_PASSWORD="guest",
        RABBITMQ_QUEUE="tiles_queue",
        RABBITMQ_DLQ="tiles_dlq",
        RABBITMQ_DLX="tiles_dlx",
    )
    return fastapi_testclient.TestClient(create_app(cfg))


def test_root_returns_404(client):
    # The HTML UI moved to the visualizer app; only the JSON API remains.
    assert client.get("/").status_code == 404


def test_cors_header_present(client):
    # The visualizer consumes this API cross-origin, so responses must carry
    # an Access-Control-Allow-Origin header.
    r = client.get("/api/summary", headers={"Origin": "http://localhost:6010"})
    assert r.status_code == 200
    assert r.headers.get("access-control-allow-origin") in (
        "*",
        "http://localhost:6010",
    )


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_summary_groups_by_type(client):
    s = client.get("/api/summary").json()
    by_type = {x["job_type"]: x for x in s}
    assert set(by_type) == {"goes19_abi_band_13", "radar_DBZH"}
    goes = by_type["goes19_abi_band_13"]
    assert goes["counts"]["total"] == 3
    assert goes["total_s"]["max"] == 120
    assert goes["stages"]["georef"] == pytest.approx(3.2)
    assert by_type["radar_DBZH"]["counts"]["dlq"] == 1


def test_jobs_filters(client):
    assert len(client.get("/api/jobs?limit=10").json()) == 4
    dlq = client.get("/api/jobs?outcome=dlq").json()
    assert len(dlq) == 1 and dlq[0]["error_message"] == "boom"
    radar = client.get("/api/jobs?type=radar_DBZH").json()
    assert all(j["job_type"] == "radar_DBZH" for j in radar)


def test_throughput(client):
    tp = client.get("/api/throughput?bucket=hour").json()
    assert tp and set(tp[0]) == {"bucket", "job_type", "count"}


def test_timeseries(client):
    ts = client.get("/api/timeseries?bucket=hour").json()
    assert ts
    assert set(ts[0]) == {
        "bucket",
        "job_type",
        "count",
        "avg_total_s",
        "p95_total_s",
        "stages",
    }
    goes = [r for r in ts if r["job_type"] == "goes19_abi_band_13"]
    assert goes and goes[0]["stages"].get("georef") == pytest.approx(3.2)


def test_jobs_offset_paginates(client):
    page1 = client.get("/api/jobs?limit=2&offset=0").json()
    page2 = client.get("/api/jobs?limit=2&offset=2").json()
    assert len(page1) == 2 and len(page2) == 2
    ids1 = {j["image_id"] for j in page1}
    ids2 = {j["image_id"] for j in page2}
    assert ids1.isdisjoint(ids2)  # distinct pages


def test_live_degrades_when_rabbitmq_down(client, tmp_path):
    # Seed in-progress jobs in the shared progress tracker.
    tracker = ProgressTracker(tmp_path / "progress_tracker.db")
    tracker.mark_in_progress("20260521320209", "band_13")
    tracker.mark_in_progress("RMA12_DBZH_20260114T170328Z", "radar")

    body = client.get("/api/live").json()
    assert set(body) == {"queues", "in_progress"}
    # RabbitMQ unreachable -> counts are n/a, not an error.
    assert body["queues"] == {"work": None, "dlq": None}
    assert len(body["in_progress"]) == 2
    assert {p["image_id"] for p in body["in_progress"]} == {
        "20260521320209",
        "RMA12_DBZH_20260114T170328Z",
    }
