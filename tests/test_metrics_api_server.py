import asyncio
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

from metrics_api.server import PollSummaryMiddleware, create_app


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
    # Alembic owns the schema; create both DBs the metrics API opens.
    run_migrations(db, tmp_path / "progress_tracker.db")
    _seed(db)
    cfg = SimpleNamespace(
        METRICS_DB_PATH=str(db),
        LOG_LEVEL="INFO",
        METRICS_API_PORT=8090,
        METRICS_API_KEY="test-key",
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


_API_KEY = {"X-API-Key": "test-key"}


def test_root_returns_status(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "service": "tiles-processor-metrics"}


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


def test_jobs_limit_zero_returns_all(client):
    # 0 = sin límite: devuelve todas (la línea de tiempo carga el rango completo).
    assert len(client.get("/api/jobs?limit=2").json()) == 2
    assert len(client.get("/api/jobs?limit=0").json()) == 4


def test_jobs_since_before_window(client):
    # Seeded finished_at: 00:00:44, 01:00:44, 02:00:44 (goes) and 05:00:03 (radar).
    # Half-open window [01:00, 03:00) → the two middle goes jobs.
    rows = client.get(
        "/api/jobs?limit=0"
        "&since=2026-06-04T01:00:00%2B00:00"
        "&before=2026-06-04T03:00:00%2B00:00"
    ).json()
    assert {j["image_id"] for j in rows} == {"i1", "i2"}


def test_jobs_hours_window_narrows_results(client):
    # Seeded rows are dated 2026-06-04, so a 1h window excludes them while the
    # unwindowed query returns every row (mirrors the /api/export window test).
    assert len(client.get("/api/jobs?hours=1").json()) == 0
    assert len(client.get("/api/jobs").json()) == 4
    # The window composes with the other filters.
    assert len(client.get("/api/jobs?hours=1&type=radar_DBZH").json()) == 0


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


def _empty_client(db_dir, api_key="test-key"):
    """A metrics-API client over a freshly-migrated, empty metrics DB."""
    db_dir.mkdir(parents=True, exist_ok=True)
    metrics = db_dir / "metrics.db"
    run_migrations(metrics, db_dir / "progress_tracker.db")
    cfg = SimpleNamespace(
        METRICS_DB_PATH=str(metrics),
        LOG_LEVEL="INFO",
        METRICS_API_PORT=8090,
        METRICS_API_KEY=api_key,
        TMP_DIR=str(db_dir),
        RABBITMQ_HOST="127.0.0.1",
        RABBITMQ_PORT=1,
        RABBITMQ_USER="guest",
        RABBITMQ_PASSWORD="guest",
        RABBITMQ_QUEUE="tiles_queue",
        RABBITMQ_DLQ="tiles_dlq",
        RABBITMQ_DLX="tiles_dlx",
    )
    return fastapi_testclient.TestClient(create_app(cfg))


def test_swagger_and_openapi_available(client):
    docs = client.get("/docs")
    assert docs.status_code == 200
    assert "swagger" in docs.text.lower()

    spec = client.get("/openapi.json").json()
    assert "/api/export" in spec["paths"]
    assert "/api/import" in spec["paths"]
    # the import path documents the 409 mismatch response
    assert "409" in spec["paths"]["/api/import"]["post"]["responses"]


def test_export_returns_versioned_dump(client):
    data = client.get("/api/export").json()
    assert data["version"] == "metrics_0001"
    assert data["window_hours"] is None
    assert data["count"] == 4 and len(data["jobs"]) == 4
    a_job = next(j for j in data["jobs"] if j["outcome"] == "success")
    assert a_job["stage_timings"]["georef"] == pytest.approx(3.2)


def test_export_schema_documented_without_response_model(client):
    # /api/export returns a raw Response (orjson) for memory, but the schema is
    # still advertised in OpenAPI so Swagger renders it.
    spec = client.get("/openapi.json").json()
    schema = spec["paths"]["/api/export"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert "MetricsExport" in schema.get("$ref", "")
    assert "MetricsExport" in spec["components"]["schemas"]


def test_export_is_gzip_compressed(client):
    r = client.get("/api/export", headers={"Accept-Encoding": "gzip"})
    assert r.status_code == 200
    assert r.headers.get("content-encoding") == "gzip"
    assert r.json()["count"] == 4  # body still decodes to the same JSON


def test_export_window_filter_narrows_results(client):
    # Seeded rows are dated 2026-06-04, so a 1h window excludes them while an
    # all-time export returns every row.
    assert len(client.get("/api/export?hours=1").json()["jobs"]) == 0
    assert len(client.get("/api/export").json()["jobs"]) == 4


def test_import_round_trip_is_idempotent(client, tmp_path):
    dump = client.get("/api/export").json()
    target = _empty_client(tmp_path / "target")

    first = target.post("/api/import", json=dump, headers=_API_KEY).json()
    assert first == {"version": "metrics_0001", "inserted": 4, "skipped": 0}
    assert len(target.get("/api/jobs?limit=10").json()) == 4

    # Re-importing the same dump inserts nothing (idempotent skip-duplicates).
    second = target.post("/api/import", json=dump, headers=_API_KEY).json()
    assert second == {"version": "metrics_0001", "inserted": 0, "skipped": 4}
    assert len(target.get("/api/jobs?limit=10").json()) == 4


def test_import_requires_api_key(client):
    dump = client.get("/api/export").json()
    assert client.post("/api/import", json=dump).status_code == 401  # missing
    assert (
        client.post(
            "/api/import", json=dump, headers={"X-API-Key": "wrong"}
        ).status_code
        == 401
    )
    assert client.post("/api/import", json=dump, headers=_API_KEY).status_code == 200


def test_import_disabled_when_no_key_configured(tmp_path):
    # The metrics API with no METRICS_API_KEY fails closed on writes (503).
    target = _empty_client(tmp_path / "nokey", api_key="")
    dump = {
        "version": "metrics_0001",
        "exported_at": "2026-06-05T10:00:00+00:00",
        "window_hours": None,
        "count": 0,
        "jobs": [],
    }
    assert target.post("/api/import", json=dump, headers=_API_KEY).status_code == 503


def test_import_rejects_version_mismatch(client):
    dump = client.get("/api/export").json()
    dump["version"] = "metrics_9999"
    r = client.post("/api/import", json=dump, headers=_API_KEY)
    assert r.status_code == 409
    assert "does not match" in r.json()["detail"]


def test_import_rejects_malformed_body(client):
    bad = {
        "version": "metrics_0001",
        "exported_at": "2026-06-05T10:00:00+00:00",
        "window_hours": None,
        "count": 1,
        "jobs": [{"image_id": "only-this-field"}],  # missing required columns
    }
    assert client.post("/api/import", json=bad, headers=_API_KEY).status_code == 422


# --- PollSummaryMiddleware -------------------------------------------------
# The debounce timing is verified manually (it needs a live event loop); these
# cover the pure aggregation/formatting logic that builds the consolidated line.


def test_poll_summary_emits_one_consolidated_line(caplog):
    mw = PollSummaryMiddleware(app=None)
    # Simulate a poll burst: throughput hit twice, arrival order preserved.
    for path in ("/api/summary", "/api/throughput", "/api/throughput", "/api/jobs"):
        mw._counts[path] += 1  # pylint: disable=protected-access
    with caplog.at_level("INFO", logger="metrics_api.server"):
        mw._emit()  # pylint: disable=protected-access
    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "served 4 request(s): /api/summary, /api/throughput, /api/jobs"
    )
    assert not mw._counts  # reset after emit  # pylint: disable=protected-access


def test_poll_summary_emit_noop_when_empty(caplog):
    mw = PollSummaryMiddleware(app=None)
    with caplog.at_level("INFO", logger="metrics_api.server"):
        mw._emit()  # pylint: disable=protected-access
    assert caplog.records == []


def test_poll_summary_records_health_probe():
    mw = PollSummaryMiddleware(app=None)

    async def record():
        mw._record("/health")  # pylint: disable=protected-access

    asyncio.run(record())  # _record needs a running loop to arm the debounce
    assert mw._counts["/health"] == 1  # pylint: disable=protected-access


def test_poll_summary_ignores_root_probe():
    mw = PollSummaryMiddleware(app=None)
    # The bare root ping returns before touching the event loop (loop-free).
    mw._record("/")  # pylint: disable=protected-access
    assert not mw._counts  # pylint: disable=protected-access
