import os
import sqlite3
import sys
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.metrics_repository import MetricsRepository
from models.job_metrics import JobMetrics, JobOutcome


def _make_metrics(image_id: str = "img1", outcome: str = JobOutcome.SUCCESS.value):
    return JobMetrics(
        work_unit_id="wu-1",
        image_id=image_id,
        data_source_id="goes19_abi_band_13",
        processor_id="goes_band_13",
        band_id="band_13",
        job_type="goes19_abi_band_13",
        product_label="GOES ABI band_13 · Cloud Tops",
        image_timestamp=image_id,
        outcome=outcome,
        worker_host="worker1",
        started_at="2026-06-04T00:00:00+00:00",
        finished_at="2026-06-04T00:00:44+00:00",
        retry_count=0,
        error_message=None,
        download_s=1.84,
        process_s=42.1,
        total_s=44.31,
        stage_timings={"georef": 3.2, "tiling": 19.3},
    )


def test_record_persists_a_row(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    repo.record(_make_metrics())

    conn = sqlite3.connect(str(tmp_path / "metrics.db"))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM job_metrics").fetchall()
    conn.close()

    assert len(rows) == 1
    row = rows[0]
    assert row["image_id"] == "img1"
    assert row["outcome"] == "success"
    assert row["job_type"] == "goes19_abi_band_13"
    assert abs(row["total_s"] - 44.31) < 1e-6
    # stage_timings round-trips as JSON
    assert '"georef"' in row["stage_timings_json"]


def test_wal_mode_is_enabled(tmp_path):
    MetricsRepository(tmp_path / "metrics.db")
    conn = sqlite3.connect(str(tmp_path / "metrics.db"))
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()
    assert mode.lower() == "wal"


def test_concurrent_writes_do_not_collide(tmp_path):
    """Simulate several workers writing concurrently to one WAL database.

    Each thread uses its own MetricsRepository (its own connections), mirroring
    separate worker processes sharing the file. With WAL + 30s busy timeout no
    write should be lost or raise 'database is locked'.
    """
    db_path = tmp_path / "metrics.db"
    MetricsRepository(db_path)  # initialize schema once

    writers = 5
    per_writer = 40
    errors: list[Exception] = []

    def worker(worker_idx: int):
        repo = MetricsRepository(db_path)
        try:
            for i in range(per_writer):
                repo.record(_make_metrics(image_id=f"w{worker_idx}-img{i}"))
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(writers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM job_metrics").fetchone()[0]
    conn.close()
    assert count == writers * per_writer
