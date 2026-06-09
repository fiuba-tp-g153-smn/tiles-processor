import os
import sqlite3
import sys
import threading

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.metrics_repository import MetricsRepository
from db.migrate import run_migrations
from models.job_metrics import JobMetrics, JobOutcome


@pytest.fixture(autouse=True)
def _migrate_schema(tmp_path):
    """Alembic owns the schema now; apply it to the temp DB before each test."""
    run_migrations(tmp_path / "metrics.db", tmp_path / "progress_tracker.db")


def _make_metrics(
    image_id: str = "img1",
    outcome: str = JobOutcome.SUCCESS.value,
    finished_at: str = "2026-06-04T00:00:44+00:00",
    job_type: str = "goes19_abi_band_13",
    product_label: str = "GOES ABI band_13 · Cloud Tops",
):
    return JobMetrics(
        work_unit_id="wu-1",
        image_id=image_id,
        data_source_id=job_type,
        processor_id="goes_band_13",
        band_id="band_13",
        job_type=job_type,
        product_label=product_label,
        image_timestamp=image_id,
        outcome=outcome,
        worker_host="worker1",
        started_at="2026-06-04T00:00:00+00:00",
        finished_at=finished_at,
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


def test_timing_series_groups_success_only(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    repo.record(_make_metrics("a", "success"))
    repo.record(_make_metrics("b", "success"))
    repo.record(_make_metrics("c", "dlq"))  # excluded from timing stats

    series = repo.timing_series(bucket="hour")
    assert len(series) == 1
    row = series[0]
    assert row["bucket"] == "2026-06-04T00"
    assert row["job_type"] == "goes19_abi_band_13"
    assert row["count"] == 2  # only successes
    assert row["avg_total_s"] == 44.31
    assert row["p95_total_s"] is not None
    assert row["stages"]["georef"] == 3.2


def test_summary_keeps_idle_types_in_window(tmp_path):
    """A type with no jobs in the window still appears: zero counts, real last run."""
    repo = MetricsRepository(tmp_path / "metrics.db")
    # Type A is inside the window; type B only ran before the cutoff.
    repo.record(
        _make_metrics("a", finished_at="2026-06-04T05:00:00+00:00", job_type="type_a")
    )
    repo.record(
        _make_metrics(
            "b",
            finished_at="2026-06-04T00:00:00+00:00",
            job_type="type_b",
            product_label="Type B label",
        )
    )

    summary = repo.summary(since="2026-06-04T04:00:00+00:00")
    by_type = {s["job_type"]: s for s in summary}

    assert set(by_type) == {"type_a", "type_b"}
    # Active type: real counts/timings within the window.
    assert by_type["type_a"]["counts"]["total"] == 1
    assert by_type["type_a"]["total_s"]["avg"] is not None
    # Idle type: zero counts, null timings, empty stages, but its real last run.
    idle = by_type["type_b"]
    assert idle["counts"]["total"] == 0
    assert idle["error_rate"] == 0.0
    assert idle["total_s"] == {"avg": None, "min": None, "max": None, "p95": None}
    assert idle["stages"] == {}
    assert idle["product_label"] == "Type B label"
    assert idle["last_finished"] == "2026-06-04T00:00:00+00:00"
    # Busiest first: the active type outranks the idle one.
    assert summary[0]["job_type"] == "type_a"


def test_summary_all_time_window_is_unchanged(tmp_path):
    """With no window, every type is present from real rows — no padded duplicates."""
    repo = MetricsRepository(tmp_path / "metrics.db")
    repo.record(_make_metrics("a", job_type="type_a"))
    repo.record(_make_metrics("b", job_type="type_b"))

    summary = repo.summary()
    job_types = [s["job_type"] for s in summary]
    assert sorted(job_types) == ["type_a", "type_b"]
    assert len(job_types) == len(set(job_types))  # no duplicates from padding
    assert all(s["counts"]["total"] == 1 for s in summary)


def test_summary_empty_db_returns_empty(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    assert repo.summary() == []
    assert repo.summary(since="2026-06-04T00:00:00+00:00") == []


def test_recent_jobs_offset(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    for i in range(5):
        # distinct finished_at so ordering (and thus paging) is deterministic
        repo.record(
            _make_metrics(f"img{i}", finished_at=f"2026-06-04T00:0{i}:00+00:00")
        )

    page1 = repo.recent_jobs(limit=2, offset=0)
    page2 = repo.recent_jobs(limit=2, offset=2)
    assert [j["image_id"] for j in page1] == ["img4", "img3"]  # newest first
    assert [j["image_id"] for j in page2] == ["img2", "img1"]


def test_recent_jobs_since_narrows_window(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    for i in range(5):  # finished at 00:00..00:04
        repo.record(
            _make_metrics(f"img{i}", finished_at=f"2026-06-04T00:0{i}:00+00:00")
        )

    recent = repo.recent_jobs(since="2026-06-04T00:03:00+00:00")
    assert [j["image_id"] for j in recent] == [
        "img4",
        "img3",
    ]  # >= cutoff, newest first


def test_recent_jobs_since_combines_with_outcome(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    repo.record(_make_metrics("old", "success", "2026-06-04T00:00:00+00:00"))
    repo.record(_make_metrics("new_ok", "success", "2026-06-04T05:00:00+00:00"))
    repo.record(_make_metrics("new_dlq", "dlq", "2026-06-04T05:00:00+00:00"))

    recent = repo.recent_jobs(since="2026-06-04T04:00:00+00:00", outcome="success")
    assert [j["image_id"] for j in recent] == ["new_ok"]


def test_recent_jobs_limit_zero_returns_all(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    for i in range(5):
        repo.record(
            _make_metrics(f"img{i}", finished_at=f"2026-06-04T00:0{i}:00+00:00")
        )

    assert len(repo.recent_jobs(limit=2)) == 2  # positive limit still caps
    assert len(repo.recent_jobs(limit=0)) == 5  # 0 = no limit (all rows)
    # Composes with the window filter.
    assert len(repo.recent_jobs(limit=0, since="2026-06-04T00:03:00+00:00")) == 2


def test_recent_jobs_before_bounds_window(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    for i in range(5):  # finished at 00:00..00:04
        repo.record(
            _make_metrics(f"img{i}", finished_at=f"2026-06-04T00:0{i}:00+00:00")
        )

    # Half-open window [00:01, 00:03): img1, img2 (newest-first).
    window = repo.recent_jobs(
        limit=0,
        since="2026-06-04T00:01:00+00:00",
        before="2026-06-04T00:03:00+00:00",
    )
    assert [j["image_id"] for j in window] == ["img2", "img1"]


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


def test_prune_to_max_rows_keeps_newest(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    for i in range(5):  # ids 1..5; img4 is newest
        repo.record(_make_metrics(image_id=f"img{i}"))

    assert repo.prune_to_max_rows(2) == 3  # keep the 2 newest, delete 3
    remaining = {job["image_id"] for job in repo.recent_jobs(limit=10)}
    assert remaining == {"img3", "img4"}


def test_prune_to_max_rows_noop_when_under_cap(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    repo.record(_make_metrics(image_id="a"))
    repo.record(_make_metrics(image_id="b"))

    assert repo.prune_to_max_rows(5) == 0
    assert len(repo.recent_jobs(limit=10)) == 2


def test_prune_to_max_rows_handles_zero_and_empty(tmp_path):
    repo = MetricsRepository(tmp_path / "metrics.db")
    assert repo.prune_to_max_rows(5) == 0  # empty table

    repo.record(_make_metrics(image_id="a"))
    assert repo.prune_to_max_rows(0) == 0  # max_rows <= 0 is a no-op
    assert len(repo.recent_jobs(limit=10)) == 1
