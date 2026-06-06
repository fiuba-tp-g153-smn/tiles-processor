import os
import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from db.migrate import ensure_migrations, run_migrations


def _tables(path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {row[0] for row in rows}
    finally:
        conn.close()


def _version(path) -> str | None:
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _columns(path, table: str) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    finally:
        conn.close()


def test_creates_both_schemas_at_head(migrated_dbs):
    assert "job_metrics" in _tables(migrated_dbs.metrics)
    assert "processed_images" in _tables(migrated_dbs.progress)
    assert _version(migrated_dbs.metrics) == "metrics_0001"
    assert _version(migrated_dbs.progress) == "progress_0001"


def test_metrics_columns_match_repository(migrated_dbs):
    assert _columns(migrated_dbs.metrics, "job_metrics") == {
        "id",
        "work_unit_id",
        "image_id",
        "data_source_id",
        "processor_id",
        "band_id",
        "job_type",
        "product_label",
        "image_timestamp",
        "outcome",
        "worker_host",
        "started_at",
        "finished_at",
        "retry_count",
        "error_message",
        "download_s",
        "process_s",
        "total_s",
        "stage_timings_json",
    }


def test_progress_columns_match_tracker(migrated_dbs):
    assert _columns(migrated_dbs.progress, "processed_images") == {
        "image_id",
        "band_id",
        "status",
        "created_at",
        "updated_at",
    }


def test_migrations_are_idempotent(tmp_path):
    metrics, progress = tmp_path / "metrics.db", tmp_path / "progress_tracker.db"
    run_migrations(metrics, progress)
    run_migrations(metrics, progress)  # must not raise
    assert _version(metrics) == "metrics_0001"
    assert _version(progress) == "progress_0001"


def test_ensure_migrations_applies_under_lock(tmp_path):
    """The startup entry migrates both DBs and creates the coordination lockfile."""
    config = SimpleNamespace(
        TMP_DIR=str(tmp_path),
        METRICS_DB_PATH=str(tmp_path / "metrics.db"),
    )

    ensure_migrations(config)
    ensure_migrations(config)  # idempotent: a second call is a no-op

    assert "job_metrics" in _tables(tmp_path / "metrics.db")
    assert "processed_images" in _tables(tmp_path / "progress_tracker.db")
    assert _version(tmp_path / "metrics.db") == "metrics_0001"
    assert (tmp_path / ".migrate.lock").exists()


def test_adopts_existing_database_without_losing_data(tmp_path):
    """An existing DB (table present, no alembic_version) is adopted, not recreated."""
    metrics, progress = tmp_path / "metrics.db", tmp_path / "progress_tracker.db"
    conn = sqlite3.connect(str(metrics))
    conn.execute("CREATE TABLE job_metrics (id INTEGER PRIMARY KEY, image_id TEXT)")
    conn.execute("INSERT INTO job_metrics (image_id) VALUES ('old')")
    conn.commit()
    conn.close()

    run_migrations(metrics, progress)

    assert _version(metrics) == "metrics_0001"
    conn = sqlite3.connect(str(metrics))
    try:
        assert conn.execute("SELECT image_id FROM job_metrics").fetchone()[0] == "old"
    finally:
        conn.close()
