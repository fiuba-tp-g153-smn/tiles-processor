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


def _stamp_before(path, version: str) -> None:
    """Rewind the stamped revision so the next upgrade re-applies from there."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("UPDATE alembic_version SET version_num = ?", (version,))
        conn.commit()
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
    assert _version(migrated_dbs.metrics) == "metrics_0003"
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
    assert _version(metrics) == "metrics_0003"
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
    assert _version(tmp_path / "metrics.db") == "metrics_0003"
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

    assert _version(metrics) == "metrics_0003"
    conn = sqlite3.connect(str(metrics))
    try:
        assert conn.execute("SELECT image_id FROM job_metrics").fetchone()[0] == "old"
    finally:
        conn.close()


def _insert_job(conn, **row) -> None:
    """Insert one job_metrics row, defaulting everything the test doesn't set."""
    values = {
        "work_unit_id": "wu",
        "image_id": "img",
        "data_source_id": row.get("job_type", "t"),
        "processor_id": "p",
        "band_id": "b",
        "job_type": "t",
        "product_label": "label",
        "image_timestamp": "20260914T193000Z",
        "outcome": "success",
        "worker_host": "host",
        "started_at": "2026-09-14T19:30:00",
        "finished_at": "2026-09-14T19:30:35",
        **row,
    }
    columns = ", ".join(values)
    placeholders = ", ".join("?" * len(values))
    conn.execute(
        f"INSERT INTO job_metrics ({columns}) VALUES ({placeholders})",
        tuple(values.values()),
    )


def _rows(path, columns: str, job_type: str) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(
            f"SELECT {columns} FROM job_metrics WHERE job_type = ? ORDER BY id",
            (job_type,),
        ).fetchall()
    finally:
        conn.close()


def test_renames_legacy_product_ids_onto_the_current_ones(tmp_path):
    """metrics_0002 merges the pre-rename rows into the current product ids."""
    metrics, progress = tmp_path / "metrics.db", tmp_path / "progress_tracker.db"
    run_migrations(metrics, progress)  # schema first; 0002 then finds no rows
    conn = sqlite3.connect(str(metrics))
    _insert_job(
        conn,
        job_type="radar_DBZH",
        processor_id="radar",
        band_id="radar_DBZH",
        image_id="RMA9_DBZH_20260914T193000Z",
        product_label="Radar RMA9 DBZH · Horizontal Reflectivity",
    )
    _insert_job(
        conn,
        job_type="goes19_abi_band_13",
        processor_id="goes_band_13",
        band_id="band_13",
        product_label="GOES ABI band_13 · Cloud Tops",
    )
    _insert_job(
        conn,
        job_type="glm_folder",
        processor_id="glm_toe",
        band_id="glm_folder_toe",
        product_label="GLM Lightning (FED/TOE/MFA)",
    )
    _insert_job(
        conn,
        job_type="ecmwf_mslp_period",
        processor_id="ecmwf_mslp_processor",
        band_id="ecmwf_mslp",
        product_label="ECMWF mslp_period",
    )
    conn.commit()
    conn.close()

    # Re-running the chain applies 0002 to the rows just inserted (and 0003,
    # which then relabels the radar row by network).
    _stamp_before(metrics, "metrics_0001")
    run_migrations(metrics, progress)

    cols = "data_source_id, processor_id, band_id, product_label, image_id"
    assert _rows(metrics, cols, "radar_sinarame_dbzh") == [
        (
            "radar_sinarame_dbzh",
            "radar_sinarame",
            "radar_sinarame_dbzh",
            "Radar SINARAME dbzh · Horizontal Reflectivity",
            "RMA9_DBZH_20260914T193000Z",  # upstream filename, left alone
        )
    ]
    assert _rows(metrics, cols, "goes19_abi_c13") == [
        (
            "goes19_abi_c13",
            "goes19_abi_c13",
            "goes19_abi_c13",
            "GOES-19 ABI c13 · Cloud Tops",
            "img",
        )
    ]
    assert _rows(metrics, cols, "goes19_glm") == [
        (
            "goes19_glm",
            "goes19_glm_toe",
            "goes19_glm_toe",
            "GLM Lightning (FED/TOE/MFA)",
            "img",
        )
    ]
    assert _rows(metrics, cols, "ecmwf_ifs_mean_sea_level_pressure_period") == [
        (
            "ecmwf_ifs_mean_sea_level_pressure_period",
            "ecmwf_ifs_mean_sea_level_pressure_processor",
            "ecmwf_ifs_mean_sea_level_pressure",
            "ECMWF mean_sea_level_pressure_period",
            "img",
        )
    ]


def test_rename_leaves_rows_already_on_the_current_ids_untouched(tmp_path):
    """A row written natively under a current id must survive the rename as-is."""
    metrics, progress = tmp_path / "metrics.db", tmp_path / "progress_tracker.db"
    run_migrations(metrics, progress)
    conn = sqlite3.connect(str(metrics))
    _insert_job(
        conn,
        job_type="radar_sinarame_dbzh",
        processor_id="radar_sinarame",
        band_id="radar_sinarame_dbzh",
        product_label="Radar RMA9 dbzh · Horizontal Reflectivity",
    )
    conn.commit()
    conn.close()

    _stamp_before(metrics, "metrics_0001")
    run_migrations(metrics, progress)

    # 0002 leaves the ids alone; the label is 0003's (see the tests below).
    assert _rows(metrics, "processor_id, band_id", "radar_sinarame_dbzh") == [
        ("radar_sinarame", "radar_sinarame_dbzh")
    ]


def test_relabels_radar_rows_by_network_instead_of_station(tmp_path):
    """metrics_0003 gives every radar row of a job type the same network label."""
    metrics, progress = tmp_path / "metrics.db", tmp_path / "progress_tracker.db"
    run_migrations(metrics, progress)
    conn = sqlite3.connect(str(metrics))
    for job_type, image_id, label in (
        (
            "radar_sinarame_zdr",
            "RMA9_zdr_1",
            "Radar RMA9 zdr · Differential Reflectivity",
        ),
        (
            "radar_sinarame_zdr",
            "RMA20_zdr_2",
            "Radar RMA20 zdr · Differential Reflectivity",
        ),
        (
            "radar_inta_kdp",
            "PAR_kdp_3",
            "Radar INTA PAR kdp · Specific Differential Phase",
        ),
        (
            "radar_inta_dbzh",
            "PER_dbzh_4",
            "radar_inta_dbzh",
        ),  # written before INTA had a label
    ):
        _insert_job(conn, job_type=job_type, image_id=image_id, product_label=label)
    conn.commit()
    conn.close()

    _stamp_before(metrics, "metrics_0002")
    run_migrations(metrics, progress)

    cols = "product_label, image_id"
    assert _rows(metrics, cols, "radar_sinarame_zdr") == [
        ("Radar SINARAME zdr · Differential Reflectivity", "RMA9_zdr_1"),
        ("Radar SINARAME zdr · Differential Reflectivity", "RMA20_zdr_2"),
    ]
    assert _rows(metrics, cols, "radar_inta_kdp") == [
        ("Radar INTA kdp · Specific Differential Phase", "PAR_kdp_3")
    ]
    assert _rows(metrics, cols, "radar_inta_dbzh") == [
        ("Radar INTA dbzh · Horizontal Reflectivity", "PER_dbzh_4")
    ]


def test_relabel_leaves_other_job_types_untouched(tmp_path):
    """Only the frozen radar products are relabeled; everything else keeps its label."""
    metrics, progress = tmp_path / "metrics.db", tmp_path / "progress_tracker.db"
    run_migrations(metrics, progress)
    conn = sqlite3.connect(str(metrics))
    _insert_job(
        conn, job_type="goes19_abi_c13", product_label="GOES-19 ABI c13 · Cloud Tops"
    )
    _insert_job(
        conn, job_type="radar_sinarame_future", product_label="Radar RMA1 future · X"
    )
    conn.commit()
    conn.close()

    _stamp_before(metrics, "metrics_0002")
    run_migrations(metrics, progress)

    assert _rows(metrics, "product_label", "goes19_abi_c13") == [
        ("GOES-19 ABI c13 · Cloud Tops",)
    ]
    assert _rows(metrics, "product_label", "radar_sinarame_future") == [
        ("Radar RMA1 future · X",)
    ]
