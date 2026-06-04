"""Persistent store for per-job performance metrics (SQLite, WAL mode).

Mirrors the proven concurrency setup of ``progress_tracker.py``: WAL journal,
30s busy timeout and autocommit. Only worker processes write here (one small
INSERT per finished job), while the dashboard reads concurrently — a load WAL
handles comfortably. The database file must live on a local shared volume
(``${TMP_DIR}/metrics.db``), never a network filesystem.
"""

import json
import logging
import math
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any

from models.job_metrics import JobMetrics

logger = logging.getLogger(__name__)

# Outcome values aggregated into per-type counts (see models.job_metrics).
_OUTCOMES = ("success", "error", "dlq", "requeued", "skipped")


class MetricsRepository:
    """Read/write access to the ``job_metrics`` table."""

    # Columns persisted from JobMetrics, in INSERT order. ``stage_timings`` is
    # serialized separately (dict -> JSON), so it is excluded here.
    _COLUMNS = (
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
    )

    def __init__(self, db_path: Path):
        self._db_path = db_path.with_suffix(".db")  # Ensure .db extension
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Open a connection with the same settings as ProgressTracker."""
        conn = sqlite3.connect(
            str(self._db_path),
            timeout=30.0,  # Wait up to 30s for a write lock
            isolation_level=None,  # Autocommit mode
        )
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create the schema and enable WAL for concurrent access."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_unit_id TEXT,
                    image_id TEXT NOT NULL,
                    data_source_id TEXT NOT NULL,
                    processor_id TEXT,
                    band_id TEXT,
                    job_type TEXT NOT NULL,
                    product_label TEXT,
                    image_timestamp TEXT,
                    outcome TEXT NOT NULL,
                    worker_host TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    download_s REAL,
                    process_s REAL,
                    total_s REAL,
                    stage_timings_json TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_type_finished "
                "ON job_metrics(job_type, finished_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_finished "
                "ON job_metrics(finished_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_outcome "
                "ON job_metrics(outcome)"
            )
            conn.execute("PRAGMA journal_mode=WAL")

    def record(self, metrics: JobMetrics) -> None:
        """Insert one finished-job record.

        Never raises on a write failure — metrics must not break processing.
        """
        data = asdict(metrics)
        values = [data[col] for col in self._COLUMNS]
        values.append(
            json.dumps(metrics.stage_timings) if metrics.stage_timings else None
        )
        placeholders = ", ".join("?" for _ in range(len(self._COLUMNS) + 1))
        columns = ", ".join((*self._COLUMNS, "stage_timings_json"))

        try:
            with self._get_connection() as conn:
                conn.execute(
                    f"INSERT INTO job_metrics ({columns}) VALUES ({placeholders})",
                    values,
                )
            logger.debug(
                "Recorded metrics: %s/%s -> %s",
                metrics.job_type,
                metrics.image_id,
                metrics.outcome,
            )
        except sqlite3.Error as exc:
            logger.warning("Failed to record job metrics: %s", exc)

    # ------------------------------------------------------------------ reads

    def summary(self, since: str | None = None) -> list[dict[str, Any]]:
        """Aggregate per-job-type statistics for the dashboard.

        Counts cover every outcome; timing statistics (avg/min/max/p95) are
        computed over successful jobs only, since failed jobs carry partial
        timings. Returns one entry per job_type, busiest first.
        """
        rows = self._select(
            "SELECT job_type, product_label, outcome, finished_at, "
            "download_s, process_s, total_s, stage_timings_json "
            "FROM job_metrics",
            since,
        )

        grouped: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            grouped.setdefault(row["job_type"], []).append(row)

        summaries = [self._summarize_type(jt, rs) for jt, rs in grouped.items()]
        summaries.sort(key=lambda s: s["counts"]["total"], reverse=True)
        return summaries

    def recent_jobs(
        self,
        limit: int = 100,
        job_type: str | None = None,
        outcome: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the most recently finished jobs, newest first."""
        clauses = []
        params: list[Any] = []
        if job_type:
            clauses.append("job_type = ?")
            params.append(job_type)
        if outcome:
            clauses.append("outcome = ?")
            params.append(outcome)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(limit, 1000)))

        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM job_metrics {where} "
                "ORDER BY finished_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._job_to_dict(row) for row in rows]

    def throughput(
        self, bucket: str = "hour", since: str | None = None
    ) -> list[dict[str, Any]]:
        """Count finished jobs per time bucket per job_type.

        ISO8601 timestamps sort lexically, so a substring is a valid bucket key:
        first 13 chars => hour ("2026-06-04T00"), first 10 => day.
        """
        width = 10 if bucket == "day" else 13
        where = "WHERE finished_at >= ?" if since else ""
        params = [since] if since else []
        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT substr(finished_at, 1, {width}) AS bucket, job_type, "
                f"COUNT(*) AS count FROM job_metrics {where} "
                "GROUP BY bucket, job_type ORDER BY bucket",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def _select(self, query: str, since: str | None) -> list[sqlite3.Row]:
        """Run a SELECT optionally filtered to finished_at >= since."""
        if since:
            query += " WHERE finished_at >= ?"
            params: list[Any] = [since]
        else:
            params = []
        with self._get_connection() as conn:
            return conn.execute(query, params).fetchall()

    def _summarize_type(self, job_type: str, rows: list[sqlite3.Row]) -> dict[str, Any]:
        """Build one job_type summary entry from its rows."""
        counts = {oc: 0 for oc in _OUTCOMES}
        for row in rows:
            if row["outcome"] in counts:
                counts[row["outcome"]] += 1
        counts["total"] = len(rows)

        ok = [r for r in rows if r["outcome"] == "success"]
        failed = counts["error"] + counts["dlq"]
        latest = max(rows, key=lambda r: r["finished_at"])

        return {
            "job_type": job_type,
            "product_label": latest["product_label"],
            "counts": counts,
            "error_rate": (failed / counts["total"]) if counts["total"] else 0.0,
            "last_finished": latest["finished_at"],
            "total_s": self._stats([r["total_s"] for r in ok]),
            "download_s": self._stats([r["download_s"] for r in ok]),
            "process_s": self._stats([r["process_s"] for r in ok]),
            "stages": self._avg_stages(ok),
        }

    @staticmethod
    def _avg_stages(rows: list[sqlite3.Row]) -> dict[str, float]:
        """Average per-stage durations across the given rows."""
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}
        for row in rows:
            raw = row["stage_timings_json"]
            if not raw:
                continue
            try:
                stages = json.loads(raw)
            except ValueError:
                continue
            for name, seconds in stages.items():
                sums[name] = sums.get(name, 0.0) + seconds
                counts[name] = counts.get(name, 0) + 1
        return {name: sums[name] / counts[name] for name in sums}

    @classmethod
    def _stats(cls, values: list[Any]) -> dict[str, float | None]:
        """Compute avg/min/max/p95 over non-null values."""
        nums = [v for v in values if v is not None]
        if not nums:
            return {"avg": None, "min": None, "max": None, "p95": None}
        return {
            "avg": sum(nums) / len(nums),
            "min": min(nums),
            "max": max(nums),
            "p95": cls._percentile(nums, 0.95),
        }

    @staticmethod
    def _percentile(values: list[float], pct: float) -> float:
        """Linear-interpolation percentile (pct in [0, 1])."""
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        k = (len(ordered) - 1) * pct
        lo = math.floor(k)
        hi = math.ceil(k)
        if lo == hi:
            return ordered[int(k)]
        return ordered[lo] * (hi - k) + ordered[hi] * (k - lo)

    @staticmethod
    def _job_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        """Convert a job_metrics row to a dict, parsing the stage-timings JSON."""
        job = dict(row)
        raw = job.pop("stage_timings_json", None)
        try:
            job["stage_timings"] = json.loads(raw) if raw else {}
        except ValueError:
            job["stage_timings"] = {}
        return job
