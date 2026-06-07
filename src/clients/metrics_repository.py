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

from clients.sqlite_utils import sqlite_connection
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
        # Ensure parent directory exists. The schema itself is owned by Alembic
        # (see migrations/metrics) and applied by the one-shot ``migrate`` step.
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        """Open a short-lived connection (see ``clients.sqlite_utils``)."""
        return sqlite_connection(self._db_path)

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
            with self._connect() as conn:
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

    def prune_to_max_rows(self, max_rows: int) -> int:
        """Keep only the most recent ``max_rows`` rows (by id); delete older ones.

        ``id`` is an autoincrement PK, so the newest rows have the highest ids. We
        find the id of the ``max_rows``-th newest row and delete everything below
        it — a fast primary-key range delete. Returns the number of rows deleted.
        """
        if max_rows <= 0:
            return 0
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM job_metrics ORDER BY id DESC LIMIT 1 OFFSET ?",
                (max_rows - 1,),
            ).fetchone()
            if row is None:
                return 0  # table holds <= max_rows rows — nothing to prune
            deleted = conn.execute(
                "DELETE FROM job_metrics WHERE id < ?", (row["id"],)
            ).rowcount
        if deleted:
            logger.info("Pruned %d job_metrics row(s); capped at %d", deleted, max_rows)
        return deleted

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

    def recent_jobs(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        limit: int = 100,
        job_type: str | None = None,
        outcome: str | None = None,
        offset: int = 0,
        since: str | None = None,
        before: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return finished jobs newest-first, with limit/offset for pagination.

        `since`/`before` (ISO8601 cutoffs) keep the half-open window
        ``since <= finished_at < before`` — the timeline's lazy loader fetches
        older chunks with both set; ISO8601 sorts lexically so string compares work.
        `limit <= 0` means no limit (all matching rows) via SQLite ``LIMIT -1`` —
        used by the timeline to load a full window; positive limits cap at 1000.
        """
        clauses = []
        params: list[Any] = []
        if job_type:
            clauses.append("job_type = ?")
            params.append(job_type)
        if outcome:
            clauses.append("outcome = ?")
            params.append(outcome)
        if since:
            clauses.append("finished_at >= ?")
            params.append(since)
        if before:
            clauses.append("finished_at < ?")
            params.append(before)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(-1 if limit <= 0 else max(1, min(limit, 1000)))
        params.append(max(0, offset))

        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM job_metrics {where} "
                "ORDER BY finished_at DESC LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return [self._job_to_dict(row) for row in rows]

    def timing_series(
        self, bucket: str = "hour", since: str | None = None
    ) -> list[dict[str, Any]]:
        """Per time-bucket, per job_type timing for the trend charts.

        Returns avg/p95 total seconds, job count, and average per-stage seconds
        for each (bucket, job_type), computed over successful jobs only. Powers
        the timing-evolution, p95-trend and per-stage stacked-area charts.
        """
        width = {"day": 10, "10min": 15}.get(bucket, 13)
        clauses = ["outcome = 'success'"]
        params: list[Any] = []
        if since:
            clauses.append("finished_at >= ?")
            params.append(since)
        where = " AND ".join(clauses)

        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT substr(finished_at, 1, {width}) AS bucket, job_type, "
                f"total_s, stage_timings_json FROM job_metrics WHERE {where} "
                "ORDER BY bucket",
                params,
            ).fetchall()

        groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            groups.setdefault((row["bucket"], row["job_type"]), []).append(row)

        series = []
        for (bkt, jt), rs in groups.items():
            stats = self._stats([r["total_s"] for r in rs])
            series.append(
                {
                    "bucket": bkt,
                    "job_type": jt,
                    "count": len(rs),
                    "avg_total_s": stats["avg"],
                    "p95_total_s": stats["p95"],
                    "stages": self._avg_stages(rs),
                }
            )
        series.sort(key=lambda d: (d["bucket"], d["job_type"]))
        return series

    def throughput(
        self, bucket: str = "hour", since: str | None = None
    ) -> list[dict[str, Any]]:
        """Count finished jobs per time bucket per job_type.

        ISO8601 timestamps sort lexically, so a substring is a valid bucket key:
        first 15 chars => 10-min ("2026-06-04T00:3"), 13 => hour, 10 => day.
        """
        width = {"day": 10, "10min": 15}.get(bucket, 13)
        where = "WHERE finished_at >= ?" if since else ""
        params = [since] if since else []
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT substr(finished_at, 1, {width}) AS bucket, job_type, "
                f"COUNT(*) AS count FROM job_metrics {where} "
                "GROUP BY bucket, job_type ORDER BY bucket",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    # --------------------------------------------------------- export / import

    def schema_version(self) -> str | None:
        """Current Alembic revision stamped in the database (None if unmanaged).

        This is the schema "version" carried by an export so an import can refuse
        a payload shaped for a different schema.
        """
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
        except sqlite3.Error:
            return None
        return row["version_num"] if row else None

    def export_jobs(self, since: str | None = None) -> list[dict[str, Any]]:
        """Return every ``job_metrics`` row matching the window, newest first."""
        rows = self._select("SELECT * FROM job_metrics", since)
        jobs = [self._job_to_dict(row) for row in rows]
        jobs.sort(key=lambda job: job["finished_at"], reverse=True)
        return jobs

    def import_jobs(self, jobs: list[dict[str, Any]]) -> dict[str, int]:
        """Idempotently insert exported job rows, skipping duplicates.

        Duplicates are detected by the natural key
        ``(work_unit_id, image_id, finished_at, outcome)`` against both the
        existing table and rows already accepted in this batch, so re-importing
        the same export is a no-op. The source ``id`` is ignored (a fresh
        autoincrement is assigned) and ``stage_timings`` is re-serialized to JSON.

        Returns ``{"inserted": n, "skipped": m}``.
        """
        columns = ", ".join((*self._COLUMNS, "stage_timings_json"))
        placeholders = ", ".join("?" for _ in range(len(self._COLUMNS) + 1))
        insert_sql = f"INSERT INTO job_metrics ({columns}) VALUES ({placeholders})"

        skipped = 0
        with self._connect() as conn:
            seen = {
                (r["work_unit_id"], r["image_id"], r["finished_at"], r["outcome"])
                for r in conn.execute(
                    "SELECT work_unit_id, image_id, finished_at, outcome FROM job_metrics"
                )
            }
            to_insert: list[list[Any]] = []
            for job in jobs:
                key = (
                    job.get("work_unit_id"),
                    job.get("image_id"),
                    job.get("finished_at"),
                    job.get("outcome"),
                )
                if key in seen:
                    skipped += 1
                    continue
                seen.add(key)
                values = [job.get(col) for col in self._COLUMNS]
                stages = job.get("stage_timings")
                values.append(json.dumps(stages) if stages else None)
                to_insert.append(values)

            if to_insert:
                try:
                    conn.execute("BEGIN")
                    conn.executemany(insert_sql, to_insert)
                    conn.execute("COMMIT")
                except sqlite3.Error:
                    conn.execute("ROLLBACK")
                    raise

        return {"inserted": len(to_insert), "skipped": skipped}

    def _select(self, query: str, since: str | None) -> list[sqlite3.Row]:
        """Run a SELECT optionally filtered to finished_at >= since."""
        if since:
            query += " WHERE finished_at >= ?"
            params: list[Any] = [since]
        else:
            params = []
        with self._connect() as conn:
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
