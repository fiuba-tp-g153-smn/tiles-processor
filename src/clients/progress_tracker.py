"""
In-progress work tracker using SQLite for atomic operations.

This module records which images are currently queued or being processed so the
producer can deduplicate work. It uses SQLite (WAL, 30s busy timeout, autocommit)
for safe concurrent access from multiple processes/workers.

State machine:
- ``IN_PROGRESS``: queued by the producer, not yet picked up — never expired.
- ``PROCESSING``: a worker is actively on it — reclaimed by ``cleanup_stale`` once
  it sits untouched longer than the TTL (covers crashes / dead-letter / stuck retries).

The database file must live on a local shared volume (same host as the producer,
workers and dashboard); SQLite file locking / WAL do not work over a network
filesystem (NFS/SMB).
"""

import logging
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Set

from clients.sqlite_utils import sqlite_connection

logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    Tracks queued/in-progress image processing to avoid duplicate work units.

    Uses SQLite for atomic operations and concurrency safety.
    Tracks:
    - image_id: The image being processed
    - band_id: Which band is being processed
    - status: Current status (IN_PROGRESS, PROCESSING)
    - created_at: Timestamp when the entry was created
    - updated_at: Timestamp of last update
    """

    def __init__(
        self,
        db_path: Path,
        ttl: timedelta | None = None,
        max_age_hours: int | None = None,
    ):
        """
        Initialize the progress tracker.

        Args:
            db_path: Path to the SQLite database file
            ttl: Maximum age for entries before cleanup (takes precedence over max_age_hours)
            max_age_hours: Deprecated. Use ttl instead. Maximum age for entries before cleanup.
        """
        self._db_path = db_path.with_suffix(".db")  # Ensure .db extension

        if ttl is not None:
            self._ttl = ttl
        elif max_age_hours is not None:
            self._ttl = timedelta(hours=max_age_hours)
        else:
            self._ttl = timedelta(hours=2)  # Default

        # Ensure parent directory exists. The schema itself is owned by Alembic
        # (see migrations/progress) and applied by the one-shot ``migrate`` step.
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        """Open a short-lived connection (see ``clients.sqlite_utils``)."""
        return sqlite_connection(self._db_path)

    def cleanup_stale(self) -> None:
        """Reclaim entries stuck in PROCESSING longer than the TTL.

        Only PROCESSING rows expire — IN_PROGRESS rows (queued, not yet picked
        up) are left alone. Call this periodically (e.g. once per producer tick),
        NOT on read paths, so lookups stay pure reads and don't take write locks.
        """
        cutoff = datetime.now(UTC) - self._ttl
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM processed_images WHERE status = 'PROCESSING' AND updated_at < ?",
                (cutoff,),
            )

    def mark_in_progress(self, image_id: str, band_id: str) -> None:
        """Mark an image as in-progress (queued by the producer)."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO processed_images
                (image_id, band_id, status, created_at, updated_at)
                VALUES (?, ?, 'IN_PROGRESS', ?, ?)
                """,
                (image_id, band_id, datetime.now(UTC), datetime.now(UTC)),
            )

    def mark_processing(self, image_id: str, band_id: str) -> None:
        """Mark an image as currently being processed by a worker.

        Transitions the row to PROCESSING and refreshes ``updated_at``, arming the
        TTL: if the worker then crashes / dead-letters / stalls, ``cleanup_stale``
        reclaims the row so the image becomes rediscoverable.
        """
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE processed_images
                SET status = 'PROCESSING', updated_at = ?
                WHERE image_id = ? AND band_id = ?
                """,
                (datetime.now(UTC), image_id, band_id),
            )
        logger.debug("Marked as processing: %s:%s", band_id, image_id)

    def mark_completed(self, image_id: str, band_id: str) -> None:
        """Mark an image as completed (remove from tracking)."""
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM processed_images WHERE image_id = ? AND band_id = ?",
                (image_id, band_id),
            )
        logger.debug("Marked completed: %s:%s", band_id, image_id)

    def get_in_progress_images(self, band_id: str) -> Set[str]:
        """Get all tracked image IDs for a band (pure read, no cleanup).

        Stale-entry reclamation is done separately via ``cleanup_stale`` so this
        lookup never acquires a write lock.
        """
        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT image_id FROM processed_images WHERE band_id = ?", (band_id,)
            )
            return {row["image_id"] for row in cursor.fetchall()}

    def list_in_progress(self) -> list[dict]:
        """Return all tracked entries, newest first (read-only, no TTL cleanup).

        Used by the dashboard's live view; deliberately avoids ``cleanup_stale``
        so a read never mutates the workers' shared state.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT image_id, band_id, status, created_at, updated_at "
                "FROM processed_images ORDER BY updated_at DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def clear_all(self) -> None:
        """Clear all entries."""
        with self._connect() as conn:
            conn.execute("DELETE FROM processed_images")
        logger.info("Cleared all entries from tracker")
