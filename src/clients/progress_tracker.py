"""
In-progress work tracker using SQLite for atomic operations.

This module provides a robust tracker to record which images are currently
being processed. It uses SQLite to ensure atomic transactions and safe
concurrenct access from multiple processes/workers.
"""

import logging
import sqlite3
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Set

logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    Tracks in-progress image processing to avoid duplicate work units.

    Uses SQLite for atomic operations and concurrency safety.
    Tracks:
    - image_id: The image being processed
    - band_id: Which band is being processed
    - status: Current status (IN_PROGRESS, COMPLETED)
    - created_at: Timestamp when processing started
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

        # Ensure parent directory exists
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(
            str(self._db_path),
            timeout=30.0,  # Wait up to 30s for lock
            isolation_level=None,  # Autocommit mode
        )
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_images (
                    image_id TEXT NOT NULL,
                    band_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (image_id, band_id)
                )
            """
            )

            # Index for cleanup queries
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON processed_images(created_at)
            """
            )

            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")

    def _cleanup_stale(self) -> None:
        """Remove entries in PROCESSING state older than ttl."""
        cutoff = datetime.now(UTC) - self._ttl
        with self._get_connection() as conn:
            # Only clean up items that are PROCESSING and older than TTL
            # IN_PROGRESS items (in queue) should NOT be expired
            conn.execute(
                "DELETE FROM processed_images WHERE status = 'PROCESSING' AND updated_at < ?",
                (cutoff,),
            )

    def is_in_progress(self, image_id: str, band_id: str) -> bool:
        """Check if an image is currently being processed."""
        self._cleanup_stale()  # Clean up before checking

        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM processed_images WHERE image_id = ? AND band_id = ?",
                (image_id, band_id),
            )
            return cursor.fetchone() is not None

    def mark_in_progress(self, image_id: str, band_id: str) -> None:
        """Mark an image as in-progress."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO processed_images 
                (image_id, band_id, status, created_at, updated_at)
                VALUES (?, ?, 'IN_PROGRESS', ?, ?)
                """,
                (image_id, band_id, datetime.now(UTC), datetime.now(UTC)),
            )

    def mark_processing(self, image_id: str, band_id: str) -> None:
        """Mark an image as currently being processed by a worker."""
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE processed_images 
                SET status = 'PROCESSING', updated_at = ?
                WHERE image_id = ? AND band_id = ?
                """,
                (datetime.now(UTC), image_id, band_id),
            )
        logger.debug(f"Marked as processing: {band_id}:{image_id}")

    def mark_completed(self, image_id: str, band_id: str) -> None:
        """Mark an image as completed (remove from tracking)."""
        with self._get_connection() as conn:
            conn.execute(
                "DELETE FROM processed_images WHERE image_id = ? AND band_id = ?",
                (image_id, band_id),
            )
        logger.debug(f"Marked completed: {band_id}:{image_id}")

    def get_in_progress_images(self, band_id: str) -> Set[str]:
        """Get all in-progress image IDs for a band."""
        self._cleanup_stale()

        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT image_id FROM processed_images WHERE band_id = ?", (band_id,)
            )
            return {row["image_id"] for row in cursor.fetchall()}

    def clear_all(self) -> None:
        """Clear all entries."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM processed_images")
        logger.info("Cleared all entries from tracker")
