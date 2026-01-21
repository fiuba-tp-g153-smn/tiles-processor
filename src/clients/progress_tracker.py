"""
In-progress work tracker for avoiding duplicate work units.

This module provides a simple file-based tracker to record which images
are currently being processed in the pipeline. This prevents the producer
from creating duplicate work units for images that are already in flight.

Thread/Process Safety:
    Uses file locking for safe concurrent access from multiple processes.
    Each operation reads the current state, modifies, and writes back atomically.
"""

import json
import logging
import fcntl
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Set

logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    Tracks in-progress image processing to avoid duplicate work units.

    The tracker maintains a JSON file with:
    - image_id: The image being processed
    - started_at: When processing started
    - band_id: Which band is being processed

    Stale entries (older than max_age) are automatically cleaned up.
    """

    def __init__(self, tracker_file: Path, max_age_hours: int = 2):
        """
        Initialize the progress tracker.

        Args:
            tracker_file: Path to the JSON file for tracking progress
            max_age_hours: Maximum age for entries before cleanup (default: 2 hours)
        """
        self._tracker_file = tracker_file
        self._max_age = timedelta(hours=max_age_hours)

        # Ensure parent directory exists
        self._tracker_file.parent.mkdir(parents=True, exist_ok=True)

        # Initialize file if it doesn't exist
        if not self._tracker_file.exists():
            self._write_data({})

    def _read_data(self) -> dict:
        """Read and return the tracker data with file locking."""
        try:
            with open(self._tracker_file, "r") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    data = json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                return data
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _write_data(self, data: dict) -> None:
        """Write tracker data with file locking."""
        with open(self._tracker_file, "w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                json.dump(data, f, indent=2)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _cleanup_stale(self, data: dict) -> dict:
        """Remove entries older than max_age."""
        now = datetime.now(UTC)
        cleaned = {}
        for key, entry in data.items():
            started_at = datetime.fromisoformat(entry["started_at"])
            if now - started_at < self._max_age:
                cleaned[key] = entry
            else:
                logger.debug(f"Cleaned up stale entry: {key}")
        return cleaned

    def is_in_progress(self, image_id: str, band_id: str) -> bool:
        """
        Check if an image is currently being processed.

        Args:
            image_id: The image identifier
            band_id: The band being processed

        Returns:
            True if the image is in progress, False otherwise
        """
        key = f"{band_id}:{image_id}"
        data = self._read_data()
        data = self._cleanup_stale(data)
        return key in data

    def mark_in_progress(self, image_id: str, band_id: str) -> None:
        """
        Mark an image as in-progress.

        Args:
            image_id: The image identifier
            band_id: The band being processed
        """
        key = f"{band_id}:{image_id}"
        data = self._read_data()
        data = self._cleanup_stale(data)
        data[key] = {
            "image_id": image_id,
            "band_id": band_id,
            "started_at": datetime.now(UTC).isoformat(),
        }
        self._write_data(data)
        logger.debug(f"Marked in progress: {key}")

    def mark_completed(self, image_id: str, band_id: str) -> None:
        """
        Mark an image as completed (remove from in-progress).

        Args:
            image_id: The image identifier
            band_id: The band being processed
        """
        key = f"{band_id}:{image_id}"
        data = self._read_data()
        if key in data:
            del data[key]
            self._write_data(data)
            logger.debug(f"Marked completed: {key}")

    def get_in_progress_images(self, band_id: str) -> Set[str]:
        """
        Get all in-progress image IDs for a band.

        Args:
            band_id: The band to check

        Returns:
            Set of image IDs currently in progress
        """
        data = self._read_data()
        data = self._cleanup_stale(data)

        in_progress = set()
        for key, entry in data.items():
            if entry["band_id"] == band_id:
                in_progress.add(entry["image_id"])
        return in_progress

    def clear_all(self) -> None:
        """Clear all in-progress entries."""
        self._write_data({})
        logger.info("Cleared all in-progress entries")
