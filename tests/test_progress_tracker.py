import os
import sys
import threading
from datetime import timedelta

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.progress_tracker import ProgressTracker
from db.migrate import run_migrations


@pytest.fixture(autouse=True)
def _migrate_schema(tmp_path):
    """Alembic owns the schema now; apply it to the temp ``p.db`` before each test."""
    run_migrations(tmp_path / "m.db", tmp_path / "p.db")


def _status(tracker: ProgressTracker, image_id: str) -> str | None:
    for row in tracker.list_in_progress():
        if row["image_id"] == image_id:
            return row["status"]
    return None


def test_status_transitions(tmp_path):
    tracker = ProgressTracker(tmp_path / "p.db")

    tracker.mark_in_progress("img1", "band_13")
    assert _status(tracker, "img1") == "IN_PROGRESS"

    tracker.mark_processing("img1", "band_13")
    assert _status(tracker, "img1") == "PROCESSING"

    tracker.mark_completed("img1", "band_13")
    assert tracker.list_in_progress() == []


def test_get_in_progress_images_filters_by_band(tmp_path):
    tracker = ProgressTracker(tmp_path / "p.db")
    tracker.mark_in_progress("a", "band_13")
    tracker.mark_in_progress("b", "band_9")

    assert tracker.get_in_progress_images("band_13") == {"a"}
    assert tracker.get_in_progress_images("band_9") == {"b"}


def test_cleanup_reclaims_stale_processing(tmp_path):
    # ttl=0 => any PROCESSING row is immediately past its deadline.
    tracker = ProgressTracker(tmp_path / "p.db", ttl=timedelta(seconds=0))
    tracker.mark_in_progress("img1", "band_13")
    tracker.mark_processing("img1", "band_13")

    tracker.cleanup_stale()
    assert tracker.get_in_progress_images("band_13") == set()


def test_cleanup_spares_fresh_processing(tmp_path):
    # A wide TTL keeps a just-marked PROCESSING row alive.
    tracker = ProgressTracker(tmp_path / "p.db", ttl=timedelta(hours=1))
    tracker.mark_in_progress("img1", "band_13")
    tracker.mark_processing("img1", "band_13")

    tracker.cleanup_stale()
    assert tracker.get_in_progress_images("band_13") == {"img1"}


def test_cleanup_spares_in_progress_even_when_old(tmp_path):
    # IN_PROGRESS (queued, not yet picked up) must never be reclaimed by the TTL.
    tracker = ProgressTracker(tmp_path / "p.db", ttl=timedelta(seconds=0))
    tracker.mark_in_progress("img1", "band_13")

    tracker.cleanup_stale()
    assert tracker.get_in_progress_images("band_13") == {"img1"}


def test_reads_do_not_mutate_state(tmp_path):
    """get_in_progress_images / list_in_progress are pure reads (no cleanup).

    Even with a stale PROCESSING row and ttl=0, a read must not reclaim it —
    only the explicit cleanup_stale() does.
    """
    tracker = ProgressTracker(tmp_path / "p.db", ttl=timedelta(seconds=0))
    tracker.mark_in_progress("img1", "band_13")
    tracker.mark_processing("img1", "band_13")

    assert tracker.get_in_progress_images("band_13") == {"img1"}
    assert tracker.get_in_progress_images("band_13") == {"img1"}
    assert len(tracker.list_in_progress()) == 1

    tracker.cleanup_stale()  # only this removes it
    assert tracker.get_in_progress_images("band_13") == set()


def test_concurrent_marks_do_not_collide(tmp_path):
    """Several worker-like threads, each its own tracker, write concurrently.

    With WAL + 30s busy timeout no write should be lost or raise locked.
    """
    db_path = tmp_path / "p.db"
    ProgressTracker(db_path)  # initialize schema once

    writers = 5
    per_writer = 40
    errors: list[Exception] = []

    def worker(worker_idx: int):
        tracker = ProgressTracker(db_path)
        try:
            for i in range(per_writer):
                image_id = f"w{worker_idx}-img{i}"
                tracker.mark_in_progress(image_id, "band_13")
                tracker.mark_processing(image_id, "band_13")
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(writers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(ProgressTracker(db_path).get_in_progress_images("band_13")) == (
        writers * per_writer
    )
