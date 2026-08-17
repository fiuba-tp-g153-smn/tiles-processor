"""Tests for ImageProcessor's per-attempt work-dir scoping (BUG-04).

Every processor keys its scratch dir on ``_work_dir_leaf(work_unit)``. Without a
bound token it is the bare ``image_id`` (direct/test calls); with one it is
``{image_id}-{token}`` so two concurrent copies of the same image never share —
and rmtree — a work dir. Only ``image_id`` is read, so a light stand-in suffices.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from processors.base_processor import ImageProcessor


class _Concrete(ImageProcessor):
    """Minimal concrete processor so the ABC can be instantiated."""

    async def process(self, downloaded_file_path, work_unit):  # pragma: no cover
        raise NotImplementedError


def _proc() -> _Concrete:
    return _Concrete(SimpleNamespace(TMP_DIR="/tmp"))


def _unit(image_id: str = "img-42") -> SimpleNamespace:
    return SimpleNamespace(image_id=image_id)


def test_leaf_without_token_is_bare_image_id():
    assert _proc()._work_dir_leaf(_unit()) == "img-42"


def test_leaf_with_token_is_scoped():
    proc = _proc()
    proc.bind_work_token("abc123")
    assert proc._work_dir_leaf(_unit()) == "img-42-abc123"


def test_concurrent_copies_get_distinct_leaves():
    """Same image, two attempts → different scratch dirs (no clobber)."""
    unit = _unit("same-image")
    a, b = _proc(), _proc()
    a.bind_work_token("aaaa1111")
    b.bind_work_token("bbbb2222")

    assert a._work_dir_leaf(unit) != b._work_dir_leaf(unit)
    assert a._work_dir_leaf(unit) == "same-image-aaaa1111"


def test_empty_token_falls_back_to_bare_id():
    """An empty token behaves like no token (defensive: never a bare '-' suffix)."""
    proc = _proc()
    proc.bind_work_token("")
    assert proc._work_dir_leaf(_unit()) == "img-42"
