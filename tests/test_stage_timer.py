import json
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from processors.base_processor import ImageProcessor


class _DummyProcessor(ImageProcessor):
    async def process(self, downloaded_file_path, work_unit):  # pragma: no cover
        return None


def _make_processor(tmp_path):
    return _DummyProcessor(SimpleNamespace(TMP_DIR=str(tmp_path)))


def test_time_stage_accumulates_same_name(tmp_path):
    proc = _make_processor(tmp_path)
    with proc._time_stage("a"):
        time.sleep(0.01)
    with proc._time_stage("a"):
        time.sleep(0.01)
    with proc._time_stage("b"):
        time.sleep(0.005)

    sink = tmp_path / "m.json"
    proc.bind_metrics_sink(sink)
    proc.flush_metrics()

    data = json.loads(sink.read_text())
    assert set(data) == {"a", "b"}
    assert data["a"] >= 0.02  # two intervals summed
    assert data["b"] >= 0.005


def test_flush_is_noop_without_timings(tmp_path):
    proc = _make_processor(tmp_path)
    sink = tmp_path / "m.json"
    proc.bind_metrics_sink(sink)
    proc.flush_metrics()
    assert not sink.exists()


def test_flush_is_noop_without_sink(tmp_path):
    proc = _make_processor(tmp_path)
    with proc._time_stage("a"):
        pass
    # No sink bound -> must not raise
    proc.flush_metrics()


def test_partial_timings_survive_failure(tmp_path):
    proc = _make_processor(tmp_path)
    sink = tmp_path / "m.json"
    proc.bind_metrics_sink(sink)
    try:
        with proc._time_stage("georef"):
            time.sleep(0.005)
        with proc._time_stage("boom"):
            raise RuntimeError("mid-pipeline")
    except RuntimeError:
        proc.flush_metrics()  # subprocess does this in a finally

    data = json.loads(sink.read_text())
    assert "georef" in data and "boom" in data
