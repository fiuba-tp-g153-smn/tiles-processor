import os
import sys
from datetime import UTC, datetime
from itertools import accumulate

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from metrics_api.timeline_codec import FORMAT_VERSION, encode_timeline


def _row(
    job_id, job_type, start, finish, outcome="success", worker="worker1", label=None
):
    return {
        "id": job_id,
        "job_type": job_type,
        "product_label": label or f"{job_type} label",
        "outcome": outcome,
        "worker_host": worker,
        "started_at": start,
        "finished_at": finish,
    }


def _decode(payload):
    """Reference decoder (mirrors the visualizer's) back to comparable rows."""
    ids = list(accumulate(payload["id"]))
    starts = list(accumulate(payload["start"], initial=payload["t0"]))[1:]
    rows = []
    for i, job_id in enumerate(ids):
        start = starts[i]
        rows.append(
            {
                "id": job_id,
                "job_type": payload["types"][payload["type"][i]],
                "product_label": payload["labels"][payload["type"][i]],
                "outcome": payload["outcomes"][payload["outcome"][i]],
                "worker_host": payload["workers"][payload["worker"][i]],
                "start_ms": start,
                "finish_ms": start + payload["dur"][i],
            }
        )
    return rows


def _ms(iso):
    return round(datetime.fromisoformat(iso).timestamp() * 1000)


ROWS = [
    # Deliberately out of start order: the encoder sorts by started_at.
    _row(12, "radar", "2026-06-04T00:00:05.250000+00:00", "2026-06-04T00:00:09+00:00"),
    _row(10, "goes", "2026-06-04T00:00:00+00:00", "2026-06-04T00:00:44.5+00:00"),
    _row(
        11,
        "radar",
        "2026-06-04T00:00:01+00:00",
        "2026-06-04T00:00:03+00:00",
        "dlq",
        None,
    ),
]


def test_round_trip_reproduces_rows_sorted_by_start():
    decoded = _decode(encode_timeline(ROWS))

    expected = sorted(ROWS, key=lambda r: _ms(r["started_at"]))
    assert [d["id"] for d in decoded] == [10, 11, 12]
    for got, row in zip(decoded, expected):
        assert got["job_type"] == row["job_type"]
        assert got["outcome"] == row["outcome"]
        assert got["worker_host"] == row["worker_host"]
        assert got["start_ms"] == _ms(row["started_at"])
        assert got["finish_ms"] == _ms(row["finished_at"])


def test_header_and_dictionaries():
    payload = encode_timeline(ROWS)

    assert payload["v"] == FORMAT_VERSION
    assert payload["t0"] == _ms("2026-06-04T00:00:00+00:00")
    assert payload["types"] == ["goes", "radar"]
    assert payload["outcomes"] == ["success", "dlq"]
    assert payload["workers"] == ["worker1", None]  # null worker kept as a value


def test_ids_and_starts_are_delta_encoded():
    payload = encode_timeline(ROWS)

    assert payload["id"] == [10, 1, 1]
    assert payload["start"] == [0, 1000, 4250]
    assert payload["dur"] == [44500, 2000, 3750]


def test_label_comes_from_the_latest_row_of_each_type():
    rows = [
        _row(
            1,
            "radar",
            "2026-06-04T00:00:00+00:00",
            "2026-06-04T00:00:01+00:00",
            label="old",
        ),
        _row(
            2,
            "radar",
            "2026-06-04T01:00:00+00:00",
            "2026-06-04T01:00:01+00:00",
            label="new",
        ),
    ]

    assert encode_timeline(rows)["labels"] == ["new"]


def test_naive_timestamps_are_taken_as_utc():
    rows = [_row(1, "goes", "2026-06-04T00:00:00", "2026-06-04T00:00:02")]

    expected = round(datetime(2026, 6, 4, tzinfo=UTC).timestamp() * 1000)
    assert encode_timeline(rows)["t0"] == expected


def test_empty_input_yields_empty_columns():
    payload = encode_timeline([])

    assert payload["t0"] is None
    for key in ("types", "labels", "outcomes", "workers", "id", "type", "start", "dur"):
        assert payload[key] == []
