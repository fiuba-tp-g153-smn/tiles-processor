"""Compact columnar encoding of finished jobs for the dashboard timeline.

The timeline draws thousands of bars per window but needs only a handful of
fields per job, so ``GET /api/timeline`` ships them as parallel arrays instead
of one object per row. Strings repeated on every row (job type, label, outcome,
worker) are sent once and referenced by index; ids and start times are
delta-encoded so they compress to almost nothing. ``total_s`` is not sent: it
matches ``finished_at - started_at`` to the millisecond, so the client derives
it from ``dur``.

Format v1, rows sorted by start time::

    v, t0                    format version; epoch ms of the earliest start
    types, labels            job types and, parallel, each type's latest label
    outcomes, workers        distinct outcomes / worker hosts (may hold null)
    type, outcome, worker    per-row index into the lists above
    id, start                first value absolute (start: relative to t0),
                             then the difference from the previous row
    dur                      finished_at - started_at, in ms
"""

import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

FORMAT_VERSION = 1

Row = Mapping[str, Any] | sqlite3.Row


def encode_timeline(rows: Sequence[Row]) -> dict[str, Any]:
    """Encode finished-job rows into the v1 columnar timeline payload."""
    starts = [_epoch_ms(row["started_at"]) for row in rows]
    order = sorted(range(len(rows)), key=starts.__getitem__)
    ordered = [rows[i] for i in order]
    start_ms = [starts[i] for i in order]
    t0 = start_ms[0] if start_ms else None

    labels = {row["job_type"]: row["product_label"] for row in ordered}
    payload: dict[str, Any] = {
        "v": FORMAT_VERSION,
        "t0": t0,
        "types": list(labels),
        "labels": list(labels.values()),
        "outcomes": _distinct(row["outcome"] for row in ordered),
        "workers": _distinct(row["worker_host"] for row in ordered),
    }
    payload.update(_columns(ordered, start_ms, payload))
    return payload


def _columns(
    rows: Sequence[Row], start_ms: list[int], dicts: Mapping[str, Any]
) -> dict[str, list[int]]:
    """Per-row arrays: dictionary indices, delta-encoded ids/starts, durations."""
    return {
        "id": _deltas([row["id"] for row in rows], origin=0),
        "type": _indices(rows, "job_type", dicts["types"]),
        "outcome": _indices(rows, "outcome", dicts["outcomes"]),
        "worker": _indices(rows, "worker_host", dicts["workers"]),
        "start": _deltas(start_ms, origin=start_ms[0] if start_ms else 0),
        "dur": [
            _epoch_ms(row["finished_at"]) - start for row, start in zip(rows, start_ms)
        ],
    }


def _epoch_ms(iso: str) -> int:
    """ISO8601 timestamp to epoch milliseconds (naive values are taken as UTC)."""
    parsed = datetime.fromisoformat(iso)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return round(parsed.timestamp() * 1000)


def _distinct(values: Iterable[Any]) -> list[Any]:
    """Unique values in first-seen order."""
    return list(dict.fromkeys(values))


def _indices(rows: Sequence[Row], key: str, values: list[Any]) -> list[int]:
    """Each row's position of ``row[key]`` within ``values``."""
    position = {value: i for i, value in enumerate(values)}
    return [position[row[key]] for row in rows]


def _deltas(values: list[int], origin: int) -> list[int]:
    """Difference of each value from the previous one (the first from ``origin``)."""
    out = []
    previous = origin
    for value in values:
        out.append(value - previous)
        previous = value
    return out
