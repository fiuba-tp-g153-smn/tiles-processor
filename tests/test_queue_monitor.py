import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from metrics_api.queue_monitor import QueueDepthMonitor

_RECONNECT_LOG = "reconnecting after previous connection dropped"


class FakeClient:
    """Stand-in for RabbitMQClient: the monitor only uses these three members."""

    def __init__(self, depths, *, connected=True, fail_read=False):
        self._depths = depths
        self.is_connected = connected
        self.fail_read = fail_read
        self.closed = False

    def get_queue_size(self, name):
        if self.fail_read:
            raise RuntimeError("channel closed")
        return self._depths[name]

    def close(self):
        self.closed = True


def _monitor(factory, **kwargs):
    return QueueDepthMonitor(
        factory, "work", "dlq", "radar_light", "wrf_light", **kwargs
    )


def _depths(radar, wrf, *, work, dlq):
    """The per-queue sizes a FakeClient should report."""
    return {"work": work, "radar_light": radar, "wrf_light": wrf, "dlq": dlq}


def _reachable(radar, wrf, *, work, dlq):
    """The depths() result for reachable queues (light = radar + wrf)."""
    return {
        "work": work,
        "radar_light": radar,
        "wrf_light": wrf,
        "light": radar + wrf,
        "dlq": dlq,
    }


_UNREACHABLE = {
    "work": None,
    "radar_light": None,
    "wrf_light": None,
    "light": None,
    "dlq": None,
}


def test_reuses_one_connection_across_polls():
    calls = []

    def factory():
        calls.append(1)
        return FakeClient(_depths(2, 3, work=3, dlq=1))

    monitor = _monitor(factory)
    try:
        assert monitor.depths() == _reachable(2, 3, work=3, dlq=1)
        assert monitor.depths() == _reachable(2, 3, work=3, dlq=1)
        assert len(calls) == 1  # connection reused, not reconnected each poll
    finally:
        monitor.close()


def test_combines_light_queues_into_one_total():
    """The `light` key is the radar+wrf sum (the dashboard's single tile)."""
    monitor = _monitor(lambda: FakeClient(_depths(4, 6, work=1, dlq=0)))
    try:
        depths = monitor.depths()
        assert depths["radar_light"] == 4
        assert depths["wrf_light"] == 6
        assert depths["light"] == 10
    finally:
        monitor.close()


def test_reconnects_when_connection_drops():
    made = []

    def factory():
        client = FakeClient(_depths(1, 1, work=1, dlq=0))
        made.append(client)
        return client

    monitor = _monitor(factory)
    try:
        monitor.depths()
        made[-1].is_connected = False  # simulate the broker dropping the connection
        monitor.depths()
        assert len(made) == 2  # a fresh client was built
        assert made[0].closed  # the stale one was discarded
    finally:
        monitor.close()


def test_read_failure_reconnects_next_poll_without_backoff():
    made = []

    def factory():
        client = FakeClient(_depths(3, 4, work=5, dlq=2), fail_read=not made)
        made.append(client)
        return client

    monitor = _monitor(factory)
    try:
        assert monitor.depths() == _UNREACHABLE  # first read fails -> n/a
        assert monitor.depths() == _reachable(3, 4, work=5, dlq=2)  # reconnects
        assert len(made) == 2
    finally:
        monitor.close()


def test_logs_on_reconnect(caplog):
    made = []

    def factory():
        client = FakeClient(_depths(1, 1, work=1, dlq=0))
        made.append(client)
        return client

    monitor = _monitor(factory)
    try:
        monitor.depths()
        made[-1].is_connected = False  # broker dropped the connection
        with caplog.at_level(logging.INFO):
            monitor.depths()
        assert _RECONNECT_LOG in caplog.text
    finally:
        monitor.close()


def test_no_reconnect_log_on_first_connect(caplog):
    monitor = _monitor(lambda: FakeClient(_depths(2, 3, work=3, dlq=1)))
    try:
        with caplog.at_level(logging.INFO):
            monitor.depths()
        assert _RECONNECT_LOG not in caplog.text  # first connect is silent
    finally:
        monitor.close()


def test_degrades_and_backs_off_when_broker_down():
    calls = []
    clock = {"t": 0.0}

    def factory():
        calls.append(1)
        raise RuntimeError("broker down")

    monitor = _monitor(factory, monotonic=lambda: clock["t"])
    try:
        assert monitor.depths() == _UNREACHABLE
        assert len(calls) == 1

        # Within the backoff window: returns n/a without re-attempting a connect.
        assert monitor.depths() == _UNREACHABLE
        assert len(calls) == 1

        # After the backoff elapses: tries to connect again.
        clock["t"] = QueueDepthMonitor._RECONNECT_BACKOFF_S + 1
        assert monitor.depths() == _UNREACHABLE
        assert len(calls) == 2
    finally:
        monitor.close()
