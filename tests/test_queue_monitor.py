import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from metrics_api.queue_monitor import QueueDepthMonitor


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


def test_reuses_one_connection_across_polls():
    calls = []

    def factory():
        calls.append(1)
        return FakeClient({"work": 3, "dlq": 1})

    monitor = QueueDepthMonitor(factory, "work", "dlq")
    try:
        assert monitor.depths() == {"work": 3, "dlq": 1}
        assert monitor.depths() == {"work": 3, "dlq": 1}
        assert len(calls) == 1  # connection reused, not reconnected each poll
    finally:
        monitor.close()


def test_reconnects_when_connection_drops():
    made = []

    def factory():
        client = FakeClient({"work": 1, "dlq": 0})
        made.append(client)
        return client

    monitor = QueueDepthMonitor(factory, "work", "dlq")
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
        client = FakeClient({"work": 5, "dlq": 2}, fail_read=not made)
        made.append(client)
        return client

    monitor = QueueDepthMonitor(factory, "work", "dlq")
    try:
        assert monitor.depths() == {
            "work": None,
            "dlq": None,
        }  # first read fails -> n/a
        assert monitor.depths() == {"work": 5, "dlq": 2}  # reconnects immediately
        assert len(made) == 2
    finally:
        monitor.close()


def test_degrades_and_backs_off_when_broker_down():
    calls = []
    clock = {"t": 0.0}

    def factory():
        calls.append(1)
        raise RuntimeError("broker down")

    monitor = QueueDepthMonitor(factory, "work", "dlq", monotonic=lambda: clock["t"])
    try:
        assert monitor.depths() == {"work": None, "dlq": None}
        assert len(calls) == 1

        # Within the backoff window: returns n/a without re-attempting a connect.
        assert monitor.depths() == {"work": None, "dlq": None}
        assert len(calls) == 1

        # After the backoff elapses: tries to connect again.
        clock["t"] = QueueDepthMonitor._RECONNECT_BACKOFF_S + 1
        assert monitor.depths() == {"work": None, "dlq": None}
        assert len(calls) == 2
    finally:
        monitor.close()
