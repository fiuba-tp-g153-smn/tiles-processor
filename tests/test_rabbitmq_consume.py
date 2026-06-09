"""Tests for RabbitMQClient.consume — strict-priority drain across queues."""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.rabbitmq_client import RabbitMQClient
from models.work_unit import WorkUnit

NORMAL = "tiles_work_queue"
LIGHT = "tiles_light_queue"


def _unit(image_id: str) -> bytes:
    return (
        WorkUnit.create(
            image_id=image_id,
            source_uri="uri",
            data_source_id="goes19_abi_band_13",
            processor_id="goes_band_13",
            output_prefix="tiles/x",
            bounds={"minx": 0.0, "miny": 0.0, "maxx": 1.0, "maxy": 1.0},
            band_id="band_13",
        )
        .to_json()
        .encode("utf-8")
    )


class FakeChannel:
    """Minimal stand-in for a pika BlockingChannel for the drain loop."""

    def __init__(self, scripts, *, on_idle):
        # scripts: {queue_name: [body, ...]} consumed FIFO; missing/empty = empty.
        self._scripts = scripts
        self.is_closed = False
        self.get_calls = []  # queue names queried, in order
        self.acked = []
        self.nacked = []
        self._tag = 0
        # process_data_events runs only when all queues are empty; use it to end
        # the otherwise-infinite drain loop in tests.
        self.idle_calls = 0
        self.connection = SimpleNamespace(process_data_events=self._process_data_events)
        self._on_idle = on_idle

    def basic_get(self, queue, auto_ack=False):
        self.get_calls.append(queue)
        bodies = self._scripts.get(queue, [])
        if not bodies:
            return None, None, None
        self._tag += 1
        return SimpleNamespace(delivery_tag=self._tag), None, bodies.pop(0)

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue=False):
        self.nacked.append((delivery_tag, requeue))

    def _process_data_events(self, time_limit=None):
        self.idle_calls += 1
        self._on_idle()


def _client(channel) -> RabbitMQClient:
    client = RabbitMQClient(host="h", port=5672, username="u", password="p")
    client._channel = channel  # pylint: disable=protected-access
    return client


def _record_callback(received):
    def cb(work_unit, _client, _delivery_tag, source_queue):
        received.append((source_queue, work_unit.image_id))
        return True

    return cb


def test_drains_normal_fully_before_touching_light():
    received = []
    client = None

    def stop():
        client.stop_consuming()

    channel = FakeChannel(
        {NORMAL: [_unit("n1"), _unit("n2")], LIGHT: [_unit("l1")]}, on_idle=stop
    )
    client = _client(channel)
    client.consume(_record_callback(received), [NORMAL, LIGHT])

    # Normal units first, light only after normal drains.
    assert received == [(NORMAL, "n1"), (NORMAL, "n2"), (LIGHT, "l1")]
    # Light was never queried until normal returned empty: before the first
    # LIGHT get there must be a NORMAL get that yielded nothing.
    first_light = channel.get_calls.index(LIGHT)
    assert channel.get_calls[first_light - 1] == NORMAL
    assert channel.acked == [1, 2, 3]


def test_falls_back_to_light_when_normal_empty():
    received = []
    client = None

    def stop():
        client.stop_consuming()

    channel = FakeChannel({LIGHT: [_unit("l1")]}, on_idle=stop)
    client = _client(channel)
    client.consume(_record_callback(received), [NORMAL, LIGHT])

    assert received == [(LIGHT, "l1")]


def test_idle_when_all_queues_empty_runs_no_callback():
    received = []
    client = None

    def stop():
        client.stop_consuming()

    channel = FakeChannel({}, on_idle=stop)
    client = _client(channel)
    client.consume(_record_callback(received), [NORMAL, LIGHT])

    assert received == []
    assert channel.idle_calls == 1
    # Both queues were polled before idling.
    assert channel.get_calls == [NORMAL, LIGHT]


def test_invalid_body_is_dead_lettered_not_acked():
    client = None

    def stop():
        client.stop_consuming()

    channel = FakeChannel({NORMAL: [b"not-json"]}, on_idle=stop)
    client = _client(channel)

    called = []
    client.consume(
        lambda *a: called.append(a) or True, [NORMAL]  # callback must not run
    )

    assert called == []
    assert channel.acked == []
    assert channel.nacked == [(1, False)]  # rejected to DLQ, not requeued


def test_single_queue_for_light_workers():
    received = []
    client = None

    def stop():
        client.stop_consuming()

    channel = FakeChannel({LIGHT: [_unit("l1")]}, on_idle=stop)
    client = _client(channel)
    client.consume(_record_callback(received), [LIGHT])

    assert received == [(LIGHT, "l1")]
    assert NORMAL not in channel.get_calls  # never looks at the normal queue
