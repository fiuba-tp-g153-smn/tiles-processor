"""Tests for RabbitMQClient.consume — strict-priority tier + round-robin tier."""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.rabbitmq_client import RabbitMQClient
from models.work_unit import WorkUnit

NORMAL = "tiles_work_queue"
RADAR_LIGHT = "tiles_radar_light_queue"
WRF_LIGHT = "tiles_wrf_light_queue"


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


def _consume(channel, *, strict, round_robin, received):
    """Run a drain loop that stops the first time every queue is empty."""
    client = _client(channel)
    channel._on_idle = client.stop_consuming  # pylint: disable=protected-access
    client.consume(_record_callback(received), strict, round_robin)


def test_strict_normal_drains_fully_before_any_light():
    """Normal queue (strict tier) is exhausted before either light queue."""
    received = []
    channel = FakeChannel(
        {
            NORMAL: [_unit("n1"), _unit("n2")],
            RADAR_LIGHT: [_unit("r1")],
            WRF_LIGHT: [_unit("w1")],
        },
        on_idle=lambda: None,
    )
    _consume(
        channel,
        strict=[NORMAL],
        round_robin=[RADAR_LIGHT, WRF_LIGHT],
        received=received,
    )

    assert received == [
        (NORMAL, "n1"),
        (NORMAL, "n2"),
        (RADAR_LIGHT, "r1"),
        (WRF_LIGHT, "w1"),
    ]
    # No light queue is touched until a NORMAL get has returned empty.
    first_light = min(
        channel.get_calls.index(RADAR_LIGHT), channel.get_calls.index(WRF_LIGHT)
    )
    assert channel.get_calls[first_light - 1] == NORMAL
    assert channel.acked == [1, 2, 3, 4]


def test_round_robin_alternates_the_two_light_queues():
    """With both light queues backlogged, pickups alternate radar/wrf."""
    received = []
    channel = FakeChannel(
        {
            RADAR_LIGHT: [_unit("r1"), _unit("r2"), _unit("r3")],
            WRF_LIGHT: [_unit("w1"), _unit("w2")],
        },
        on_idle=lambda: None,
    )
    _consume(
        channel, strict=[], round_robin=[RADAR_LIGHT, WRF_LIGHT], received=received
    )

    assert received == [
        (RADAR_LIGHT, "r1"),
        (WRF_LIGHT, "w1"),
        (RADAR_LIGHT, "r2"),
        (WRF_LIGHT, "w2"),
        (RADAR_LIGHT, "r3"),
    ]


def test_round_robin_no_starvation_when_one_light_queue_empty():
    """An empty starting queue still lets the other light queue be served."""
    received = []
    channel = FakeChannel({WRF_LIGHT: [_unit("w1")]}, on_idle=lambda: None)
    # rr starts at index 0 (RADAR_LIGHT), which is empty; WRF_LIGHT must still serve.
    _consume(
        channel, strict=[], round_robin=[RADAR_LIGHT, WRF_LIGHT], received=received
    )

    assert received == [(WRF_LIGHT, "w1")]


def test_idle_polls_all_tiers_then_idles_once():
    received = []
    channel = FakeChannel({}, on_idle=lambda: None)
    _consume(
        channel,
        strict=[NORMAL],
        round_robin=[RADAR_LIGHT, WRF_LIGHT],
        received=received,
    )

    assert received == []
    assert channel.idle_calls == 1
    # Strict tier first, then both light queues, before idling.
    assert channel.get_calls == [NORMAL, RADAR_LIGHT, WRF_LIGHT]


def test_light_worker_has_no_strict_queue():
    received = []
    channel = FakeChannel({RADAR_LIGHT: [_unit("r1")]}, on_idle=lambda: None)
    _consume(
        channel, strict=[], round_robin=[RADAR_LIGHT, WRF_LIGHT], received=received
    )

    assert received == [(RADAR_LIGHT, "r1")]
    assert NORMAL not in channel.get_calls  # never looks at the normal queue


def test_invalid_body_is_dead_lettered_not_acked():
    channel = FakeChannel({NORMAL: [b"not-json"]}, on_idle=lambda: None)
    client = _client(channel)
    channel._on_idle = client.stop_consuming  # pylint: disable=protected-access

    called = []
    client.consume(lambda *a: called.append(a) or True, [NORMAL], [])

    assert called == []  # callback must not run on undecodable bodies
    assert channel.acked == []
    assert channel.nacked == [(1, False)]  # rejected to DLQ, not requeued
