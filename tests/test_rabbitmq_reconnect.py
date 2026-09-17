"""Tests for RabbitMQClient's reconnect-on-publish path.

A stalled discovery tick lets the broker time the producer's connection out
(heartbeats only flow while the caller is inside pika). Before these paths
reconnected, every later publish raised "Not connected to RabbitMQ" until the
container was restarted by hand.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.rabbitmq_client import RabbitMQClient
from models.work_unit import WorkUnit


def _unit() -> WorkUnit:
    return WorkUnit.create(
        image_id="img-1",
        source_uri="uri",
        data_source_id="goes19_abi_c13",
        processor_id="goes19_abi_c13",
        output_prefix="tiles/x",
        bounds={"minx": 0.0, "miny": 0.0, "maxx": 1.0, "maxy": 1.0},
        band_id="goes19_abi_c13",
    )


class FakeChannel:
    """Minimal stand-in for a pika BlockingChannel."""

    def __init__(self):
        self.is_closed = False
        self.published = []
        self.declared = []

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.published.append((exchange, routing_key, body))

    def queue_declare(self, queue, passive=False, **kwargs):
        self.declared.append(queue)

        class _Result:
            method = type("M", (), {"message_count": 7})()

        return _Result()


class FakeConnection:
    def __init__(self, channel):
        self._channel = channel
        self.is_open = True
        self.closed = False

    def channel(self):
        return self._channel

    def close(self):
        self.closed = True
        self.is_open = False


def _client(monkeypatch, connections):
    """A client whose connect() hands out `connections` in order."""
    client = RabbitMQClient(host="broker", port=5672, username="u", password="p")
    handed_out = []

    def fake_connect(max_retries=5, retry_delay=2.0):
        if not connections:
            raise RuntimeError("Failed to connect to RabbitMQ")
        connection = connections.pop(0)
        handed_out.append((max_retries, retry_delay))
        client._connection = connection  # pylint: disable=protected-access
        client._channel = connection.channel()  # pylint: disable=protected-access

    monkeypatch.setattr(client, "connect", fake_connect)
    client.connect()
    return client, handed_out


def test_publish_reconnects_after_broker_drops_the_connection(monkeypatch):
    dead, live = FakeChannel(), FakeChannel()
    client, handed_out = _client(
        monkeypatch, [FakeConnection(dead), FakeConnection(live)]
    )

    # The broker timed the connection out underneath us.
    dead.is_closed = True
    client._connection.is_open = False  # pylint: disable=protected-access

    client.publish(_unit())

    assert len(live.published) == 1, "publish should land on the new channel"
    assert not dead.published
    # Reconnect uses the short budget, not connect()'s 5 x 2s default.
    assert handed_out[-1] == (2, 1.0)


def test_publish_raises_when_the_reconnect_also_fails(monkeypatch):
    dead = FakeChannel()
    client, _ = _client(monkeypatch, [FakeConnection(dead)])

    dead.is_closed = True
    client._connection.is_open = False  # pylint: disable=protected-access

    with pytest.raises(RuntimeError):
        client.publish(_unit())


def test_get_queue_size_reconnects(monkeypatch):
    dead, live = FakeChannel(), FakeChannel()
    client, _ = _client(monkeypatch, [FakeConnection(dead), FakeConnection(live)])

    dead.is_closed = True
    client._connection.is_open = False  # pylint: disable=protected-access

    assert client.get_queue_size("tiles_work_queue") == 7
    assert live.declared == ["tiles_work_queue"]


def test_publish_to_dlq_reconnects(monkeypatch):
    dead, live = FakeChannel(), FakeChannel()
    client, _ = _client(monkeypatch, [FakeConnection(dead), FakeConnection(live)])

    dead.is_closed = True
    client._connection.is_open = False  # pylint: disable=protected-access

    client.publish_to_dlq(_unit(), "boom")

    assert len(live.published) == 1


def test_healthy_channel_is_reused(monkeypatch):
    live = FakeChannel()
    client, handed_out = _client(monkeypatch, [FakeConnection(live)])

    client.publish(_unit())
    client.publish(_unit())

    assert len(live.published) == 2
    assert len(handed_out) == 1, "no reconnect while the channel is healthy"


class DeliveringChannel(FakeChannel):
    """A channel that also hands out delivery tags, like a consuming worker's."""

    def __init__(self, bodies):
        super().__init__()
        self._bodies = bodies
        self._tag = 0

    def basic_get(self, queue, auto_ack=False):
        if not self._bodies:
            return None, None, None
        self._tag += 1
        return (
            type("M", (), {"delivery_tag": self._tag})(),
            None,
            self._bodies.pop(0),
        )

    def basic_ack(self, delivery_tag):
        pass

    def basic_nack(self, delivery_tag, requeue=True):
        pass


def test_reconnect_is_refused_while_a_delivery_is_outstanding(monkeypatch):
    """A worker mid-message must not silently land on a new channel.

    Its delivery tag belongs to the dead channel and the broker has already
    requeued that message, so reconnecting would let the worker publish a retry
    and then ack a tag that means nothing — a duplicate.
    """
    consuming = DeliveringChannel([_unit().to_json().encode("utf-8")])
    spare = FakeChannel()
    client, _ = _client(monkeypatch, [FakeConnection(consuming), FakeConnection(spare)])

    assert client.poll_one(["tiles_work_queue"], []) is not None

    consuming.is_closed = True
    client._connection.is_open = False  # pylint: disable=protected-access

    with pytest.raises(RuntimeError, match="outstanding"):
        client.publish(_unit())
    assert not spare.published, "must not reconnect mid-delivery"


def test_reconnect_resumes_once_the_delivery_is_settled(monkeypatch):
    consuming = DeliveringChannel([_unit().to_json().encode("utf-8")])
    spare = FakeChannel()
    client, _ = _client(monkeypatch, [FakeConnection(consuming), FakeConnection(spare)])

    _, delivery_tag, _ = client.poll_one(["tiles_work_queue"], [])
    client.ack(delivery_tag)

    consuming.is_closed = True
    client._connection.is_open = False  # pylint: disable=protected-access

    client.publish(_unit())

    assert len(spare.published) == 1
