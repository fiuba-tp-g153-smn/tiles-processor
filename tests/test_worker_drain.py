"""Tests for Worker._drain — bounded-concurrency overlap of units."""

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from models.work_unit import WorkUnit
from worker.worker import Worker

NORMAL = "tiles_work_queue"
RADAR_LIGHT = "tiles_radar_light_queue"
WRF_LIGHT = "tiles_wrf_light_queue"


def _config(concurrency: int) -> SimpleNamespace:
    return SimpleNamespace(
        RABBITMQ_QUEUE=NORMAL,
        RABBITMQ_RADAR_LIGHT_QUEUE=RADAR_LIGHT,
        RABBITMQ_WRF_LIGHT_QUEUE=WRF_LIGHT,
        WORKER_TYPE="light",
        WORKER_CONCURRENCY=concurrency,
        WORKER_ID="wid",
    )


def _unit(image_id: str) -> WorkUnit:
    return WorkUnit.create(
        image_id=image_id,
        source_uri="uri",
        data_source_id="goes19_abi_band_13",
        processor_id="goes_band_13",
        output_prefix="tiles/x",
        bounds={"minx": 0.0, "miny": 0.0, "maxx": 1.0, "maxy": 1.0},
        band_id="band_13",
    )


class _FakeMQ:
    """Hands out a fixed list of messages, then None; records acks."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.acked = []

    def poll_one(self, _strict, _round_robin):
        return self._messages.pop(0) if self._messages else None

    def service_events(self, _time_limit=0.0):
        pass

    def ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def publish(self, *_a, **_k):
        pass

    def publish_to_dlq(self, *_a, **_k):
        pass

    def nack(self, *_a, **_k):
        pass


class _GatedHandler:
    """handle() blocks on a shared gate so concurrency can be observed."""

    def __init__(self, gate: asyncio.Event):
        self._gate = gate
        self.started = []
        self.current = 0
        self.max_concurrent = 0

    async def handle(self, work_unit, _collector):
        self.current += 1
        self.max_concurrent = max(self.max_concurrent, self.current)
        self.started.append(work_unit.image_id)
        try:
            await self._gate.wait()
        finally:
            self.current -= 1

    def release_progress(self, _work_unit):
        pass


@pytest.mark.asyncio
async def test_drain_overlaps_units_up_to_concurrency():
    """With K=2 and 3 queued units, exactly 2 run concurrently (3rd waits)."""
    gate = asyncio.Event()
    messages = [(_unit(f"img{i}"), i, RADAR_LIGHT) for i in range(3)]
    mq = _FakeMQ(messages)
    handler = _GatedHandler(gate)

    worker = Worker(_config(concurrency=2), mq, handler)
    worker._loop = asyncio.get_running_loop()

    drain = asyncio.create_task(worker._drain())

    # Wait until the two concurrency slots are occupied.
    for _ in range(1000):
        if len(handler.started) >= 2:
            break
        await asyncio.sleep(0)

    assert handler.max_concurrent == 2  # K cap honored
    assert handler.current == 2
    assert handler.started == ["img0", "img1"]  # 3rd not started while slots full

    # Release everything and let the loop shut down.
    worker._running = False
    gate.set()
    await asyncio.wait_for(drain, timeout=5)

    assert handler.max_concurrent == 2  # never exceeded K
    # Only the two admitted units are acked; the 3rd was never polled at shutdown.
    assert sorted(mq.acked) == [0, 1]


@pytest.mark.asyncio
async def test_drain_admits_next_unit_as_a_slot_frees():
    """A 3rd unit starts once one of the first two finishes (slot reuse)."""
    gate = asyncio.Event()
    messages = [(_unit(f"img{i}"), i, RADAR_LIGHT) for i in range(3)]
    mq = _FakeMQ(messages)

    class _OneShotHandler(_GatedHandler):
        async def handle(self, work_unit, _collector):
            self.current += 1
            self.max_concurrent = max(self.max_concurrent, self.current)
            self.started.append(work_unit.image_id)
            try:
                # img0 returns immediately, freeing a slot; others block.
                if work_unit.image_id != "img0":
                    await self._gate.wait()
            finally:
                self.current -= 1

    handler = _OneShotHandler(gate)
    worker = Worker(_config(concurrency=2), mq, handler)
    worker._loop = asyncio.get_running_loop()

    drain = asyncio.create_task(worker._drain())

    for _ in range(1000):
        if "img2" in handler.started:
            break
        await asyncio.sleep(0)

    assert "img2" in handler.started  # freed slot admitted the 3rd unit
    assert handler.max_concurrent == 2

    worker._running = False
    gate.set()
    await asyncio.wait_for(drain, timeout=5)
