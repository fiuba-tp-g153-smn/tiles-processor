"""Tests for Worker._consume_tiers — strict vs round-robin queues per worker type."""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from worker.worker import Worker

NORMAL = "tiles_work_queue"
RADAR_LIGHT = "tiles_radar_light_queue"
WRF_LIGHT = "tiles_wrf_light_queue"


def _worker(worker_type: str) -> Worker:
    config = SimpleNamespace(
        RABBITMQ_QUEUE=NORMAL,
        RABBITMQ_RADAR_LIGHT_QUEUE=RADAR_LIGHT,
        RABBITMQ_WRF_LIGHT_QUEUE=WRF_LIGHT,
        WORKER_TYPE=worker_type,
    )
    return Worker(config=config, mq_client=None, handler=None)


def test_normal_worker_strict_normal_then_round_robin_lights():
    strict, round_robin = _worker("normal")._consume_tiers()
    assert strict == [NORMAL]
    assert round_robin == [RADAR_LIGHT, WRF_LIGHT]


def test_light_worker_only_round_robins_lights():
    strict, round_robin = _worker("light")._consume_tiers()
    assert strict == []
    assert round_robin == [RADAR_LIGHT, WRF_LIGHT]
