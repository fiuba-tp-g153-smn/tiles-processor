"""Persistent RabbitMQ connection for the metrics API's queue-depth probes.

The ``/api/live`` endpoint reads the work/DLQ queue depths on every poll. Instead
of opening and tearing down an AMQP connection each time (connection churn plus a
wall of pika logs), ``QueueDepthMonitor`` keeps **one** connection and reuses it.

``pika``'s ``BlockingConnection`` is not thread-safe, and FastAPI runs the sync
``/api/live`` handler in a worker-thread pool, so all broker I/O is confined to a
single dedicated thread (a one-worker executor). The monitor reconnects when the
connection drops and degrades to ``None`` counts when the broker is unreachable,
so the metrics API reports "n/a" rather than erroring.
"""

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from clients.rabbitmq_client import RabbitMQClient

logger = logging.getLogger(__name__)


class QueueDepthMonitor:
    """Read RabbitMQ queue depths over a single, reused, thread-confined connection."""

    # After a failed *connect*, wait this long before trying again, so a down
    # broker isn't hammered (and pika's connect logs aren't re-spammed) every poll.
    _RECONNECT_BACKOFF_S = 30.0

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        client_factory: Callable[[], RabbitMQClient],
        work_queue: str,
        dlq: str,
        radar_light_queue: str,
        wrf_light_queue: str,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client_factory = client_factory
        self._work_queue = work_queue
        self._dlq = dlq
        self._radar_light_queue = radar_light_queue
        self._wrf_light_queue = wrf_light_queue
        self._monotonic = monotonic
        self._client: RabbitMQClient | None = None
        self._healthy: bool | None = None  # None = unknown (transition-only logging)
        self._retry_after = 0.0
        # BlockingConnection is not thread-safe: pin all broker I/O to one thread.
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="rmq-probe"
        )

    def depths(self) -> dict[str, int | None]:
        """Return queue depths (None when unreachable).

        Keys: ``work``, ``radar_light``, ``wrf_light``, ``dlq``, plus ``light``
        (the radar+wrf sum) kept for the dashboard's single light-queue tile.
        """
        return self._executor.submit(self._probe).result()

    def close(self) -> None:
        """Close the connection and stop the probe thread (call on app shutdown)."""
        self._executor.submit(self._discard).result()
        self._executor.shutdown(wait=True)

    # ------------------------------------------------------------------
    # Everything below runs on the single executor thread (pika affinity).
    # ------------------------------------------------------------------

    def _probe(self) -> dict[str, int | None]:
        if self._client is None and self._monotonic() < self._retry_after:
            return self._unreachable()  # still backing off after a failed connect

        try:
            client = self._ensure_client()
        except Exception:  # pylint: disable=broad-exception-caught
            # Connect failed -> broker unreachable; back off before retrying.
            self._mark_unhealthy(backoff=True)
            return self._unreachable()

        try:
            radar_light = client.get_queue_size(self._radar_light_queue)
            wrf_light = client.get_queue_size(self._wrf_light_queue)
            depths: dict[str, int | None] = {
                "work": client.get_queue_size(self._work_queue),
                "radar_light": radar_light,
                "wrf_light": wrf_light,
                "light": radar_light + wrf_light,  # combined: dashboard tile
                "dlq": client.get_queue_size(self._dlq),
            }
        except Exception:  # pylint: disable=broad-exception-caught
            # Read failed on an established connection -> drop and reconnect next
            # poll (no backoff: the connection may just have been recycled).
            self._mark_unhealthy(backoff=False)
            return self._unreachable()

        self._mark_healthy()
        return depths

    @staticmethod
    def _unreachable() -> dict[str, int | None]:
        """All depths None: broker unreachable, reported as n/a."""
        return {
            "work": None,
            "radar_light": None,
            "wrf_light": None,
            "light": None,
            "dlq": None,
        }

    def _ensure_client(self) -> RabbitMQClient:
        if self._client is None or not self._client.is_connected:
            reconnecting = self._healthy is not None  # not the first-ever connect
            self._discard()
            if reconnecting:
                logger.info(
                    "RabbitMQ queue-depth probe reconnecting after previous connection dropped"
                )
            self._client = self._client_factory()
        return self._client

    def _mark_healthy(self) -> None:
        if self._healthy is False:
            logger.info("RabbitMQ queue-depth probe recovered")
        self._healthy = True

    def _mark_unhealthy(self, *, backoff: bool) -> None:
        if self._healthy is not False:  # only log on the ok/unknown -> failed edge
            logger.warning(
                "RabbitMQ queue-depth probe failed; showing queue depths as n/a",
                exc_info=True,
            )
        self._healthy = False
        self._discard()
        if backoff:
            self._retry_after = self._monotonic() + self._RECONNECT_BACKOFF_S

    def _discard(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        self._client = None
