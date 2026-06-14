"""RabbitMQ client for the work queue system."""

import json
import logging
import time
from typing import Callable, Optional

import pika
from pika.exceptions import AMQPConnectionError
from pika.adapters.blocking_connection import BlockingChannel

from models.work_unit import WorkUnit
from clients.message_queue_client import MessageQueueClient

logger = logging.getLogger(__name__)


class RabbitMQClient(MessageQueueClient):
    """
    RabbitMQ client for publishing and consuming work units.

    This client manages connections to RabbitMQ and provides methods for:
    - Publishing work units to the work queue
    - Consuming work units with manual acknowledgment
    - Sending failed work units to the dead letter queue

    Connection Management:
        - Automatic reconnection on connection loss
        - Separate connections for publishing and consuming (recommended by RabbitMQ)

    Queue Configuration:
        - Durable queues (survive broker restart)
        - Manual acknowledgment (at-least-once delivery)
        - Dead letter queue for failed messages
        - Prefetch count of 1 (fair dispatch to workers)
    """

    # When every consumed queue is empty, idle this long (processing I/O, so
    # heartbeats stay alive) before polling again. Bounds pickup latency;
    # negligible for second-to-minute jobs.
    _IDLE_POLL_S = 1.0

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        queue_name: str = "tiles_work_queue",
        dlq_name: str = "tiles_dead_letter_queue",
        dlx_name: str = "tiles_dlx",
        virtual_host: str = "/",
        extra_queue_names: Optional[list[str]] = None,
    ):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._virtual_host = virtual_host

        self._queue_name = queue_name
        self._dlq_name = dlq_name
        self._dlx_name = dlx_name
        # Extra work queues (the radar/WRF light queues). They are declared
        # alongside the main queue so any role (producer, worker, metrics) can
        # publish to or probe them. Workers consume the queues they ask for in
        # consume(); the producer routes per work unit.
        self._extra_queue_names = extra_queue_names or []

        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        # Drives the consume() drain loop; flipped off by stop_consuming().
        self._consuming = False
        # Persistent round-robin cursor for poll_one's light-queue tier: one past
        # the last served queue, so successive polls alternate fairly and neither
        # light queue head-of-line blocks the other.
        self._rr = 0

    def _get_connection_params(self) -> pika.ConnectionParameters:
        """Build connection parameters."""
        credentials = pika.PlainCredentials(self._username, self._password)
        return pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            virtual_host=self._virtual_host,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )

    def connect(self, max_retries: int = 5, retry_delay: float = 2.0) -> None:
        """
        Establish connection to RabbitMQ with retry logic.

        Args:
            max_retries: Maximum connection attempts
            retry_delay: Seconds between retry attempts
        """
        for attempt in range(max_retries):
            try:
                logger.info(
                    "Connecting to RabbitMQ at %s:%d...", self._host, self._port
                )
                self._connection = pika.BlockingConnection(
                    self._get_connection_params()
                )
                self._channel = self._connection.channel()
                self._setup_queues()
                logger.info("Connected to RabbitMQ successfully")
                return
            except AMQPConnectionError as e:
                logger.warning(
                    "Connection attempt %d/%d failed: %s", attempt + 1, max_retries, e
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(
                        f"Failed to connect to RabbitMQ after {max_retries} attempts"
                    ) from e

    def _setup_queues(self) -> None:
        """Declare queues and exchanges for the work queue system."""
        if self._channel is None:
            raise RuntimeError("RabbitMQ channel is not initialized")

        # Declare dead letter exchange
        self._channel.exchange_declare(
            exchange=self._dlx_name,
            exchange_type="direct",
            durable=True,
        )

        # Declare dead letter queue
        self._channel.queue_declare(
            queue=self._dlq_name,
            durable=True,
        )

        # Bind dead letter queue to exchange
        self._channel.queue_bind(
            queue=self._dlq_name,
            exchange=self._dlx_name,
            routing_key=self._dlq_name,
        )

        # Declare main work queue with dead letter configuration
        self._declare_work_queue(self._queue_name)

        # Declare the extra light work queues with the same DLX config. Declares
        # are idempotent, so whichever role boots first creates them; identical
        # arguments everywhere avoid a PRECONDITION_FAILED on redeclare.
        declared_extra = []
        for queue_name in self._extra_queue_names:
            if queue_name and queue_name != self._queue_name:
                self._declare_work_queue(queue_name)
                declared_extra.append(queue_name)

        logger.info(
            "Queues configured: %s, %s",
            ", ".join([self._queue_name, *declared_extra]),
            self._dlq_name,
        )

    def _declare_work_queue(self, queue_name: str) -> None:
        """Declare a durable work queue that dead-letters to the shared DLQ."""
        if self._channel is None:
            raise RuntimeError("RabbitMQ channel is not initialized")
        self._channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": self._dlx_name,
                "x-dead-letter-routing-key": self._dlq_name,
            },
        )

    def stop_consuming(self) -> None:
        """Signal the consume() drain loop to stop.

        The drain runs in the main thread and the worker's signal handler runs
        there too, so a plain flag is safe: the loop exits at its next iteration
        (after the in-flight message, or within _IDLE_POLL_S when idle).
        """
        self._consuming = False

    def close(self) -> None:
        """Close the connection to RabbitMQ."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            logger.info("RabbitMQ connection closed")

    def publish(self, work_unit: WorkUnit, queue_name: Optional[str] = None) -> None:
        """
        Publish a work unit to a work queue.

        Messages are published with:
        - delivery_mode=2 (persistent)
        - content_type=application/json

        Args:
            work_unit: The work unit to publish
            queue_name: Target queue. Defaults to this client's main queue, so
                worker requeue/retry paths return units to the queue they consume.
                The producer passes an explicit queue to route light vs heavy work.
        """
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")

        message = work_unit.to_json()

        self._channel.basic_publish(
            exchange="",
            routing_key=queue_name or self._queue_name,
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type="application/json",
            ),
        )

        logger.debug("Published work unit: %s", work_unit)

    def publish_to_dlq(self, work_unit: WorkUnit, error: str) -> None:
        """
        Publish a failed work unit to the dead letter queue.

        Args:
            work_unit: The failed work unit
            error: Error message describing the failure
        """
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")

        # Add error info to the work unit data
        data = work_unit.to_dict()
        data["error"] = error
        data["failed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        message = json.dumps(data)

        self._channel.basic_publish(
            exchange=self._dlx_name,
            routing_key=self._dlq_name,
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )

        logger.warning("Sent to DLQ: %s - Error: %s", work_unit, error)

    def consume(
        self,
        callback: Callable[[WorkUnit, "MessageQueueClient", int, str], bool],
        strict_queues: list[str],
        round_robin_queues: list[str],
    ) -> None:
        """
        Drain messages from two tiers: strict-priority then round-robin.

        Blocks until stop_consuming() is called or the connection drops. Each
        iteration pulls one message via poll_one (strict tier first, then a fair
        round-robin of the light queues); when every queue is empty it idles for
        _IDLE_POLL_S (servicing heartbeats) and polls again. This callback form
        processes one message at a time; the worker's bounded-concurrency drain
        loop uses poll_one/service_events directly to overlap units.

        Args:
            callback: Called per message with
                (work_unit, client, delivery_tag, source_queue). Returns True to
                ack, False to leave unacked (requeued when the connection closes).
            strict_queues: Highest-priority queues, scanned in descending order
                (empty for light workers, which have no normal queue).
            round_robin_queues: Queues drained fairly/alternating once the strict
                tier is empty.
        """
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")
        if not strict_queues and not round_robin_queues:
            raise ValueError("consume() requires at least one queue")

        logger.info(
            "Started consuming: strict=%s round-robin=%s",
            strict_queues,
            round_robin_queues,
        )
        self._consuming = True
        while self._consuming:
            message = self.poll_one(strict_queues, round_robin_queues)
            if message is None:
                # Every queue empty: idle while servicing heartbeats/I/O.
                self.service_events(self._IDLE_POLL_S)
                continue
            work_unit, delivery_tag, source_queue = message
            try:
                if callback(work_unit, self, delivery_tag, source_queue):
                    self.ack(delivery_tag)
                    logger.debug("Acknowledged work unit: %s", work_unit)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.exception("Error processing message: %s", e)
                # Reject and don't requeue - let it go to DLQ.
                self.nack(delivery_tag, requeue=False)

    def poll_one(
        self, strict_queues: list[str], round_robin_queues: list[str]
    ) -> tuple[WorkUnit, int, str] | None:
        """Fetch one message without acking; None when every queue is empty.

        Scans ``strict_queues`` in order and returns the first message found
        (strict priority); only when all are empty does it pull from
        ``round_robin_queues``, advancing a persistent cursor so the light
        queues alternate fairly and neither head-of-line blocks the other.
        Returns ``(work_unit, delivery_tag, source_queue)``. Undecodable bodies
        are dead-lettered and skipped. The caller acks/nacks after processing.
        """
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")

        for queue_name in strict_queues:
            message = self._basic_get_one(queue_name)
            if message is not None:
                return message

        count = len(round_robin_queues)
        for offset in range(count):
            index = (self._rr + offset) % count
            message = self._basic_get_one(round_robin_queues[index])
            if message is not None:
                self._rr = (index + 1) % count
                return message
        return None

    def _basic_get_one(self, queue_name: str) -> tuple[WorkUnit, int, str] | None:
        """basic_get one message and decode it; None when the queue is empty.

        Undecodable bodies are nacked to the DLQ (not requeued) and reported as
        absent so the drain loop keeps making progress.
        """
        if self._channel is None:
            raise RuntimeError("RabbitMQ channel is not initialized")
        method, _props, body = self._channel.basic_get(queue=queue_name, auto_ack=False)
        if method is None or body is None:
            return None
        try:
            work_unit = WorkUnit.from_json(body.decode("utf-8"))
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception(
                "Discarding undecodable message from %s: %s", queue_name, e
            )
            self._channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return None
        logger.info("Received work unit from %s: %s", queue_name, work_unit)
        return work_unit, method.delivery_tag, queue_name

    def service_events(self, time_limit: float = 0.0) -> None:
        """Pump connection I/O (heartbeats, acks) without consuming a message.

        Lets the worker's bounded-concurrency drain loop keep the connection's
        heartbeat alive while it awaits in-flight work, without blocking on new
        messages. ``time_limit=0`` returns as soon as pending events are drained.
        """
        if self._channel is None or self._channel.is_closed:
            return
        self._channel.connection.process_data_events(time_limit=time_limit)

    def ack(self, delivery_tag: int) -> None:
        """Manually acknowledge a message."""
        if self._channel and not self._channel.is_closed:
            self._channel.basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag: int, requeue: bool = True) -> None:
        """Manually negative-acknowledge a message."""
        if self._channel and not self._channel.is_closed:
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def get_queue_size(self, queue_name: Optional[str] = None) -> int:
        """Get the number of messages in a queue."""
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")

        target_queue = queue_name if queue_name else self._queue_name

        result = self._channel.queue_declare(queue=target_queue, passive=True)
        return result.method.message_count

    @property
    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ."""
        return self._connection is not None and self._connection.is_open
