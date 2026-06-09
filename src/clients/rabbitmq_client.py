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
        light_queue_name: Optional[str] = None,
    ):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._virtual_host = virtual_host

        self._queue_name = queue_name
        self._dlq_name = dlq_name
        self._dlx_name = dlx_name
        # Optional second work queue for lightweight units (radar/WRF). When set,
        # it is declared alongside the main queue so any role (producer, worker,
        # metrics) can publish to or probe it. Workers still consume only their
        # own _queue_name; the producer routes per work unit.
        self._light_queue_name = light_queue_name

        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        # Drives the consume() drain loop; flipped off by stop_consuming().
        self._consuming = False

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

        # Declare the optional light work queue with the same DLX config. Declares
        # are idempotent, so whichever role boots first creates it; identical
        # arguments everywhere avoid a PRECONDITION_FAILED on redeclare.
        if self._light_queue_name and self._light_queue_name != self._queue_name:
            self._declare_work_queue(self._light_queue_name)

        logger.info(
            "Queues configured: %s%s, %s",
            self._queue_name,
            f", {self._light_queue_name}" if self._light_queue_name else "",
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
        queues: list[str],
    ) -> None:
        """
        Drain messages from one or more queues in strict priority order.

        Blocks until stop_consuming() is called or the connection drops. Each
        iteration tries the queues in order and processes the first message
        found, then restarts from the highest-priority queue — so a fallback
        queue is touched only once every higher-priority queue is empty (a
        normal worker drains its normal queue fully before stealing light work).
        When all queues are empty it idles for _IDLE_POLL_S (keeping the
        connection's heartbeats alive) and polls again.

        Args:
            callback: Called per message with
                (work_unit, client, delivery_tag, source_queue). Returns True to
                ack, False to leave unacked (requeued when the connection closes).
            queues: Queue names in descending priority.
        """
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")
        if not queues:
            raise ValueError("consume() requires at least one queue")

        logger.info("Started consuming from %s (priority order)", queues)
        self._consuming = True
        while self._consuming:
            for queue_name in queues:
                method, _props, body = self._channel.basic_get(
                    queue=queue_name, auto_ack=False
                )
                if method is None or body is None:
                    continue  # this queue is empty — fall through to the next
                self._handle_message(callback, queue_name, method.delivery_tag, body)
                break  # restart the scan from the highest-priority queue
            else:
                # Every queue empty: idle while servicing heartbeats/I/O.
                self._channel.connection.process_data_events(
                    time_limit=self._IDLE_POLL_S
                )

    def _handle_message(
        self,
        callback: Callable[[WorkUnit, "MessageQueueClient", int, str], bool],
        source_queue: str,
        delivery_tag: int,
        body: bytes,
    ) -> None:
        """Decode one message, run the callback, and ack on success."""
        if self._channel is None:
            raise RuntimeError("RabbitMQ channel is not initialized")
        try:
            work_unit = WorkUnit.from_json(body.decode("utf-8"))
            logger.info("Received work unit from %s: %s", source_queue, work_unit)

            should_ack = callback(work_unit, self, delivery_tag, source_queue)

            if should_ack:
                self._channel.basic_ack(delivery_tag=delivery_tag)
                logger.debug("Acknowledged work unit: %s", work_unit)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception("Error processing message: %s", e)
            # Reject and don't requeue - let it go to DLQ.
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

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
