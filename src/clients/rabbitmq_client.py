"""RabbitMQ client for the work queue system."""

import logging
import time
from typing import Callable, Optional

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from models.work_unit import WorkUnit

logger = logging.getLogger(__name__)

# Queue names
WORK_QUEUE = "tiles_work_queue"
DEAD_LETTER_QUEUE = "tiles_dead_letter_queue"
DEAD_LETTER_EXCHANGE = "tiles_dlx"


class RabbitMQClient:
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

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        virtual_host: str = "/",
    ):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._virtual_host = virtual_host

        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None

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
                logger.info(f"Connecting to RabbitMQ at {self._host}:{self._port}...")
                self._connection = pika.BlockingConnection(
                    self._get_connection_params()
                )
                self._channel = self._connection.channel()
                self._setup_queues()
                logger.info("Connected to RabbitMQ successfully")
                return
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(
                    f"Connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(
                        f"Failed to connect to RabbitMQ after {max_retries} attempts"
                    )

    def _setup_queues(self) -> None:
        """Declare queues and exchanges for the work queue system."""
        # Declare dead letter exchange
        self._channel.exchange_declare(
            exchange=DEAD_LETTER_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        # Declare dead letter queue
        self._channel.queue_declare(
            queue=DEAD_LETTER_QUEUE,
            durable=True,
        )

        # Bind dead letter queue to exchange
        self._channel.queue_bind(
            queue=DEAD_LETTER_QUEUE,
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key=DEAD_LETTER_QUEUE,
        )

        # Declare main work queue with dead letter configuration
        self._channel.queue_declare(
            queue=WORK_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
            },
        )

        logger.info(f"Queues configured: {WORK_QUEUE}, {DEAD_LETTER_QUEUE}")

    def close(self) -> None:
        """Close the connection to RabbitMQ."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            logger.info("RabbitMQ connection closed")

    def publish(self, work_unit: WorkUnit) -> None:
        """
        Publish a work unit to the work queue.

        Messages are published with:
        - delivery_mode=2 (persistent)
        - content_type=application/json

        Args:
            work_unit: The work unit to publish
        """
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")

        message = work_unit.to_json()

        self._channel.basic_publish(
            exchange="",
            routing_key=WORK_QUEUE,
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type="application/json",
            ),
        )

        logger.debug(f"Published work unit: {work_unit}")

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

        import json

        message = json.dumps(data)

        self._channel.basic_publish(
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key=DEAD_LETTER_QUEUE,
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )

        logger.warning(f"Sent to DLQ: {work_unit} - Error: {error}")

    def consume(
        self,
        callback: Callable[[WorkUnit, "RabbitMQClient", int], bool],
        prefetch_count: int = 1,
    ) -> None:
        """
        Start consuming messages from the work queue.

        This is a blocking operation that runs until the connection is closed
        or an error occurs.

        Args:
            callback: Function called for each message. Receives (work_unit, client, delivery_tag).
                     Should return True if message should be acked, False for nack/requeue.
            prefetch_count: Number of messages to prefetch (default: 1 for fair dispatch)
        """
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")

        # Fair dispatch - don't give more than prefetch_count messages to a worker at once
        self._channel.basic_qos(prefetch_count=prefetch_count)

        def on_message(
            channel: BlockingChannel,
            method: Basic.Deliver,
            properties: BasicProperties,
            body: bytes,
        ):
            delivery_tag = method.delivery_tag
            try:
                work_unit = WorkUnit.from_json(body.decode("utf-8"))
                logger.info(f"Received work unit: {work_unit}")

                # Call the handler
                should_ack = callback(work_unit, self, delivery_tag)

                if should_ack:
                    channel.basic_ack(delivery_tag=delivery_tag)
                    logger.debug(f"Acknowledged work unit: {work_unit}")

            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                # Reject and don't requeue - let it go to DLQ
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

        self._channel.basic_consume(
            queue=WORK_QUEUE,
            on_message_callback=on_message,
            auto_ack=False,
        )

        logger.info(f"Started consuming from {WORK_QUEUE}")
        self._channel.start_consuming()

    def ack(self, delivery_tag: int) -> None:
        """Manually acknowledge a message."""
        if self._channel and not self._channel.is_closed:
            self._channel.basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag: int, requeue: bool = True) -> None:
        """Manually negative-acknowledge a message."""
        if self._channel and not self._channel.is_closed:
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def get_queue_size(self, queue_name: str = WORK_QUEUE) -> int:
        """Get the number of messages in a queue."""
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Not connected to RabbitMQ")

        result = self._channel.queue_declare(queue=queue_name, passive=True)
        return result.method.message_count
