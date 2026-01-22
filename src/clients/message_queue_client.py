"""Abstract base class for message queue clients."""

from abc import ABC, abstractmethod
from typing import Callable, Any, Optional

from models.work_unit import WorkUnit


class MessageQueueClient(ABC):
    """
    Abstract base class for message queue clients.

    Defines the standard interface for interacting with a message queue system
    including connection management, publishing, consuming, and acknowledgment.
    """

    @abstractmethod
    def connect(self, max_retries: int = 5, retry_delay: float = 2.0) -> None:
        """
        Establish connection to the message queue.

        Args:
            max_retries: Maximum connection attempts
            retry_delay: Seconds between retry attempts
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the message queue."""
        pass

    @abstractmethod
    def publish(self, work_unit: WorkUnit) -> None:
        """
        Publish a work unit to the work queue.

        Args:
            work_unit: The work unit to publish
        """
        pass

    @abstractmethod
    def publish_to_dlq(self, work_unit: WorkUnit, error: str) -> None:
        """
        Publish a failed work unit to the dead letter queue.

        Args:
            work_unit: The failed work unit
            error: Error message describing the failure
        """
        pass

    @abstractmethod
    def consume(
        self,
        callback: Callable[[WorkUnit, "MessageQueueClient", int], bool],
        prefetch_count: int = 1,
    ) -> None:
        """
        Start consuming messages from the work queue.

        Args:
            callback: Function called for each message. Receives (work_unit, client, delivery_tag).
                     Should return True if message should be acked, False for nack/requeue.
            prefetch_count: Number of messages to prefetch
        """
        pass

    @abstractmethod
    def ack(self, delivery_tag: int) -> None:
        """
        Manually acknowledge a message.

        Args:
            delivery_tag: Unique identifier for the message
        """
        pass

    @abstractmethod
    def nack(self, delivery_tag: int, requeue: bool = True) -> None:
        """
        Manually negative-acknowledge a message.

        Args:
            delivery_tag: Unique identifier for the message
            requeue: Whether to requeue the message
        """
        pass

    @abstractmethod
    def get_queue_size(self, queue_name: Optional[str] = None) -> int:
        """
        Get the number of messages in a queue.

        Args:
            queue_name: Name of the queue to check. If None, checks the default work queue.

        Returns:
            Number of messages in the queue
        """
        pass
