"""Abstract base class for inline (non-subprocess) processors."""

from abc import ABC, abstractmethod

from clients.message_queue_client import MessageQueueClient
from models.work_unit import WorkUnit


class InlineProcessor(ABC):
    """
    Processor that runs in the main worker process (no subprocess isolation).

    Use this for work units whose processing does NOT require heavy scientific
    libraries (cfgrib, rioxarray, GDAL) and needs access to infrastructure
    clients (e.g. S3, RabbitMQ) that cannot be passed into a subprocess.
    """

    @abstractmethod
    async def process(
        self,
        file_path: str,
        work_unit: WorkUnit,
        mq_client: MessageQueueClient,
    ) -> None:
        """
        Process the downloaded file inline.

        Args:
            file_path: Local path to the downloaded file.
            work_unit: Metadata for the work unit being processed.
            mq_client: RabbitMQ client for publishing follow-up work units.
        """
