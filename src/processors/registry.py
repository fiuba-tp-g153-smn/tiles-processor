"""Registry for image processors."""

import logging
from typing import Dict, Type

from processors.base_processor import ImageProcessor

logger = logging.getLogger(__name__)


class ProcessorRegistry:
    """
    Registry for managing image processors.

    Processors are registered by ID and can be retrieved for processing work units.
    The registry stores processor classes (not instances) to allow lazy instantiation
    with the appropriate configuration.
    """

    _processors: Dict[str, Type[ImageProcessor]] = {}

    @classmethod
    def register(cls, processor_id: str, processor_class: Type[ImageProcessor]) -> None:
        """
        Register a processor class.

        Args:
            processor_id: Unique identifier for the processor (e.g., "goes_band_13")
            processor_class: The processor class to register
        """
        cls._processors[processor_id] = processor_class
        logger.info(f"Registered processor: {processor_id}")

    @classmethod
    def get(cls, processor_id: str) -> Type[ImageProcessor]:
        """
        Get a processor class by ID.

        Args:
            processor_id: The ID of the processor

        Returns:
            The processor class

        Raises:
            KeyError: If no processor with the given ID exists
        """
        if processor_id not in cls._processors:
            raise KeyError(
                f"Unknown processor: {processor_id}. "
                f"Available: {list(cls._processors.keys())}"
            )
        return cls._processors[processor_id]

    @classmethod
    def get_all_ids(cls) -> list[str]:
        """Get all registered processor IDs."""
        return list(cls._processors.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered processors. Useful for testing."""
        cls._processors.clear()
