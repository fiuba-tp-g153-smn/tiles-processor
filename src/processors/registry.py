"""Registry for image processors."""

from logging import getLogger
from typing import Dict, Type

from processors.base_processor import ImageProcessor

logger = getLogger(__name__)


class ProcessorRegistry:
    """
    Registry for managing image processors.

    Instance-based registry to avoid global state.
    Stores processor classes (not instances) to allow lazy instantiation.
    """

    def __init__(self):
        self._processors: Dict[str, Type[ImageProcessor]] = {}

    def register(
        self, processor_id: str, processor_class: Type[ImageProcessor]
    ) -> None:
        """
        Register a processor class.

        Args:
            processor_id: Unique identifier for the processor (e.g., "goes_band_13")
            processor_class: The processor class to register
        """
        self._processors[processor_id] = processor_class
        logger.info(f"Registered processor: {processor_id}")

    def get(self, processor_id: str) -> Type[ImageProcessor]:
        """
        Get a processor class by ID.

        Args:
            processor_id: The ID of the processor

        Returns:
            The processor class

        Raises:
            KeyError: If no processor with the given ID exists
        """
        if processor_id not in self._processors:
            raise KeyError(
                f"Unknown processor: {processor_id}. "
                f"Available: {list(self._processors.keys())}"
            )
        return self._processors[processor_id]

    def get_all_ids(self) -> list[str]:
        """Get all registered processor IDs."""
        return list(self._processors.keys())

    def clear(self) -> None:
        """Clear all registered processors. Useful for testing."""
        self._processors.clear()
