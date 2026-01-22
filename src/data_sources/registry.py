"""Registry for data sources."""

import logging
from typing import Dict, List

from data_sources.base import DataSource

logger = logging.getLogger(__name__)


class DataSourceRegistry:
    """
    Registry for managing data sources.

    Provides a central place to register and retrieve data sources.
    """

    _sources: Dict[str, DataSource] = {}

    @classmethod
    def register(cls, source: DataSource) -> None:
        """
        Register a data source.

        Args:
            source: The data source to register.
        """
        cls._sources[source.source_id] = source
        logger.info(f"Registered data source: {source.source_id}")

    @classmethod
    def get(cls, source_id: str) -> DataSource:
        """
        Get a data source by ID.

        Args:
            source_id: The ID of the data source.

        Returns:
            The data source.

        Raises:
            KeyError: If no data source with the given ID exists.
        """
        if source_id not in cls._sources:
            raise KeyError(
                f"Unknown data source: {source_id}. "
                f"Available: {list(cls._sources.keys())}"
            )
        return cls._sources[source_id]

    @classmethod
    def get_all(cls) -> List[DataSource]:
        """Get all registered data sources."""
        return list(cls._sources.values())

    @classmethod
    def get_all_ids(cls) -> List[str]:
        """Get all registered data source IDs."""
        return list(cls._sources.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered data sources. Useful for testing."""
        cls._sources.clear()
