"""Registry for data sources."""

from logging import getLogger
from typing import Dict, List

from data_sources.base import DataSource

logger = getLogger(__name__)


class DataSourceRegistry:
    """
    Registry for managing data sources.

    Instance-based registry to avoid global state.
    """

    def __init__(self):
        self._sources: Dict[str, DataSource] = {}

    def register(self, source: DataSource) -> None:
        """
        Register a data source.

        Args:
            source: The data source to register.
        """
        self._sources[source.source_id] = source
        logger.info("Registered data source: %s", source.source_id)

    def get(self, source_id: str) -> DataSource:
        """
        Get a data source by ID.

        Args:
            source_id: The ID of the data source.

        Returns:
            The data source.

        Raises:
            KeyError: If no data source with the given ID exists.
        """
        if source_id not in self._sources:
            raise KeyError(
                f"Unknown data source: {source_id}. "
                f"Available: {list(self._sources.keys())}"
            )
        return self._sources[source_id]

    def get_all(self) -> List[DataSource]:
        """Get all registered data sources."""
        return list(self._sources.values())

    def get_all_ids(self) -> List[str]:
        """Get all registered data source IDs."""
        return list(self._sources.keys())

    def clear(self) -> None:
        """Clear all registered data sources. Useful for testing."""
        self._sources.clear()
