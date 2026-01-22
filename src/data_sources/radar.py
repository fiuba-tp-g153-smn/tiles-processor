"""Weather Radar data source implementation (placeholder)."""

from logging import getLogger
from pathlib import Path

from data_sources.base import DataSource, ImageInfo, DiscoveryConfig

logger = getLogger(__name__)


class RadarDataSource(DataSource):
    """
    Data source for weather radar imagery.

    This is a placeholder implementation. To be implemented when
    integrating with a weather radar data source (e.g., NEXRAD).
    """

    def __init__(self, radar_id: str = "nexrad"):
        """
        Initialize Radar data source.

        Args:
            radar_id: Identifier for the radar source (e.g., "nexrad")
        """
        self._radar_id = radar_id

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return f"radar_{self._radar_id}"

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return "radar"

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new radar images that need processing.

        TODO: Implement actual radar discovery logic.

        Args:
            config: Discovery configuration

        Returns:
            Empty list (placeholder implementation)
        """
        logger.warning(f"[{self.source_id}] Radar discovery not implemented")
        return []

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download a radar image to the specified destination.

        TODO: Implement actual radar download logic.

        Args:
            source_uri: URI of the source image
            dest_path: Local path to save the downloaded file

        Returns:
            Path to the downloaded file.

        Raises:
            NotImplementedError: Always (placeholder implementation)
        """
        raise NotImplementedError(
            f"[{self.source_id}] Radar download not implemented for {source_uri}"
        )
