"""Base classes and protocols for data sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Set


@dataclass
class DiscoveryConfig:
    """Configuration for image discovery."""

    current_time: datetime
    existing_tilesets: Set[str]
    in_progress_images: Set[str]
    bounds: dict


@dataclass
class ImageInfo:
    """Information about a discovered image."""

    image_id: str
    source_uri: str
    data_source_id: str
    processor_id: str
    output_prefix: str


class DataSource(ABC):
    """
    Abstract base class for data sources.

    Data sources are responsible for:
    1. Discovering new images from their source (e.g., S3 bucket)
    2. Downloading images to local storage

    Implementations should be stateless and thread-safe.
    """

    @property
    @abstractmethod
    def source_id(self) -> str:
        """Unique identifier for this data source."""

    @property
    @abstractmethod
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""

    @abstractmethod
    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new images from the data source.

        Args:
            config: Discovery configuration with current time, existing tilesets, etc.

        Returns:
            List of ImageInfo for images that need processing.
        """

    @abstractmethod
    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download an image to the specified destination.

        Args:
            source_uri: URI of the source image (e.g., S3 key)
            dest_path: Local path to save the downloaded file

        Returns:
            Path to the downloaded file.

        Raises:
            RuntimeError: If download fails.
        """
