"""Weather Radar data source - discovers H5 files via a repository."""

from logging import getLogger
from pathlib import Path

from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from data_sources.radar_repository import RadarFileRepository
from models.radar_config import (
    RadarProductConfig,
    parse_radar_filename,
)

logger = getLogger(__name__)


class RadarDataSource(DataSource):
    """
    Data source for weather radar imagery.

    Discovers .H5 files via a RadarFileRepository (local or remote).
    Files follow SINARAME naming convention:
        RMA1_0315_01_DBZH_20260114T170328Z.H5

    Each RadarDataSource instance is configured for a specific product
    (DBZH, VRAD, RHOHV, etc.) and only discovers files matching that product
    and the correct subvolume (01 for most, 02 for VRAD).
    """

    # Cantidad máxima de imágenes a descubrir por ciclo por producto.
    # 12 imágenes necesarias + 2 de margen = 14 (cada ~5 min, cubre ~70 min).
    # Análogo a Goes19AbiDataSource.TARGET_IMAGES = 26 (24 + 2 margen).
    TARGET_IMAGES = 14

    def __init__(
        self, product_config: RadarProductConfig, repository: RadarFileRepository
    ):
        """
        Initialize Radar data source for a specific product.

        Args:
            product_config: Configuration for the radar product
            repository: Repository for listing and downloading H5 files
        """
        self._product_config = product_config
        self._repository = repository

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return f"radar_{self._product_config.product_id}"

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return "radar"

    @property
    def product_config(self) -> RadarProductConfig:
        """Get the radar product configuration."""
        return self._product_config

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new radar images from local folder.

        Filters files by:
        1. Product (variable) matching this source's product_id
        2. Subvolume matching this source's expected subvolume
        3. Not already processed (not in existing_tilesets)
        4. Not in progress

        Args:
            config: Discovery configuration

        Returns:
            List of ImageInfo for files needing processing
        """
        source_uris = await self._repository.list_files()

        if not source_uris:
            logger.warning("[%s] No H5 files found by repository", self.source_id)
            return []

        new_images = []
        product_id = self._product_config.product_id
        expected_subvolume = self._product_config.subvolume

        for source_uri in source_uris:
            filepath = Path(source_uri)
            try:
                parsed = parse_radar_filename(filepath.name)
            except ValueError as e:
                logger.debug("Skipping file with invalid name: %s (%s)", filepath, e)
                continue

            # Filter by product (DBZH, VRAD, RHOHV, etc.)
            if parsed["variable"] != product_id:
                continue

            # Filtrar por subvolumen: cada producto define qué subvolumen usar
            # en radar_config.py (ej: DBZH usa "01", VRAD usa "02").
            # Archivos con subvolumen distinto al esperado se descartan.
            # Ejemplo: RMA12_0315_02_DBZH_*.H5 se ignora porque DBZH
            # espera subvolume="01".
            if parsed["subvolume"] != expected_subvolume:
                continue

            # Build image_id: radar_id/variable/timestamp
            image_id = (
                f"{parsed['radar_id']}_{parsed['variable']}_{parsed['timestamp']}"
            )

            # Check if already processed
            if image_id in config.existing_tilesets:
                logger.debug("Skipping %s (already processed)", image_id)
                continue

            # Check if in progress
            if image_id in config.in_progress_images:
                logger.debug("Skipping %s (in progress)", image_id)
                continue

            new_images.append(
                ImageInfo(
                    image_id=image_id,
                    source_uri=source_uri,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._product_config.s3_prefix,
                )
            )

        # Ordenar por timestamp descendente y tomar solo los más recientes
        new_images.sort(key=lambda img: img.image_id, reverse=True)
        target_images = new_images[: self.TARGET_IMAGES]

        logger.info(
            "[%s] Found %d new files, publishing %d (limit %d, from %d total H5 files)",
            self.source_id,
            len(new_images),
            len(target_images),
            self.TARGET_IMAGES,
            len(source_uris),
        )
        return target_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Copy radar file from local source to destination.

        Since files are already local, this just copies them to the
        worker's temp directory for processing.

        Args:
            source_uri: Absolute path to the source H5 file
            dest_path: Local path to save the file

        Returns:
            Path to the copied file (with .H5 extension).
        """
        dest_with_ext = await self._repository.download(source_uri, dest_path)

        logger.info(
            "[%s] Downloaded %s to %s",
            self.source_id,
            Path(source_uri).name,
            dest_with_ext,
        )
        return dest_with_ext
