"""WRF-ARG4K data source — discovers FIELD2D NetCDF files via a repository."""

from logging import getLogger
from pathlib import Path

from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from data_sources.wrf_repository import WrfFileRepository
from models.wrf_config import WrfProductConfig, parse_wrf_filename

logger = getLogger(__name__)


class WrfDataSource(DataSource):
    """
    Data source for WRF-ARG4K model output (SMN Argentina).

    Discovers FIELD2D .nc files via the injected repository (local folder or
    S3 bucket with the same layout). One instance per
    product: each creates image_ids unique to (product_id, init_tag, fxxx).
    The processor derives the FIELD3D path from the FIELD2D path when needed.

    File naming convention:
        WRF_ARG4K.FCST_L0_FIELD2D.01H.<INIT_TAG>.<FXXX>.M000.nc
    F000 (initialization hour) is processed for every product except those
    with ``skip_f000=True`` (1h-accumulation products lacking pp01H at init).
    """

    def __init__(self, product_config: WrfProductConfig, repository: WrfFileRepository):
        self._product_config = product_config
        self._repository = repository

    @property
    def source_id(self) -> str:
        return f"wrf_{self._product_config.product_id}"

    @property
    def processor_id(self) -> str:
        return "wrf"

    @property
    def product_config(self) -> WrfProductConfig:
        """The WRF product configuration for this source."""
        return self._product_config

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """Discover unprocessed WRF forecast steps for this product."""
        source_uris = await self._repository.list_files()
        if not source_uris:
            logger.warning(
                "[%s] No WRF FIELD2D files found in input dir", self.source_id
            )
            return []

        new_images = []
        for source_uri in source_uris:
            filename = Path(source_uri).name
            try:
                parsed = parse_wrf_filename(filename)
            except ValueError as e:
                logger.debug("Skipping file with invalid name: %s (%s)", filename, e)
                continue

            # F000 se descarta solo en productos de acumulado 1h (pp01H), donde
            # la variable no existe en la hora de inicialización. El resto sí.
            if parsed["fnum"] == 0 and self._product_config.skip_f000:
                continue

            image_id = (
                f"{self._product_config.product_id}_"
                f"{parsed['init_tag']}_{parsed['fxxx']}"
            )

            if image_id in config.existing_tilesets:
                logger.debug("Skipping %s (already processed)", image_id)
                continue
            if image_id in config.in_progress_images:
                logger.debug("Skipping %s (in progress)", image_id)
                continue

            new_images.append(
                ImageInfo(
                    image_id=image_id,
                    source_uri=source_uri,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._product_config.s3_tiles_prefix,
                )
            )

        logger.info(
            "[%s] Found %d new forecast steps to process",
            self.source_id,
            len(new_images),
        )
        return new_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Copy WRF FIELD2D NetCDF file to the worker's work directory."""
        dest = await self._repository.download(source_uri, dest_path)
        logger.info(
            "[%s] Copied %s → %s",
            self.source_id,
            Path(source_uri).name,
            dest,
        )
        return dest
