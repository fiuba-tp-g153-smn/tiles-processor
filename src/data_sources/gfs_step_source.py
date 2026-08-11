"""GFS step data source: pulls a cached GRIB from S3 for product processing."""

import json
import logging
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from models.gfs_config import GFS_MSLP_CONFIG, GFS_STEP_DATA_SOURCE_ID

logger = logging.getLogger(__name__)


class GfsStepDataSource(DataSource):
    """Downloads the cached GRIB for one already-ingested forecast step.

    `GfsGribDownloader` enqueues one WorkUnit per (step, enabled product); this
    source handles their download stage by reading `grib_path` out of the work
    unit's JSON `source_uri` and pulling that object from S3.

    One source serves all three products: they read the same object, and only
    the processor downstream differs. `discover_images` always returns [] —
    these units are fanned out by the inline downloader, never discovered by
    the producer.
    """

    def __init__(self, s3_client: S3Client):
        self._s3_client = s3_client

    @property
    def source_id(self) -> str:
        return GFS_STEP_DATA_SOURCE_ID

    @property
    def processor_id(self) -> str:
        """Nominal processor; the real one travels on each WorkUnit.

        Products are rendered by different processors (`gfs_mslp` vs
        `gfs_upper_level`), so this value is only a placeholder for the
        DataSource interface and is never used for routing.
        """
        return GFS_MSLP_CONFIG.processor_id

    @property
    def uses_existing_tilesets(self) -> bool:
        """`discover_images` always returns []; the LIST would feed nothing."""
        return False

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """Always []: steps are fanned out by GfsGribDownloader, not discovered."""
        return []

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Download the cached GRIB named by the work unit."""
        grib_key = json.loads(source_uri)["grib_path"]
        target = dest_path.with_suffix(".grib2")
        target.parent.mkdir(parents=True, exist_ok=True)

        logger.info("[GFS] Downloading cached GRIB %s → %s", grib_key, target)
        await self._s3_client.download_to_file(grib_key, target)
        logger.info("[GFS] GRIB ready (%.2f MB)", target.stat().st_size / 1e6)
        return target
