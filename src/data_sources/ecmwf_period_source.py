"""ECMWF period data source: downloads GRIB from S3 for period processing."""

import json
import logging
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from models.ecmwf_config import ECMWF_TP_CONFIG, EcmwfProductConfig

logger = logging.getLogger(__name__)


class EcmwfPeriodDataSource(DataSource):
    """
    Data source for ECMWF period processing.

    EcmwfGribDownloader enqueues one WorkUnit per 3h period; this data source
    handles the download step: it reads the grib_path from source_uri (JSON)
    and downloads the cached GRIB from S3 to a local temp file.

    discover_images() always returns [] because periods are never discovered by
    the producer — they are enqueued dynamically by EcmwfGribDownloader.
    """

    def __init__(
        self,
        product_config: EcmwfProductConfig = ECMWF_TP_CONFIG,
        s3_client: S3Client | None = None,
    ):
        self._product_config = product_config
        self._s3_client = s3_client

    @property
    def source_id(self) -> str:
        return "ecmwf_tp_period"

    @property
    def processor_id(self) -> str:
        return "ecmwf_period_processor"

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """Always returns [] — periods are enqueued by EcmwfGribDownloader, not the producer."""
        return []

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download the GRIB file from S3 for a given period work unit.

        Args:
            source_uri: JSON string containing at minimum {"grib_path": "..."}.
            dest_path: Suggested destination path; extension will be set to .grib.

        Returns:
            Path to the downloaded .grib file.
        """
        if self._s3_client is None:
            raise RuntimeError("EcmwfPeriodDataSource requires an S3 client")

        period_meta = json.loads(source_uri)
        grib_s3_key = period_meta["grib_path"]

        target = dest_path.with_suffix(".grib")
        target.parent.mkdir(parents=True, exist_ok=True)

        logger.info("[ECMWF] Downloading GRIB from S3: %s → %s", grib_s3_key, target)
        await self._s3_client.download_to_file(grib_s3_key, target)
        logger.info(
            "[ECMWF] GRIB downloaded (%.1f MB)", target.stat().st_size / 1e6
        )
        return target
