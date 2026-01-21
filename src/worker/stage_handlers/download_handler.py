"""Download stage handler - downloads satellite images from NOAA S3."""

import logging
from pathlib import Path

from clients.s3_client import S3Client
from config import Config
from constants import constants
from models.work_unit import WorkUnit
from worker.stage_handlers.base_handler import BaseStageHandler

logger = logging.getLogger(__name__)


class DownloadHandler(BaseStageHandler):
    """
    Handler for the DOWNLOAD stage.

    Downloads a single satellite image from NOAA's public S3 bucket
    and saves it to the local filesystem.

    Input: work_unit.paths.source_s3_uri
    Output: work_unit.paths.local_netcdf (path to downloaded file)
    """

    def __init__(self, config: Config):
        super().__init__(config)
        # S3 client for NOAA public bucket (unsigned access)
        self._s3_client = S3Client(
            constants.GOES19_BUCKET_NAME, max_concurrent_downloads=1
        )

    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """Download the satellite image from NOAA S3."""
        logger.info(f"[DOWNLOAD] Starting for {work_unit.image_id}")

        # Prepare output directory
        raw_dir = self._ensure_dir(self._get_band_dir(work_unit) / "raw")

        # Extract S3 key from the full URI
        # URI format: s3://noaa-goes19/path/to/file.nc or just the key
        source_uri = work_unit.paths.source_s3_uri
        if source_uri.startswith("s3://"):
            # Remove s3://bucket-name/ prefix
            parts = source_uri.replace("s3://", "").split("/", 1)
            s3_key = parts[1] if len(parts) > 1 else parts[0]
        else:
            s3_key = source_uri

        # Download the file
        local_path = raw_dir / work_unit.image_id

        # Use S3 client to download
        content = await self._s3_client.download_single_file(s3_key)

        if content is None:
            raise RuntimeError(f"Failed to download {s3_key}")

        # Write to local file
        local_path.write_bytes(content)
        logger.info(f"[DOWNLOAD] Saved to {local_path}")

        # Update work unit with the local path
        work_unit.paths.downloaded_file = str(local_path)

        return work_unit
