"""Cleanup stage handler - removes temporary files and manages S3 retention."""

import logging
import shutil
from pathlib import Path

from clients.progress_tracker import ProgressTracker
from clients.s3_client import S3Client
from config import Config
from models.work_unit import WorkUnit
from worker.stage_handlers.base_handler import BaseStageHandler

logger = logging.getLogger(__name__)


class CleanupHandler(BaseStageHandler):
    """
    Handler for the CLEANUP stage.

    Removes all temporary files created during processing for this image.
    Also manages S3 retention by keeping only the newest N tilesets.

    Input: All paths in work_unit.paths
    Output: None (terminal stage)

    Files cleaned:
        - local_netcdf: Raw downloaded file
        - georef_data: Pickled georeferenced dataset
        - temp_data: Pickled brightness temperature data
        - geotiff: Generated GeoTIFF file
        - tiles_dir: Generated tile directory
    """

    # Keep newest N tilesets in S3
    S3_RETENTION_COUNT = 26

    def __init__(self, config: Config):
        super().__init__(config)
        # S3 client for MinIO (for retention management)
        self._minio_client = S3Client.create_with_credentials(
            bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
            endpoint=config.S3_TILES_DATA_ENDPOINT,
            access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
            secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
            secure=config.S3_TILES_DATA_SECURE,
        )

        # Progress tracker to mark images as completed
        tracker_file = Path(config.TMP_DIR) / "progress_tracker.json"
        self._progress_tracker = ProgressTracker(tracker_file)

    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """Clean up temporary files for this work unit."""
        logger.info(f"[CLEANUP] Starting for {work_unit.image_id}")

        # Clean up local files
        files_to_delete = [
            work_unit.paths.local_netcdf,
            work_unit.paths.georef_data,
            work_unit.paths.temp_data,
            work_unit.paths.geotiff,
        ]

        for file_path in files_to_delete:
            self._cleanup_file(file_path)

        # Clean up tiles directory
        if work_unit.paths.tiles_dir:
            tiles_dir = Path(work_unit.paths.tiles_dir)
            if tiles_dir.exists():
                try:
                    shutil.rmtree(tiles_dir)
                    logger.debug(f"Deleted directory: {tiles_dir}")
                except Exception as e:
                    logger.warning(f"Failed to delete {tiles_dir}: {e}")

        # Manage S3 retention (keep newest N tilesets)
        await self._cleanup_s3_retention(work_unit.band_config.s3_prefix)

        # Mark image as completed in progress tracker
        self._progress_tracker.mark_completed(work_unit.image_id, work_unit.band_id)

        logger.info(f"[CLEANUP] Completed for {work_unit.image_id}")

        return work_unit

    async def _cleanup_s3_retention(self, s3_prefix: str) -> None:
        """
        Delete old tilesets from S3 to maintain retention policy.

        Keeps the newest S3_RETENTION_COUNT tilesets, deletes older ones.
        Tilesets are sorted alphabetically (timestamps ensure proper ordering).
        """
        try:
            # List all tilesets under this prefix
            prefixes = await self._minio_client.list_prefixes(
                f"{s3_prefix}/", delimiter="/"
            )

            if len(prefixes) <= self.S3_RETENTION_COUNT:
                logger.debug(
                    f"S3 cleanup: {len(prefixes)} tilesets, keeping all "
                    f"(threshold: {self.S3_RETENTION_COUNT})"
                )
                return

            # Sort and identify old tilesets to delete
            sorted_prefixes = sorted(prefixes)
            prefixes_to_delete = sorted_prefixes[: -self.S3_RETENTION_COUNT]

            logger.info(
                f"S3 cleanup: deleting {len(prefixes_to_delete)} old tilesets "
                f"(keeping newest {self.S3_RETENTION_COUNT})"
            )

            for prefix in prefixes_to_delete:
                tileset_name = prefix.rstrip("/").split("/")[-1]
                full_prefix = f"{s3_prefix}/{tileset_name}"
                try:
                    await self._minio_client.delete_prefix(full_prefix)
                    logger.info(f"S3 cleanup: deleted {full_prefix}")
                except Exception as e:
                    logger.warning(f"S3 cleanup failed for {full_prefix}: {e}")

        except Exception as e:
            logger.error(f"S3 retention cleanup failed: {e}")
