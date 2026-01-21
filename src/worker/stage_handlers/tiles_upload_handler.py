"""Tiles and upload stage handler - generates tiles and uploads to MinIO."""

import logging
import shutil
import subprocess
import uuid
from pathlib import Path

from clients.s3_client import S3Client
from config import Config
from models.work_unit import WorkUnit
from worker.stage_handlers.base_handler import BaseStageHandler

logger = logging.getLogger(__name__)


class TilesUploadHandler(BaseStageHandler):
    """
    Handler for the TILES_AND_UPLOAD stage.

    Generates XYZ web tiles from GeoTIFF and uploads them to MinIO S3.
    This combines tile generation and upload into a single stage to avoid
    storing large tile directories on the shared filesystem longer than necessary.

    Input: work_unit.paths.geotiff
    Output: work_unit.paths.tiles_dir, work_unit.paths.s3_tileset_prefix
    """

    # gdal2tiles settings
    GDAL_PROCESSES = 2
    ZOOM_LEVELS = "3-7"

    def __init__(self, config: Config):
        super().__init__(config)
        # S3 client for MinIO (authenticated)
        self._minio_client = S3Client.create_with_credentials(
            bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
            endpoint=config.S3_TILES_DATA_ENDPOINT,
            access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
            secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
            secure=config.S3_TILES_DATA_SECURE,
        )

    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """Generate tiles and upload to MinIO."""
        logger.info(f"[TILES_UPLOAD] Starting for {work_unit.image_id}")

        if not work_unit.paths.geotiff:
            raise ValueError("geotiff path is required for TILES_AND_UPLOAD stage")

        geotiff_path = Path(work_unit.paths.geotiff)
        if not geotiff_path.exists():
            raise FileNotFoundError(f"GeoTIFF file not found: {geotiff_path}")

        # Prepare tiles directory
        tiles_dir = self._ensure_dir(self._get_band_dir(work_unit) / "tiles")

        # Generate tiles
        import asyncio

        tiles_output_dir = await asyncio.to_thread(
            self._generate_tiles, geotiff_path, tiles_dir
        )

        logger.info(f"[TILES_UPLOAD] Tiles generated at {tiles_output_dir}")

        # Upload to MinIO
        band_config = work_unit.band_config
        tileset_name = f"{geotiff_path.stem}_tiles"
        s3_prefix = f"{band_config.s3_prefix}/{tileset_name}"

        await self._minio_client.ensure_bucket_exists()
        await self._minio_client.upload_directory(tiles_output_dir, s3_prefix)

        logger.info(
            f"[TILES_UPLOAD] Uploaded to s3://{self._config.S3_TILES_DATA_BUCKET_NAME}/{s3_prefix}"
        )

        # Update work unit
        work_unit.paths.tiles_dir = str(tiles_output_dir)
        work_unit.paths.s3_tileset_prefix = s3_prefix

        return work_unit

    def _generate_tiles(self, geotiff_path: Path, output_base_dir: Path) -> Path:
        """Generate XYZ tiles using gdal2tiles."""
        tileset_name = f"{geotiff_path.stem}_tiles"
        tiles_output_dir = output_base_dir / tileset_name

        # Use temporary directory for atomic operation
        tmp_tiles_dir = output_base_dir / str(uuid.uuid4())
        tmp_tiles_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Run gdal2tiles
            cmd = [
                "gdal2tiles.py",
                "-z",
                self.ZOOM_LEVELS,
                "-w",
                "leaflet",
                "--tiledriver=WEBP",
                f"--processes={self.GDAL_PROCESSES}",
                str(geotiff_path),
                str(tmp_tiles_dir),
            ]

            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                logger.error(f"gdal2tiles failed: {result.stderr}")
                raise RuntimeError(f"gdal2tiles failed for {geotiff_path.name}")

            # Atomic move to final location
            if tiles_output_dir.exists():
                shutil.rmtree(tiles_output_dir)

            tmp_tiles_dir.rename(tiles_output_dir)

            return tiles_output_dir

        except Exception as e:
            if tmp_tiles_dir.exists():
                shutil.rmtree(tmp_tiles_dir)
            raise
