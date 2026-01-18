"""
GOES-19 Band 13 (Cloud Tops) Processing Job.

This module processes GOES-19 ABI Band 13 (10.33 µm - Clean IR Window) satellite
imagery to generate web map tiles for cloud top temperature visualization.

Pipeline Overview:
    1. DOWNLOAD: Fetches 24 images (4 hours) from NOAA's noaa-goes19 S3 bucket
    2. GEOREFERENCE: Applies GOES satellite projection and coordinate transformation
    3. BRIGHTNESS TEMP: Converts radiance to temperature using Planck equation
    4. GEOTIFF: Creates colorized RGBA GeoTIFFs clipped to configured bounds
    5. TILES: Generates XYZ web tiles (zoom 3-7, WEBP format, Leaflet-compatible)
    6. UPLOAD: Uploads tiles to MinIO S3 bucket
    7. CLEANUP: Deletes all local files (storage is in S3 only)

Band 13 Specifications:
    - Wavelength: 10.33 µm (Clean Infrared Window)
    - Purpose: Cloud top temperature monitoring, storm tracking
    - Temperature range: 183.15K to 323.15K (-90°C to +50°C)
    - Color palette: Grayscale → Red (cold clouds appear red)

Execution Frequency:
    GOES-19 publishes Full Disk images every 10 minutes.
    Recommended schedule: */30 * * * * (every 30 minutes)

Storage Strategy:
    - All tiles are stored in MinIO S3 bucket (no local retention)
    - Local files are temporary and deleted after upload
    - S3 retention: keeps newest 26 tilesets (4+ hours of data)

Example GOES-19 filename:
    OR_ABI-L1b-RadF-M6C13_G19_s20250141230210_e20250141239518_c20250141239557.nc
    └── s20250141230210 = start time: 2025, day 014, 12:30:21.0 UTC
"""

import logging
import shutil
from typing import Callable, List
from datetime import datetime, UTC, timedelta
from pathlib import Path

from constants import constants
from config import Config
from clients.s3_client import S3Client
from services.compute_brightness_temperatures import (
    ComputeBrightnessTemperaturesService,
)
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from services.generate_tiles import GenerateTilesService
from services.setup_goes_georreferencing import SetupGOESGeorreferencingService

logger = logging.getLogger(__name__)


class ProcessBand13Job:
    """
    Scheduled job for processing GOES-19 Band 13 (Cloud Tops) imagery.

    This job downloads the last 24 satellite images (4 hours of data at 10-minute
    intervals), processes them through the full pipeline, uploads tiles to MinIO,
    and cleans up all local files.

    Attributes:
        _noaa_s3_client: S3 client for NOAA's public bucket (unsigned access)
        _minio_s3_client: S3 client for MinIO bucket (authenticated access)
        _s3_prefix: S3 key prefix for this band's tiles

    Pipeline stages:
        1. Download (Smart Skip) - checks S3 for existing tiles first
        2. → SetupGOESGeorreferencingService (georeferencing)
        3. → ComputeBrightnessTemperaturesService (Planck equation)
        4. → GenerateGeoTIFFFilesService (colorized GeoTIFFs)
        5. → GenerateTilesService (XYZ web tiles)
        6. → Upload to MinIO S3
        7. → Cleanup (delete ALL local files, S3 retention: 26 tilesets)

    Storage:
        - Local: Temporary only, deleted after upload
        - S3: band_13/tiles/{tileset_id}_tiles/{z}/{x}/{y}.webp
    """

    def __init__(self):
        self._config = Config()
        self._bucket_name = constants.GOES19_BUCKET_NAME
        self._l1b_products_path = "ABI-L1b-RadF"
        self._product_base_file_pattern = "C13_G19"
        self._s3_prefix = "band_13/tiles"

        # S3 client for NOAA public bucket (unsigned access for downloads)
        self._noaa_s3_client = S3Client(self._bucket_name, max_concurrent_downloads=6)

        # S3 client for MinIO bucket (authenticated access for uploads)
        self._minio_s3_client = S3Client.create_with_credentials(
            bucket_name=self._config.S3_TILES_DATA_BUCKET_NAME,
            endpoint=self._config.S3_TILES_DATA_ENDPOINT,
            access_key=self._config.S3_TILES_DATA_RW_ACCESS_KEY,
            secret_key=self._config.S3_TILES_DATA_RW_SECRET_KEY,
            secure=self._config.S3_TILES_DATA_SECURE,
        )

    async def run(self):
        current_time = datetime.now(UTC)
        dirs = self._prepare_directories()

        logger.info(
            "Starting Band 13 job - searching for last 24 satellite images (4 hours)"
        )

        files_to_process = await self._get_files_to_process(current_time, dirs)
        if not files_to_process:
            logger.info("No new files to process - all images already have tiles")
            await self._perform_cleanup(dirs)
            return

        logger.info(
            f"Found {len(files_to_process)} new images to process (tiles not yet generated)"
        )
        await self._run_processing_pipeline(files_to_process, dirs)
        await self._perform_cleanup(dirs)

    def _prepare_directories(self) -> dict[str, Path]:
        base = Path.cwd() / self._config.TMP_DIR / "band_13"
        dirs = {
            "raw": base / "raw",
            "tiles": base / "tiles",
            "geotiff": base / "geotiff",
        }
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)
        return dirs

    async def _get_files_to_process(
        self, current_time: datetime, dirs: dict[str, Path]
    ) -> dict:
        """Download missing files and filter out those already processed (checking S3)."""

        # Pre-fetch existing tilesets from S3 bucket
        existing_s3_tilesets = await self._get_existing_s3_tilesets()
        logger.info(f"Found {len(existing_s3_tilesets)} existing tilesets in S3")

        def check_tiles_exist_in_s3(s3_key: str) -> bool:
            stem = Path(s3_key).stem
            tileset_name = f"{stem}_tiles"
            return tileset_name in existing_s3_tilesets

        downloaded = await self._download_last_hour_files(
            current_time, dirs["raw"], skip_if=check_tiles_exist_in_s3
        )

        # files with None content were skipped by skip_if (tiles exist in S3)
        return {k: v for k, v in downloaded.items() if v is not None}

    async def _get_existing_s3_tilesets(self) -> set:
        """Get set of tileset names that exist in MinIO S3 bucket."""
        try:
            prefixes = await self._minio_s3_client.list_prefixes(
                f"{self._s3_prefix}/", delimiter="/"
            )
            tilesets = set()
            for prefix in prefixes:
                tileset_name = prefix.rstrip("/").split("/")[-1]
                tilesets.add(tileset_name)
            return tilesets
        except Exception as e:
            logger.warning(f"Error listing S3 tilesets: {e}")
            return set()

    async def _run_processing_pipeline(self, files: dict, dirs: dict[str, Path]):
        """
        Async Pipeline Execution Pattern:

        This method orchestrates multiple async services sequentially, with each
        service internally parallelizing its work using asyncio.Semaphore for
        concurrency control.

        Execution Flow:
            1. Files are processed in batches (BATCH_SIZE=4) to limit memory
            2. Each batch awaits 3 async services sequentially (geo → temp → tiff)
            3. Within each service, files are processed concurrently via Semaphore
            4. After all batches, tile generation runs once on all GeoTIFFs

        Memory Management:
            - Batching prevents loading all 24 datasets into memory at once
            - gc.collect() forces garbage collection between pipeline stages
            - del statements explicitly release large numpy arrays
        """
        import gc

        file_count = len(files)
        # Process in batches to control memory usage
        # 24 files / 4 = 6 batches. This prevents holding all datasets in memory.
        BATCH_SIZE = 4

        all_geotiff_files = []
        file_items = list(files.items())

        for i in range(0, file_count, BATCH_SIZE):
            batch_items = file_items[i : i + BATCH_SIZE]
            batch_files = dict(batch_items)
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (file_count + BATCH_SIZE - 1) // BATCH_SIZE

            logger.info(
                f"Processing Batch {batch_num}/{total_batches} ({len(batch_files)} files)"
            )

            try:
                # 1. Georeference
                logger.info(f"  [1/3] Georeferencing...")
                geo_data = await SetupGOESGeorreferencingService(batch_files).run()

                # 2. Brightness Temp
                logger.info(f"  [2/3] Computing brightness temperatures...")
                bt_data = await ComputeBrightnessTemperaturesService(geo_data).run()

                # Clear georeferenced data to free memory
                del geo_data
                gc.collect()

                # 3. GeoTIFF
                logger.info(f"  [3/3] Generating GeoTIFFs...")
                geotiff_files = await GenerateGeoTIFFFilesService(
                    bt_data,
                    dirs["geotiff"],
                    self._config,
                    color_palette=GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE,
                    vmin=183.15,
                    vmax=323.15,
                    product_name="Cloud_Tops",
                ).run()

                all_geotiff_files.extend(geotiff_files)

                # Clear brightness temp data to free memory
                del bt_data
                del geotiff_files
                gc.collect()  # Force garbage collection after each batch check

                logger.info(f"Batch {batch_num} complete")

            except Exception as e:
                logger.exception(f"Pipeline failed during batch {batch_num}: {e}")
                raise

        # 4. Tiles (Process all tiles at once as it iterates files from disk)
        if all_geotiff_files:
            logger.info(
                f"[4/5] Generating web tiles for {len(all_geotiff_files)} GeoTIFFs..."
            )
            await GenerateTilesService(all_geotiff_files, dirs["tiles"]).run()
            logger.info(f"[4/5] Tile generation complete")

            # 5. Upload tiles to MinIO
            logger.info(f"[5/5] Uploading tiles to MinIO...")
            await self._upload_tiles_to_minio(all_geotiff_files, dirs["tiles"])
            logger.info(f"[5/5] Tile upload complete")

    async def _perform_cleanup(self, dirs: dict[str, Path]):
        """
        Cleanup local and S3 storage.

        Local: Delete ALL files (no local retention, S3 is the source of truth)
        S3: Keep newest 26 tilesets (4+ hours of data)
        """
        logger.info("Running cleanup...")

        # Delete all local files (S3 is the source of truth)
        self._cleanup_local_directory(dirs["raw"])
        self._cleanup_local_directory(dirs["geotiff"])
        self._cleanup_local_directory(dirs["tiles"])

        # S3 retention: keep newest 26 tilesets
        await self._cleanup_s3_tiles(keep_count=26)

    def _cleanup_local_directory(self, directory: Path):
        """Delete all files in a directory (no local retention)."""
        if not directory.exists():
            return

        try:
            # Delete all contents but keep the directory
            for item in directory.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            logger.info(f"Cleanup: cleared {directory.name}/")
        except Exception as e:
            logger.warning(f"Cleanup failed for {directory}: {e}")

    async def _upload_tiles_to_minio(
        self, geotiff_files: List[Path], tiles_dir: Path
    ) -> None:
        """Upload generated tile directories to MinIO S3 bucket."""
        await self._minio_s3_client.ensure_bucket_exists()

        for geotiff_path in geotiff_files:
            tileset_name = f"{geotiff_path.stem}_tiles"
            local_tileset_dir = tiles_dir / tileset_name
            s3_key_prefix = f"{self._s3_prefix}/{tileset_name}"

            if local_tileset_dir.exists():
                await self._minio_s3_client.upload_directory(
                    local_tileset_dir, s3_key_prefix
                )

    async def _cleanup_s3_tiles(self, keep_count: int) -> None:
        """Delete old tile directories from MinIO S3 bucket (keeps newest keep_count)."""
        try:
            prefixes = await self._minio_s3_client.list_prefixes(
                f"{self._s3_prefix}/", delimiter="/"
            )

            if len(prefixes) <= keep_count:
                logger.info(
                    f"S3 cleanup: {len(prefixes)} tilesets in S3, keeping all (threshold: {keep_count})"
                )
                return

            sorted_prefixes = sorted(prefixes)
            prefixes_to_delete = sorted_prefixes[:-keep_count]
            logger.info(
                f"S3 cleanup: deleting {len(prefixes_to_delete)} old tilesets from S3"
            )

            for prefix in prefixes_to_delete:
                tileset_name = prefix.rstrip("/").split("/")[-1]
                s3_prefix = f"{self._s3_prefix}/{tileset_name}"
                try:
                    await self._minio_s3_client.delete_prefix(s3_prefix)
                    logger.info(f"S3 cleanup: deleted {s3_prefix}")
                except Exception as e:
                    logger.warning(f"S3 cleanup failed for {s3_prefix}: {e}")
        except Exception as e:
            logger.error(f"S3 cleanup failed: {e}")

    async def _download_last_hour_files(
        self,
        current_time: datetime,
        local_cache_dir: Path,
        skip_if: Callable[[str], bool] = None,
    ) -> dict[str, bytes]:
        """
        Async Download with Concurrency-Limited S3 Client.

        This method coordinates multiple async S3 downloads across hourly buckets.
        The S3Client internally limits concurrent downloads (max_concurrent_downloads=6)
        using asyncio.Semaphore to prevent overwhelming the network or S3 rate limits.

        Pattern:
            - Sequential iteration over hours (current → past)
            - Each hour's download is an async operation with internal parallelism
            - skip_if callback enables smart caching (skip already-processed files)
        """
        target_files = 24
        all_files = {}
        hours_back = 0

        while len(all_files) < target_files and hours_back <= 5:
            # Determine how many files we still need
            needed = target_files - len(all_files)
            search_time = current_time - timedelta(hours=hours_back)

            # Helper to download one hour batch
            batch = await self._download_hour_batch(
                search_time, needed, local_cache_dir, skip_if
            )

            all_files.update(batch)
            hours_back += 1

        logger.info(
            f"Download complete: collected {len(all_files)}/{target_files} images"
        )
        return all_files

    async def _download_hour_batch(
        self, time: datetime, needed: int, cache_dir: Path, skip_if: Callable
    ) -> dict:
        path = self._build_directory_path(time)
        hour_str = time.strftime("%Y-%m-%d %H:00 UTC")
        logger.info(
            f"Searching S3 for images from {hour_str} (need {needed} more to reach 24)"
        )

        # For previous hours (needed < 6), only get the newest files (minutes 50,40,30...)
        # Logic: 1 file every 10 mins = 6 files/hour.
        # If we need N files, we look for minutes >= 60 - (N * 10).
        file_filter = None
        if needed < 6:
            min_minute = 60 - (needed * 10)
            file_filter = lambda f: self._is_minute_ge(f, min_minute)

        return await self._noaa_s3_client.download_folder(
            path,
            file_pattern=self._product_base_file_pattern,
            file_filter=file_filter,
            local_cache_dir=cache_dir,
            skip_if=skip_if,
        )

    def _is_minute_ge(self, file_path: str, min_minute: int) -> bool:
        """Filter helper: returns True if file minute >= min_minute."""
        m = self._extract_minute_from_filename(file_path)
        return m is not None and m >= min_minute

    def _build_directory_path(self, time: datetime) -> str:
        return f"{self._l1b_products_path}/{time.strftime('%Y/%j/%H')}"

    def _extract_minute_from_filename(self, file_path: str) -> int | None:
        try:
            filename = file_path.split("/")[-1]
            start_idx = filename.find("_s")
            if start_idx == -1:
                return None
            # _sYYYYJJJHHMMSSS -> extract MM (idx 9-11 relative to start of timestamp)
            timestamp_str = filename[start_idx + 2 : start_idx + 16]
            return int(timestamp_str[9:11])
        except (ValueError, IndexError):
            return None
