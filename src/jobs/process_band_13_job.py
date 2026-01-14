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

Band 13 Specifications:
    - Wavelength: 10.33 µm (Clean Infrared Window)
    - Purpose: Cloud top temperature monitoring, storm tracking
    - Temperature range: 183.15K to 323.15K (-90°C to +50°C)
    - Color palette: Grayscale → Red (cold clouds appear red)

Execution Frequency:
    GOES-19 publishes Full Disk images every 10 minutes.
    Recommended schedule: */30 * * * * (every 30 minutes)

File Handling:
    - GOES files have unique timestamps in filenames (no collisions)
    - GeoTIFFs are atomically overwritten if same timestamp is reprocessed
    - Tile directories are deleted and replaced on reprocessing
    - Output: .tmp/band_13/geotiff/*.tif and .tmp/band_13/tiles/*_tiles/

Example GOES-19 filename:
    OR_ABI-L1b-RadF-M6C13_G19_s20250141230210_e20250141239518_c20250141239557.nc
    └── s20250141230210 = start time: 2025, day 014, 12:30:21.0 UTC
"""
import logging
from typing import Callable, Optional
from datetime import datetime, UTC, timedelta
from pathlib import Path

from constants import constants
from config import config
from clients import s3_client
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
    intervals), processes them through the full pipeline, and generates web map
    tiles for visualization.

    Attributes:
        _bucket_name: NOAA's public S3 bucket (noaa-goes19)
        _l1b_products_path: S3 prefix for ABI Level 1b Full Disk products
        _product_base_file_pattern: Filter pattern for Band 13 files (C13_G19)
        _s3_client: Async S3 client with concurrency limiting

    Pipeline stages:
        1. Download (Smart Skip + Caching)
           - Checks if tiles already exist (skips processing if true)
           - Checks local 'raw' directory (skips download if present)
           - Downloads from S3 if needed
        2. → SetupGOESGeorreferencingService (georeferencing)
        3. → ComputeBrightnessTemperaturesService (Planck equation)
        4. → GenerateGeoTIFFFilesService (colorized GeoTIFFs)
        5. → GenerateTilesService (XYZ web tiles)
        6. → Cleanup (Retention Policy: keeps last 26 files)

    Output directories:
        - Raw: {TMP_DIR}/band_13/raw/ (Cached input)
        - GeoTIFFs: {TMP_DIR}/band_13/geotiff/ (Intermediate)
        - Tiles: {TMP_DIR}/band_13/tiles/ (Final Output)
    """
    def __init__(self):
        self._bucket_name = constants.GOES19_BUCKET_NAME
        self._l1b_products_path = "ABI-L1b-RadF"
        self._product_base_file_pattern = "C13_G19"
        self._s3_client = s3_client.S3Client(
            self._bucket_name, max_concurrent_downloads=6
        )

    async def run(self):
        current_time = datetime.now(UTC)

        # Prepare raw directory for caching downloads
        raw_output_dir = Path.cwd() / config.TMP_DIR / "band_13" / "raw"
        raw_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare tiles directory to check for existing outputs
        tiles_output_dir = Path.cwd() / config.TMP_DIR / "band_13" / "tiles"

        def check_tiles_exist(s3_key: str) -> bool:
            """Check if tiles already exist for the given S3 file."""
            file_stem = Path(s3_key).stem
            expected_tile_dir = tiles_output_dir / f"{file_stem}_tiles"
            exists = expected_tile_dir.exists()
            if exists:
                logger.debug(f"Tiles exist for {s3_key}, skipping.")
            return exists

        # Download files from the last hour (6 files)
        # files that match 'check_tiles_exist' will return None as content
        downloaded_files = await self._download_last_hour_files(
            current_time, 
            local_cache_dir=raw_output_dir,
            skip_if=check_tiles_exist
        )
        
        # Filter only files that have content (new or not skipped)
        files_to_process = {
            k: v for k, v in downloaded_files.items() if v is not None
        }

        if not files_to_process:
            logger.info("All files have already been processed. Nothing to do.")
            return

        logger.info(f"Processing {len(files_to_process)} new files...")

        georreferenced_data = await SetupGOESGeorreferencingService(
            files_to_process
        ).run()
        logger.info("Georreferencing completed.")
        
        brightness_temperature_data = await ComputeBrightnessTemperaturesService(
            georreferenced_data
        ).run()
        logger.info("Brightness temperature computation completed.")

        geotiff_output_dir = Path.cwd() / config.TMP_DIR / "band_13" / "geotiff"
        geotiff_files = await GenerateGeoTIFFFilesService(
            brightness_temperature_data,
            geotiff_output_dir,
            color_palette=GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE,
            vmin=183.15,
            vmax=323.15,
            product_name="Cloud_Tops",
        ).run()
        logger.info("GeoTIFF generation completed.")

        await GenerateTilesService(geotiff_files, tiles_output_dir).run()
        logger.info("Tiles generation completed.")
        
        # Cleanup old files (retention policy)
        # We need to keep at least 24 images for the rolling window cache to work effective.
        # Keeping 26 gives us a small buffer (25th and 26th oldest).
        RETENTION_COUNT = 26
        
        logger.info(f"Running cleanup (keeping newest {RETENTION_COUNT} files)...")
        self._cleanup_directory(raw_output_dir, "*.nc", RETENTION_COUNT)
        self._cleanup_directory(geotiff_output_dir, "*.tif", RETENTION_COUNT)

    def _cleanup_directory(self, directory: Path, pattern: str, keep_count: int):
        """
        Delete old files in a directory, keeping only the newest `keep_count`.
        Files are sorted by name (which includes timestamp for GOES files).
        """
        if not directory.exists():
            return

        files = sorted(directory.glob(pattern))
        
        if len(files) <= keep_count:
            return

        # Identify files to delete (all except the last `keep_count`)
        files_to_delete = files[:-keep_count]
        
        for file_path in files_to_delete:
            try:
                file_path.unlink()
                logger.info(f"Cleanup: deleted old file {file_path.name}")
            except Exception as e:
                logger.warning(f"Cleanup: failed to delete {file_path}: {e}")

    async def _download_last_hour_files(
        self, 
        current_time: datetime, 
        local_cache_dir: Path,
        skip_if: Callable[[str], bool] = None
    ) -> dict[str, bytes]:
        """
        Download the last 24 images (4 hours of data, 1 image every 10 minutes).
        Example for 13:23 UTC:
          - Folder 13h: 10, 00
          - Folder 12h: 50, 40, 30, 20, 10, 00
          - Folder 11h: 50, 40, 30, 20, 10, 00
          - Folder 10h: 50, 40, 30, 20, 10, 00
          - Folder 9h: 50, 40, 30, 20
        """
        TARGET_FILES = 24
        all_files = {}
        hours_back = 0
        
        while len(all_files) < TARGET_FILES:
            # Calculate the hour to search
            search_time = current_time - timedelta(hours=hours_back)
            search_path = self._build_directory_path(search_time)
            
            files_still_needed = TARGET_FILES - len(all_files)
            
            if hours_back == 0:
                # Current hour: download all available
                logger.info(f"Downloading from current hour: {search_path} (time: {search_time.isoformat()})")
                hour_files = await self._s3_client.download_folder(
                    search_path,
                    file_pattern=self._product_base_file_pattern,
                    local_cache_dir=local_cache_dir,
                    skip_if=skip_if,
                )
            else:
                # Previous hours: filter by minutes if necessary
                logger.info(f"Downloading from hour -{hours_back}: {search_path} (time: {search_time.isoformat()})")
                
                # If we need less than 6 files from this hour, filter by minute
                if files_still_needed < 6:
                    min_minute = 60 - (files_still_needed * 10)
                    logger.info(f"Need {files_still_needed} files, filtering minutes >= {min_minute}")
                    
                    def minute_filter(file_path: str) -> bool:
                        file_minute = self._extract_minute_from_filename(file_path)
                        return file_minute is not None and file_minute >= min_minute
                    
                    hour_files = await self._s3_client.download_folder(
                        search_path,
                        file_pattern=self._product_base_file_pattern,
                        file_filter=minute_filter,
                        local_cache_dir=local_cache_dir,
                        skip_if=skip_if,
                    )
                else:
                    # We need all files from this hour
                    hour_files = await self._s3_client.download_folder(
                        search_path,
                        file_pattern=self._product_base_file_pattern,
                        local_cache_dir=local_cache_dir,
                        skip_if=skip_if,
                    )
            
            all_files.update(hour_files)
            logger.info(f"Downloaded {len(hour_files)} files from hour -{hours_back}. Total so far: {len(all_files)}")
            
            hours_back += 1
            
            # Safety limit to avoid infinite loops (maximum 5 hours back)
            if hours_back > 5:
                logger.warning(f"Reached maximum hours back limit. Downloaded {len(all_files)}/{TARGET_FILES} files.")
                break
        
        logger.info(f"Total files downloaded: {len(all_files)}")
        return all_files

    def _build_directory_path(self, time: datetime) -> str:
        """Build the directory path for a specific hour."""
        return f"{self._l1b_products_path}/{time.strftime('%Y/%j/%H')}"

    def _extract_minute_from_filename(self, file_path: str) -> int | None:
        """
        Extract the minute from the GOES filename.
        Typical format: OR_ABI-L1b-RadF-M6C13_G19_sYYYYJJJHHMMSSS...
        The start timestamp is after '_s'
        Format: YYYY (4) + JJJ (3) + HH (2) + MM (2) + SSS (3) = 14 characters
        """
        try:
            filename = file_path.split("/")[-1]
            # Search for the pattern _sYYYYJJJHHMMSSS
            start_idx = filename.find("_s")
            if start_idx == -1:
                return None
            # The format is _sYYYYJJJHHMMSSS
            # Positions: 0-3=year, 4-6=day, 7-8=hour, 9-10=minute, 11-13=second
            timestamp_str = filename[start_idx + 2:start_idx + 2 + 14]
            minute = int(timestamp_str[9:11])
            return minute
        except (ValueError, IndexError):
            logger.warning(f"Could not extract minute from filename: {file_path}")
            return None