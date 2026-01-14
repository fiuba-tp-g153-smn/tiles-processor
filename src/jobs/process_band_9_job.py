from datetime import datetime, UTC, timedelta
from pathlib import Path
import logging

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


class ProcessBand9Job:
    def __init__(self):
        self._bucket_name = constants.GOES19_BUCKET_NAME
        self._l1b_products_path = "ABI-L1b-RadF"
        self._product_base_file_pattern = "C09_G19"
        self._s3_client = s3_client.S3Client(
            self._bucket_name, max_concurrent_downloads=6
        )

    async def run(self):
        current_time = datetime.now(UTC)

        # Download files from the last hour (6 files)
        downloaded_files = await self._download_last_hour_files(current_time)
        
        georreferenced_data = await SetupGOESGeorreferencingService(
            downloaded_files
        ).run()
        logger.info("Georreferencing completed.")
        brightness_temperature_data = await ComputeBrightnessTemperaturesService(
            georreferenced_data
        ).run()
        logger.info("Brightness temperature computation completed.")

        geotiff_output_dir = Path.cwd() / config.TMP_DIR / "band_9" / "geotiff"
        geotiff_files = await GenerateGeoTIFFFilesService(
            brightness_temperature_data,
            geotiff_output_dir,
            color_palette=GenerateGeoTIFFFilesService.WATER_VAPOR_PALETTE,
            vmin=220.0,  # ~ -53°C - Adjusted range for water vapor
            vmax=260.0,  # ~ -13°C - Concentrates real values across the palette
            product_name="Water_Vapor",
        ).run()
        logger.info("GeoTIFF generation completed.")

        tiles_output_dir = Path.cwd() / config.TMP_DIR / "band_9" / "tiles"
        await GenerateTilesService(geotiff_files, tiles_output_dir).run()
        logger.info("Tiles generation completed.")

    async def _download_last_hour_files(self, current_time: datetime) -> dict[str, bytes]:
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
                )
            else:
                # Previous hours: filter by minutes if necessary
                logger.info(f"Downloading from hour -{hours_back}: {search_path} (time: {search_time.isoformat()})")
                
                # If we need less than 6 files from this hour, filter by minute
                if files_still_needed < 6:
                    min_minute = 60 - (files_still_needed * 10)
                    logger.info(f"Need {files_still_needed} files, filtering minutes >= {min_minute}")
                    
                    def minute_filter(file_path: str) -> bool:
                        minute = self._extract_minute_from_filename(file_path)
                        return minute is not None and minute >= min_minute
                    
                    hour_files = await self._s3_client.download_folder(
                        search_path,
                        file_pattern=self._product_base_file_pattern,
                        file_filter=minute_filter,
                    )
                else:
                    # We need all files from this hour
                    hour_files = await self._s3_client.download_folder(
                        search_path,
                        file_pattern=self._product_base_file_pattern,
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
        Typical format: OR_ABI-L1b-RadF-M6C09_G19_sYYYYJJJHHMMSSS...
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
