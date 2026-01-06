from datetime import datetime, UTC
import logging
from pathlib import Path

from constants import constants
from clients import s3_client
from services.compute_brightness_temperatures import (
    ComputeBrightnessTemperaturesService,
)
from services.setup_goes_georreferencing import SetupGOESGeorreferencingService

logger = logging.getLogger(__name__)


class ProcessBand13Job:
    def __init__(self):
        self._bucket_name = constants.GOES19_BUCKET_NAME
        self._l1b_products_path = "ABI-L1b-RadF"
        self._product_base_file_pattern = "C13_G19"
        self._s3_client = s3_client.S3Client(
            self._bucket_name, max_concurrent_downloads=5
        )

    async def run(self):
        output_dir = Path.cwd() / ".tmp" / "band_13" / "nc_files"
        output_dir.mkdir(parents=True, exist_ok=True)
        current_time = datetime.now(UTC)

        downloaded_files = await self._s3_client.download_folder(
            self._last_timestamp_directory(current_time),
            file_pattern=self._product_base_file_pattern,
        )
        georreferenced_data = await SetupGOESGeorreferencingService(
            downloaded_files
        ).run()
        logger.info("Georreferencing completed.")
        brightness_temperature_data = await ComputeBrightnessTemperaturesService(
            georreferenced_data
        ).run()
        logger.info("Brightness temperature computation completed.")

        for file_path, content in downloaded_files.items():
            dest_path = output_dir / Path(file_path).name
            with open(dest_path, "wb") as dest_file:
                dest_file.write(content)

    def _last_timestamp_directory(self, current_time):
        return f"{self._l1b_products_path}/{current_time.strftime("%Y/%j/%H")}"
