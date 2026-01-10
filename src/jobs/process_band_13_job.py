import logging
from datetime import datetime, UTC, timedelta
from pathlib import Path

from constants import constants
from clients import s3_client
from services.compute_brightness_temperatures import (
    ComputeBrightnessTemperaturesService,
)
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from services.generate_tiles import GenerateTilesService
from services.setup_goes_georreferencing import SetupGOESGeorreferencingService

logger = logging.getLogger(__name__)


class ProcessBand13Job:
    def __init__(self):
        self._bucket_name = constants.GOES19_BUCKET_NAME
        self._l1b_products_path = "ABI-L1b-RadF"
        self._product_base_file_pattern = "C13_G19"
        self._s3_client = s3_client.S3Client(
            self._bucket_name, max_concurrent_downloads=6
        )

    async def run(self):
        current_time = datetime.now(UTC)

        # Descargar archivos de la última hora (6 archivos)
        downloaded_files = await self._download_last_hour_files(current_time)
        
        georreferenced_data = await SetupGOESGeorreferencingService(
            downloaded_files
        ).run()
        logger.info("Georreferencing completed.")
        brightness_temperature_data = await ComputeBrightnessTemperaturesService(
            georreferenced_data
        ).run()
        logger.info("Brightness temperature computation completed.")

        geotiff_output_dir = Path.cwd() / ".tmp" / "band_13" / "geotiff"
        geotiff_files = await GenerateGeoTIFFFilesService(
            brightness_temperature_data, geotiff_output_dir
        ).run()
        logger.info("GeoTIFF generation completed.")

        tiles_output_dir = Path.cwd() / ".tmp" / "band_13" / "tiles"
        await GenerateTilesService(geotiff_files, tiles_output_dir).run()
        logger.info("Tiles generation completed.")

    async def _download_last_hour_files(self, current_time: datetime) -> dict[str, bytes]:
        """
        Descarga los últimos 6 archivos (última hora).
        Ejemplo para 14:25 UTC:
          - Carpeta 14h: 10, 00
          - Carpeta 13h: 50, 40, 30, 20
        """
        all_files = {}
        
        # 1. Descargar todos los disponibles de la hora actual
        current_hour_path = self._build_directory_path(current_time)
        logger.info(f"Downloading from current hour: {current_hour_path} (time: {current_time.isoformat()})")
        
        current_hour_files = await self._s3_client.download_folder(
            current_hour_path,
            file_pattern=self._product_base_file_pattern,
        )
        all_files.update(current_hour_files)
        files_from_current_hour = len(current_hour_files)
        logger.info(f"Downloaded {files_from_current_hour} files from current hour folder")
        
        # 2. Calcular cuántos archivos faltan de la hora anterior
        files_needed_from_previous = 6 - files_from_current_hour
        
        if files_needed_from_previous > 0:
            # Hora anterior (puede ser día anterior si current_time.hour == 0)
            previous_hour_time = current_time - timedelta(hours=1)
            previous_hour_path = self._build_directory_path(previous_hour_time)
            
            logger.info(f"Downloading from previous hour: {previous_hour_path} (time: {previous_hour_time.isoformat()})")
            
            # Calcular el minuto mínimo a descargar de la hora anterior
            min_minute_previous = 60 - (files_needed_from_previous * 10)
            
            logger.info(f"Need {files_needed_from_previous} files from previous hour, filtering minutes >= {min_minute_previous}")
            
            # Crear filtro para descargar solo archivos con minuto >= min_minute_previous
            def minute_filter(file_path: str) -> bool:
                file_minute = self._extract_minute_from_filename(file_path)
                passes = file_minute is not None and file_minute >= min_minute_previous
                logger.info(f"Filter check: {file_path} -> minute: {file_minute}, passes: {passes}")
                return passes
            
            previous_hour_files = await self._s3_client.download_folder(
                previous_hour_path,
                file_pattern=self._product_base_file_pattern,
                file_filter=minute_filter,
            )
            all_files.update(previous_hour_files)
            
            logger.info(f"Downloaded {len(previous_hour_files)} files from previous hour folder (minutes >= {min_minute_previous})")
        
        logger.info(f"Total files downloaded: {len(all_files)}")
        return all_files

    def _build_directory_path(self, time: datetime) -> str:
        """Construye la ruta del directorio para una hora específica."""
        return f"{self._l1b_products_path}/{time.strftime('%Y/%j/%H')}"

    def _extract_minute_from_filename(self, file_path: str) -> int | None:
        """
        Extrae el minuto del nombre del archivo GOES.
        Formato típico: OR_ABI-L1b-RadF-M6C13_G19_sYYYYJJJHHMMSSS...
        El timestamp de inicio está después de '_s'
        Formato: YYYY (4) + JJJ (3) + HH (2) + MM (2) + SSS (3) = 14 caracteres
        """
        try:
            filename = file_path.split("/")[-1]
            # Buscar el patrón _sYYYYJJJHHMMSSS
            start_idx = filename.find("_s")
            if start_idx == -1:
                return None
            # El formato es _sYYYYJJJHHMMSSS
            # Posiciones: 0-3=año, 4-6=día, 7-8=hora, 9-10=minuto, 11-13=segundo
            timestamp_str = filename[start_idx + 2:start_idx + 2 + 14]
            minute = int(timestamp_str[9:11])
            return minute
        except (ValueError, IndexError):
            logger.warning(f"Could not extract minute from filename: {file_path}")
            return None