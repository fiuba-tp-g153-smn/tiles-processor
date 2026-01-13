from datetime import datetime, UTC, timedelta
from pathlib import Path
import logging

from constants import constants
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

        geotiff_output_dir = Path.cwd() / ".tmp" / "band_9" / "geotiff"
        geotiff_files = await GenerateGeoTIFFFilesService(
            brightness_temperature_data,
            geotiff_output_dir,
            color_palette=GenerateGeoTIFFFilesService.WATER_VAPOR_PALETTE,
            vmin=183.15,  # -90°C en Kelvin
            vmax=323.15,  # +50°C en Kelvin
            product_name="Water_Vapor",
        ).run()
        logger.info("GeoTIFF generation completed.")

        tiles_output_dir = Path.cwd() / ".tmp" / "band_9" / "tiles"
        await GenerateTilesService(geotiff_files, tiles_output_dir).run()
        logger.info("Tiles generation completed.")

    async def _download_last_hour_files(self, current_time: datetime) -> dict[str, bytes]:
        """
        Descarga las últimas 24 imágenes (4 horas de datos, 1 imagen cada 10 minutos).
        Ejemplo para 13:23 UTC:
          - Carpeta 13h: 10, 00
          - Carpeta 12h: 50, 40, 30, 20, 10, 00
          - Carpeta 11h: 50, 40, 30, 20, 10, 00
          - Carpeta 10h: 50, 40, 30, 20, 10, 00
          - Carpeta 9h: 50, 40, 30, 20
        """
        TARGET_FILES = 24
        all_files = {}
        hours_back = 0
        
        while len(all_files) < TARGET_FILES:
            # Calcular la hora a buscar
            search_time = current_time - timedelta(hours=hours_back)
            search_path = self._build_directory_path(search_time)
            
            files_still_needed = TARGET_FILES - len(all_files)
            
            if hours_back == 0:
                # Hora actual: descargar todos los disponibles
                logger.info(f"Downloading from current hour: {search_path} (time: {search_time.isoformat()})")
                hour_files = await self._s3_client.download_folder(
                    search_path,
                    file_pattern=self._product_base_file_pattern,
                )
            else:
                # Horas anteriores: filtrar por minutos si es necesario
                logger.info(f"Downloading from hour -{hours_back}: {search_path} (time: {search_time.isoformat()})")
                
                # Si necesitamos menos de 6 archivos de esta hora, filtrar por minuto
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
                    # Necesitamos todos los archivos de esta hora
                    hour_files = await self._s3_client.download_folder(
                        search_path,
                        file_pattern=self._product_base_file_pattern,
                    )
            
            all_files.update(hour_files)
            logger.info(f"Downloaded {len(hour_files)} files from hour -{hours_back}. Total so far: {len(all_files)}")
            
            hours_back += 1
            
            # Límite de seguridad para evitar loops infinitos (máximo 5 horas atrás)
            if hours_back > 5:
                logger.warning(f"Reached maximum hours back limit. Downloaded {len(all_files)}/{TARGET_FILES} files.")
                break
        
        logger.info(f"Total files downloaded: {len(all_files)}")
        return all_files

    def _build_directory_path(self, time: datetime) -> str:
        """Construye la ruta del directorio para una hora específica."""
        return f"{self._l1b_products_path}/{time.strftime('%Y/%j/%H')}"

    def _extract_minute_from_filename(self, file_path: str) -> int | None:
        """
        Extrae el minuto del nombre del archivo GOES.
        Formato típico: OR_ABI-L1b-RadF-M6C09_G19_sYYYYJJJHHMMSSS...
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
