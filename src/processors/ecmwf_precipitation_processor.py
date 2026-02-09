"""
ECMWF Total Precipitation processor.

This processor handles ECMWF weather model forecasts:
GRIB → Extract timesteps → Calculate 6h intervals → GeoTIFF → Tiles → Upload
"""

import asyncio
import cfgrib
import gc
import logging
from pathlib import Path
from typing import List, Tuple

import xarray as xr

from config import Config
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor
from clients.s3_client import S3Client
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from services.generate_tiles import GenerateTilesService

logger = logging.getLogger(__name__)


class EcmwfPrecipitationProcessor(ImageProcessor):
    """
    Processor for ECMWF total precipitation forecasts.

    Processes a single model run (00Z or 12Z) with:
    - 144 hours of forecast (6 days)
    - 3-hour timesteps
    - Generates 24 tilesets (one per 6-hour interval)
    """

    # Forecast configuration
    FORECAST_HOURS = 144  # 6 days
    TIMESTEP_HOURS = 3  # Data every 3 hours
    INTERVAL_HOURS = 6  # Group into 6-hour intervals

    def __init__(self, config: Config):
        super().__init__(config)
        self._minio_client = S3Client.create_with_credentials(
            bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
            endpoint=config.S3_TILES_DATA_ENDPOINT,
            access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
            secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
            secure=config.S3_TILES_DATA_SECURE,
        )

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """
        Process ECMWF GRIB file and generate tiles for all 6-hour intervals.

        Pipeline:
        1. Load GRIB file with all timesteps
        2. Calculate 6-hour precipitation intervals (0-6h, 6-12h, ..., 138-144h)
        3. For each interval:
           - Generate GeoTIFF with precipitation data
           - Generate tiles using gdal2tiles
           - Upload tiles to S3
        4. Cleanup

        Args:
            downloaded_file_path: Path to the downloaded GRIB file
            work_unit: Work unit with metadata
        """
        grib_path = Path(downloaded_file_path)
        product_config = work_unit.product_config

        try:
            logger.info(
                f"[{work_unit.product_id}] Starting ECMWF processing for {work_unit.image_id}"
            )

            # Setup directories
            product_dir = self._get_band_dir(work_unit)
            geotiff_dir = self._ensure_dir(product_dir / "geotiff")
            tiles_dir = self._ensure_dir(product_dir / "tiles")

            # Load GRIB file
            logger.info(f"[{work_unit.product_id}] Loading GRIB file")
            dataset = await asyncio.to_thread(self._load_grib_file, grib_path)

            # Calculate 6-hour intervals
            logger.info(f"[{work_unit.product_id}] Calculating 6-hour precipitation intervals")
            intervals = self._calculate_6h_intervals(dataset)

            logger.info(
                f"[{work_unit.product_id}] Generated {len(intervals)} 6-hour intervals"
            )

            # Process each interval
            for interval_data in intervals:
                await self._process_interval(
                    interval_data=interval_data,
                    work_unit=work_unit,
                    product_config=product_config,
                    geotiff_dir=geotiff_dir,
                    tiles_dir=tiles_dir,
                )

            logger.info(
                f"[{work_unit.product_id}] Completed processing all intervals "
                f"for {work_unit.image_id}"
            )

        finally:
            # Cleanup
            self._cleanup_file(grib_path)
            if geotiff_dir.exists():
                self._cleanup_directory(geotiff_dir)
            if tiles_dir.exists():
                self._cleanup_directory(tiles_dir)

            gc.collect()

    async def _process_interval(
        self,
        interval_data: Tuple[str, xr.DataArray],
        work_unit: WorkUnit,
        product_config,
        geotiff_dir: Path,
        tiles_dir: Path,
    ) -> None:
        """
        Process a single 6-hour precipitation interval.

        Args:
            interval_data: Tuple of (interval_name, precipitation_array)
            work_unit: Work unit metadata
            product_config: Product configuration
            geotiff_dir: Directory for GeoTIFFs
            tiles_dir: Directory for tiles
        """
        interval_name, precip_array = interval_data

        logger.info(f"[{work_unit.product_id}] Processing interval {interval_name}")

        # Rename coordinates from latitude/longitude to y/x for GenerateGeoTIFFFilesService
        # ECMWF data comes with 'latitude' and 'longitude', but the service expects 'x' and 'y'
        precip_array = precip_array.rename({"latitude": "y", "longitude": "x"})

        # Generate GeoTIFF using GenerateGeoTIFFFilesService
        geotiff_filename = f"{work_unit.image_id}_{interval_name}.tif"
        geotiff_path = geotiff_dir / geotiff_filename

        # Use GenerateGeoTIFFFilesService with skip_reprojection=True (data already in EPSG:4326)
        palette = getattr(GenerateGeoTIFFFilesService, product_config.palette_name)
        service = GenerateGeoTIFFFilesService(
            brightness_temperatures={geotiff_filename: precip_array},
            output_dir=geotiff_dir,
            config=self.config,
            color_palette=palette,
            vmin=product_config.vmin,
            vmax=product_config.vmax,
            product_name=product_config.product_name,
            max_concurrency=1,  # Only one file to process
            skip_reprojection=True,  # ECMWF data already in EPSG:4326
        )
        await service.run()

        # Generate tiles using GenerateTilesService
        tileset_name = f"{work_unit.image_id}_{interval_name}_tiles"
        tileset_dir = tiles_dir / tileset_name

        tiles_service = GenerateTilesService(
            geotiff_files=[geotiff_path],
            output_dir=tiles_dir,
        )
        await tiles_service.run()

        # Upload to S3
        s3_prefix = f"{product_config.s3_prefix}/{work_unit.image_id}/{interval_name}"
        await self._upload_tiles_to_s3(
            tiles_dir=tileset_dir,
            s3_prefix=s3_prefix,
        )

        # Cleanup interval-specific files
        self._cleanup_file(geotiff_path)
        self._cleanup_directory(tileset_dir)

        logger.info(f"[{work_unit.product_id}] Completed interval {interval_name}")

    def _load_grib_file(self, grib_path: Path) -> xr.Dataset:
        """
        Load GRIB file using cfgrib/xarray.

        Args:
            grib_path: Path to GRIB file

        Returns:
            xarray Dataset with precipitation data
        """

        # Load GRIB file
        # cfgrib automatically handles GRIB1/GRIB2 and creates proper coordinates
        dataset = xr.open_dataset(
            grib_path,
            engine="cfgrib",
            backend_kwargs={"indexpath": ""},  # Don't create .idx files
        )

        logger.info(
            f"Loaded GRIB with variables: {list(dataset.data_vars)}, "
            f"coords: {list(dataset.coords)}"
        )

        return dataset

    def _calculate_6h_intervals(self, dataset: xr.Dataset) -> List[Tuple[str, xr.DataArray]]:
        """
        Calculate 6-hour precipitation intervals from cumulative data.

        ECMWF total precipitation is cumulative from step 0. To get 6-hour
        intervals, we subtract consecutive timesteps:
        - 0-6h: tp(6h) - tp(0h)
        - 6-12h: tp(12h) - tp(6h)
        - etc.

        Args:
            dataset: xarray Dataset with total precipitation

        Returns:
            List of (interval_name, precipitation_array) tuples
        """
        from datetime import timedelta

        # Get total precipitation variable (usually 'tp')
        tp_var = dataset["tp"]

        # Log available steps for debugging
        logger.info(f"Available steps in dataset: {tp_var.step.values}")

        intervals = []

        # Generate intervals: 0-6, 6-12, 12-18, ..., 138-144
        for start_hour in range(0, self.FORECAST_HOURS, self.INTERVAL_HOURS):
            end_hour = start_hour + self.INTERVAL_HOURS

            # Select timesteps using timedelta (cfgrib represents steps as timedelta64)
            try:
                if start_hour == 0:
                    # First interval: just use 6h data (0h is usually 0 or not available)
                    tp_end = tp_var.sel(step=timedelta(hours=end_hour))
                    precip_interval = tp_end
                else:
                    # Subsequent intervals: difference between end and start
                    tp_start = tp_var.sel(step=timedelta(hours=start_hour))
                    tp_end = tp_var.sel(step=timedelta(hours=end_hour))
                    precip_interval = tp_end - tp_start

                # Convert from m to mm (ECMWF uses meters)
                precip_interval = precip_interval * 1000.0

                # Format interval name: 000-006h, 006-012h, etc.
                interval_name = f"{start_hour:03d}-{end_hour:03d}h"

                intervals.append((interval_name, precip_interval))

                logger.debug(
                    f"Interval {interval_name}: "
                    f"min={float(precip_interval.min()):.2f}mm, "
                    f"max={float(precip_interval.max()):.2f}mm"
                )

            except KeyError as e:
                logger.warning(f"Could not find timestep for interval {start_hour}-{end_hour}h: {e}")
                continue

        return intervals

    async def _upload_tiles_to_s3(
        self,
        tiles_dir: Path,
        s3_prefix: str,
    ) -> None:
        """
        Upload generated tiles to S3.

        Args:
            tiles_dir: Directory containing tiles
            s3_prefix: S3 prefix for upload (e.g., "models/ecmwf/total_precipitation/2026-02-05T00Z/000-006h")
        """
        logger.info(f"Uploading tiles to s3://{s3_prefix}")

        # Count tiles
        tile_files = list(tiles_dir.rglob("*.png"))
        total_tiles = len(tile_files)

        logger.info(f"Uploading {total_tiles} tiles to {s3_prefix}")

        # Upload all tiles
        await self._minio_client.upload_directory(
            local_dir=tiles_dir,
            s3_prefix=s3_prefix,
        )

        logger.info(f"Upload complete: {total_tiles} tiles to {s3_prefix}")
