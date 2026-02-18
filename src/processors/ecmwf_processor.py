"""
ECMWF precipitation processor.

This processor handles ECMWF total precipitation forecast data:
Download GRIB -> Extract Period -> Calculate Differential -> GeoTIFF -> Tiles -> Upload
"""

import asyncio
import gc
import json
import logging
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from config import Config
from factories import create_minio_client
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor, ShutdownRequested
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from services.processing_steps import (
    build_rgba_data_array,
    normalize_and_colorize,
    run_gdal2tiles,
)

logger = logging.getLogger(__name__)


class EcmwfProcessor(ImageProcessor):
    """
    Processor for ECMWF total precipitation forecasts.

    Processes 6-hour precipitation periods from ECMWF IFS GRIB files.
    Calculates precipitation differential (accumulation within the period)
    and generates colorized map tiles.
    """

    # gdal2tiles settings
    GDAL_PROCESSES = 2
    ZOOM_LEVELS = "3-7"

    # Reprojection resolution in degrees (None = auto-compute from source)
    REPROJECT_RESOLUTION = None

    def __init__(self, config: Config):
        super().__init__(config)
        self._minio_client = create_minio_client(config)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Execute the full processing pipeline for ECMWF precipitation."""
        logger.info(
            "[%s] Starting processing for %s",
            work_unit.processor_id.upper(),
            work_unit.image_id,
        )

        # Parse source_uri to get time period metadata
        metadata = json.loads(work_unit.source_uri)
        hour_start = metadata["hour_start"]
        hour_end = metadata["hour_end"]

        logger.info(
            "[%s] Processing period h%03d-h%03d",
            work_unit.processor_id.upper(),
            hour_start,
            hour_end,
        )

        # Verify input
        grib_path = Path(downloaded_file_path)
        if not grib_path.exists():
            raise FileNotFoundError(f"GRIB file not found: {grib_path}")

        # Setup per-image work directory
        band_dir = self._get_band_dir(work_unit)
        image_stem = Path(work_unit.image_id).stem
        work_dir = self._ensure_dir(band_dir / image_stem)
        geotiff_dir = self._ensure_dir(work_dir / "geotiff")
        tiles_dir = self._ensure_dir(work_dir / "tiles")

        precip_data = None

        try:
            # Extract and process precipitation data
            precip_data = await self._extract_period_precipitation(
                grib_path, hour_start, hour_end
            )

            await self._generate_and_upload(
                precip_data, geotiff_dir, tiles_dir, work_unit
            )
            precip_data = None

        except ShutdownRequested:
            logger.info(
                "Shutdown requested, aborting processing for %s",
                work_unit.image_id,
            )
            raise
        except Exception as e:
            logger.error("Processing failed for %s: %s", work_unit.image_id, e)
            raise
        finally:
            self._cleanup_directory(work_dir)
            gc.collect()

    async def _extract_period_precipitation(
        self, grib_path: Path, hour_start: int, hour_end: int
    ) -> xr.DataArray:
        """
        Extract precipitation differential for a specific period from GRIB.

        The GRIB contains accumulated precipitation values. This function
        calculates the differential (precip in this period) by subtracting
        the accumulation at the start from the accumulation at the end.

        Args:
            grib_path: Path to ECMWF GRIB file
            hour_start: Start hour of period (e.g., 0, 6, 12, ...)
            hour_end: End hour of period (e.g., 6, 12, 18, ...)

        Returns:
            DataArray with precipitation for this period (mm)
        """
        logger.info(
            "Extracting precipitation period h%03d-h%03d from GRIB",
            hour_start,
            hour_end,
        )

        # Run in thread pool (cfgrib operations are blocking)
        loop = asyncio.get_event_loop()
        precip_data = await loop.run_in_executor(
            None, self._load_and_compute_differential, grib_path, hour_start, hour_end
        )

        return precip_data

    def _load_and_compute_differential(
        self, grib_path: Path, hour_start: int, hour_end: int
    ) -> xr.DataArray:
        """
        Load GRIB and compute precipitation differential (blocking operation).

        Args:
            grib_path: Path to GRIB file
            hour_start: Start forecast hour
            hour_end: End forecast hour

        Returns:
            Precipitation differential DataArray
        """
        # Open GRIB with cfgrib backend
        # Filter for total precipitation parameter
        ds = xr.open_dataset(
            grib_path,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {"shortName": "tp"},
            },
        )

        logger.debug("GRIB dataset variables: %s", list(ds.data_vars))
        logger.debug("GRIB dataset coords: %s", list(ds.coords))
        logger.debug("GRIB dataset dims: %s", ds.dims)
        logger.debug("GRIB step coordinate type: %s", type(ds.coords["step"]))
        logger.debug("GRIB step values: %s", ds.coords["step"].values)

        # Get total precipitation variable
        tp_var = ds["tp"]

        # Convert hours to timedelta (GRIB step coordinate is timedelta)
        step_end = pd.Timedelta(hours=hour_end)

        # Special case: first period (0-6h)
        # ECMWF GRIB files don't contain step=0 (analysis time)
        # The precipitation at step=6h already represents accumulation from 0h to 6h
        if hour_start == 0:
            tp_end = tp_var.sel(step=step_end)
            # Convert from meters to millimeters (*1000)
            precip_diff = tp_end * 1000.0
        else:
            # Normal case: compute differential between two steps
            step_start = pd.Timedelta(hours=hour_start)
            tp_start = tp_var.sel(step=step_start)
            tp_end = tp_var.sel(step=step_end)
            # Calculate differential (precipitation in this period)
            # Convert from meters to millimeters (*1000)
            precip_diff = (tp_end - tp_start) * 1000.0

        # Set metadata
        precip_diff.attrs["long_name"] = "Total Precipitation"
        precip_diff.attrs["units"] = "mm"

        # Ensure CRS is set
        if "latitude" in ds.coords and "longitude" in ds.coords:
            # Regular lat/lon grid
            precip_diff = precip_diff.rename({"latitude": "y", "longitude": "x"})
            precip_diff.rio.write_crs("EPSG:4326", inplace=True)
        else:
            logger.warning("GRIB does not have standard lat/lon coordinates")
            # Try to use rioxarray's automatic CRS detection
            precip_diff.rio.write_crs("EPSG:4326", inplace=True)

        ds.close()
        return precip_diff

    async def _generate_and_upload(
        self, precip_data, geotiff_dir, tiles_dir, work_unit
    ):
        """Generate GeoTIFF, tiles, and upload to S3."""
        # Get product config from work_unit
        # Note: ECMWF uses ecmwf_config, not band_config
        from models.ecmwf_config import ECMWF_CONFIGS

        product_id = work_unit.processor_id.replace("ecmwf_", "")
        ecmwf_config = ECMWF_CONFIGS.get(product_id)

        if ecmwf_config is None:
            raise ValueError(f"Unknown ECMWF product: {product_id}")

        # Determine palette
        if ecmwf_config.palette_name == "PRECIPITATION_PALETTE":
            color_palette = GenerateGeoTIFFFilesService.PRECIPITATION_PALETTE
        else:
            # Fallback to default palette
            color_palette = GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE

        # 1. GeoTIFF Generation
        self._check_shutdown()
        logger.info("Step 1: GeoTIFF Generation")
        geotiff_path = await asyncio.to_thread(
            self._generate_geotiff,
            precip_data,
            geotiff_dir,
            work_unit.image_id,
            work_unit.bounds,
            ecmwf_config.vmin,
            ecmwf_config.vmax,
            ecmwf_config.product_name,
            color_palette,
        )

        del precip_data
        gc.collect()

        # 2. Tile Generation
        self._check_shutdown()
        logger.info("Step 2: Tile Generation")
        tiles_output_dir = await asyncio.to_thread(
            self._generate_tiles, geotiff_path, tiles_dir
        )

        # 3. Upload to S3
        self._check_shutdown()
        logger.info("Step 3: Upload to S3")
        tileset_name = f"{geotiff_path.stem}_tiles"
        s3_prefix = f"{ecmwf_config.s3_prefix}/{tileset_name}"

        await self._minio_client.ensure_bucket_exists()
        await self._minio_client.upload_directory(tiles_output_dir, s3_prefix)

        logger.info("Processing complete: %s", s3_prefix)

        # Cleanup intermediate files
        self._cleanup_file(geotiff_path)
        self._cleanup_directory(tiles_output_dir)
        gc.collect()

    def _generate_geotiff(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        precip_data: xr.DataArray,
        output_dir: Path,
        image_id: str,
        bounds: dict,
        vmin: float,
        vmax: float,
        product_name: str,
        color_palette: list,
    ) -> Path:
        """Generate a colorized RGBA GeoTIFF."""
        logger.info("Generating GeoTIFF for %s", image_id)
        logger.debug("Bounds: %s", bounds)
        logger.debug("Input data shape: %s", precip_data.shape)

        # Clean attributes
        if "grid_mapping" in precip_data.attrs:
            del precip_data.attrs["grid_mapping"]

        # Reproject (ECMWF data should already be in EPSG:4326, but ensure consistency)
        logger.debug("Ensuring EPSG:4326 projection...")
        precip_reproj = precip_data.rio.reproject(
            "EPSG:4326", resolution=self.REPROJECT_RESOLUTION
        )
        precip_reproj.rio.write_nodata(np.nan, inplace=True)
        logger.debug("Reprojected shape: %s", precip_reproj.shape)

        # Clip to bounds
        logger.debug(
            "Clipping to bounds: minx=%s, miny=%s, maxx=%s, maxy=%s",
            bounds["minx"],
            bounds["miny"],
            bounds["maxx"],
            bounds["maxy"],
        )
        precip_clipped = precip_reproj.rio.clip_box(
            minx=bounds["minx"],
            miny=bounds["miny"],
            maxx=bounds["maxx"],
            maxy=bounds["maxy"],
        )
        logger.info(
            "Clipped data shape: %s (y=%d, x=%d)",
            precip_clipped.shape,
            precip_clipped.shape[0],
            precip_clipped.shape[1],
        )

        # Warn if clipped data is very small
        if precip_clipped.shape[0] < 100 or precip_clipped.shape[1] < 100:
            logger.warning(
                "Clipped data is small (%s), this may result in missing zoom levels",
                precip_clipped.shape,
            )

        del precip_reproj
        gc.collect()

        # Normalize and Colorize
        coords_x = precip_clipped["x"]
        coords_y = precip_clipped["y"]
        r, g, b, a = normalize_and_colorize(
            precip_clipped, vmin, vmax, color_palette
        )
        del precip_clipped
        gc.collect()

        # Create RGBA
        rgb = build_rgba_data_array(r, g, b, a, coords_x, coords_y, product_name)
        del r, g, b, a
        gc.collect()

        # Save
        stem = Path(image_id).stem
        output_path = output_dir / f"{stem}.tif"
        tmp_path = output_dir / f"{uuid.uuid4()}.tif"

        try:
            rgb.rio.to_raster(tmp_path)
            tmp_path.rename(output_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        del rgb
        gc.collect()
        return output_path

    def _generate_tiles(self, geotiff_path: Path, output_base_dir: Path) -> Path:
        """Generate XYZ tiles using gdal2tiles."""
        tiles_dir = run_gdal2tiles(
            geotiff_path,
            output_base_dir,
            zoom_levels=self.ZOOM_LEVELS,
            processes=self.GDAL_PROCESSES,
        )
        self._validate_tiles(tiles_dir)
        return tiles_dir

    def _validate_tiles(self, tiles_dir: Path) -> None:
        """Validate that the expected zoom levels were generated."""
        # Parse zoom range from ZOOM_LEVELS (e.g., "3-7")
        zoom_parts = self.ZOOM_LEVELS.split("-")
        min_zoom = int(zoom_parts[0])
        max_zoom = int(zoom_parts[1]) if len(zoom_parts) > 1 else min_zoom

        missing_zooms = []
        for zoom in range(min_zoom, max_zoom + 1):
            zoom_dir = tiles_dir / str(zoom)
            if not zoom_dir.exists():
                missing_zooms.append(zoom)
            else:
                # Count tiles at this zoom level
                tile_count = sum(1 for _ in zoom_dir.rglob("*.webp"))
                if tile_count == 0:
                    missing_zooms.append(zoom)
                else:
                    logger.debug("Zoom %d: %d tiles generated", zoom, tile_count)

        if missing_zooms:
            logger.warning(
                "Missing or empty zoom levels: %s. Expected range: %d-%d",
                missing_zooms,
                min_zoom,
                max_zoom,
            )
            # List what was actually generated
            generated_zooms = [
                d.name for d in tiles_dir.iterdir() if d.is_dir() and d.name.isdigit()
            ]
            logger.warning(
                "Actually generated zoom directories: %s",
                sorted(generated_zooms, key=int),
            )
