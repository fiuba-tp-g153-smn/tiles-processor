"""
Shared GOES processor logic.

This class implements the full pipeline for GOES satellite imagery:
Download -> Georeference -> Brightness Temp -> GeoTIFF -> Tiles -> Upload
"""

import asyncio
import gc
import logging
import uuid
from pathlib import Path

import numpy as np
import xarray as xr

from config import Config
from factories import create_s3_client
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor, ShutdownRequested
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from services.processing_steps import (
    apply_goes_georeferencing,
    build_rgba_data_array,
    compute_brightness_temperature,
    normalize_and_colorize,
    run_gdal2tiles,
)

logger = logging.getLogger(__name__)


class GoesProcessor(ImageProcessor):
    """
    Processor for GOES satellite imagery (Band 13, Band 9, etc.).

    Implements the Strategy pattern for the full processing pipeline.
    """

    # gdal2tiles settings
    GDAL_PROCESSES = 2
    ZOOM_LEVELS = "3-7"

    # Reprojection resolution in degrees (None = auto-compute from source)
    REPROJECT_RESOLUTION = None

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Execute the full processing pipeline."""
        logger.info(
            "[%s] Starting processing for %s",
            work_unit.processor_id.upper(),
            work_unit.image_id,
        )

        # Verify input
        netcdf_path = Path(downloaded_file_path)
        if not netcdf_path.exists():
            raise FileNotFoundError(f"NetCDF file not found: {netcdf_path}")

        # Setup per-image work directory to isolate concurrent workers
        band_dir = self._get_band_dir(work_unit)
        image_stem = work_unit.image_id
        work_dir = self._ensure_dir(band_dir / image_stem)
        geotiff_dir = self._ensure_dir(work_dir / "geotiff")
        tiles_dir = self._ensure_dir(work_dir / "tiles")

        # variables to hold data in memory
        dataset = None
        bt_data = None

        try:
            dataset, bt_data = await self._run_science_pipeline(
                netcdf_path, dataset, bt_data
            )
            await self._generate_and_upload(bt_data, geotiff_dir, tiles_dir, work_unit)
            bt_data = None
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

    async def _run_science_pipeline(self, netcdf_path, dataset, bt_data):
        """Run georeferencing and brightness temperature computation."""
        # 1. Georeference
        # NOTE: Uses self._apply_georeferencing (not the module function)
        # because Band2Processor overrides it with memory-optimized loading.
        self._check_shutdown()
        logger.info("Step 1: Georeferencing")
        dataset = await asyncio.to_thread(self._apply_georeferencing, netcdf_path)

        # 2. Brightness Temperature
        # NOTE: Uses self._compute_brightness_temperature (not the module function)
        # because Band2Processor overrides it with reflectance computation.
        self._check_shutdown()
        logger.info("Step 2: Brightness Temperature")
        bt_data = await asyncio.to_thread(self._compute_brightness_temperature, dataset)

        del dataset
        gc.collect()
        return None, bt_data

    async def _generate_and_upload(self, bt_data, geotiff_dir, tiles_dir, work_unit):
        """Generate GeoTIFF, tiles, and upload to S3."""
        band_config = work_unit.band_config

        # Determine palette
        if band_config.palette_name == "WATER_VAPOR_PALETTE":
            color_palette = GenerateGeoTIFFFilesService.WATER_VAPOR_PALETTE
        elif band_config.palette_name == "VISIBLE_PALETTE":
            color_palette = GenerateGeoTIFFFilesService.VISIBLE_PALETTE
        else:
            color_palette = GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE

        # 3. GeoTIFF Generation
        self._check_shutdown()
        logger.info("Step 3: GeoTIFF Generation")
        geotiff_path = await asyncio.to_thread(
            self._generate_geotiff,
            bt_data,
            geotiff_dir,
            work_unit.image_id,
            work_unit.bounds,
            band_config.vmin,
            band_config.vmax,
            band_config.product_name,
            color_palette,
        )

        del bt_data
        gc.collect()

        # 4. Tile Generation
        self._check_shutdown()
        logger.info("Step 4: Tile Generation")
        tiles_output_dir = await asyncio.to_thread(
            self._generate_tiles, geotiff_path, tiles_dir
        )

        # 5. Upload to S3
        # pylint: disable=duplicate-code
        self._check_shutdown()
        logger.info("Step 5: Upload to S3")
        s3_prefix = f"{band_config.s3_prefix}/{geotiff_path.stem}"

        await self._s3_client.ensure_bucket_exists()
        await self._s3_client.upload_directory(tiles_output_dir, s3_prefix)

        logger.info("Processing complete: %s", s3_prefix)

        # Cleanup intermediate files
        self._cleanup_file(geotiff_path)
        self._cleanup_directory(tiles_output_dir)
        gc.collect()
        # pylint: enable=duplicate-code

        return geotiff_path

    def _apply_georeferencing(self, netcdf_path: Path) -> xr.Dataset:
        """Apply GOES satellite projection transformation.

        Subclasses (e.g. Band2Processor) override this for custom loading.
        """
        return apply_goes_georeferencing(netcdf_path)

    def _compute_brightness_temperature(self, dataset: xr.Dataset) -> xr.DataArray:
        """Convert radiance to brightness temperature using Planck function.

        Subclasses (e.g. Band2Processor) override this for reflectance.
        """
        return compute_brightness_temperature(dataset)

    def _generate_geotiff(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        bt_data: xr.DataArray,
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
        logger.debug("Input data shape: %s", bt_data.shape)

        # Clean attributes
        if "grid_mapping" in bt_data.attrs:
            del bt_data.attrs["grid_mapping"]

        # Reproject
        logger.debug("Reprojecting to EPSG:4326...")
        bt_reproj = bt_data.rio.reproject(
            "EPSG:4326", resolution=self.REPROJECT_RESOLUTION
        )
        bt_reproj.rio.write_nodata(np.nan, inplace=True)
        logger.debug("Reprojected shape: %s", bt_reproj.shape)

        # Clip to bounds
        logger.debug(
            "Clipping to bounds: minx=%s, miny=%s, maxx=%s, maxy=%s",
            bounds["minx"],
            bounds["miny"],
            bounds["maxx"],
            bounds["maxy"],
        )
        bt_clipped = bt_reproj.rio.clip_box(
            minx=bounds["minx"],
            miny=bounds["miny"],
            maxx=bounds["maxx"],
            maxy=bounds["maxy"],
        )
        logger.info(
            "Clipped data shape: %s (y=%d, x=%d)",
            bt_clipped.shape,
            bt_clipped.shape[0],
            bt_clipped.shape[1],
        )

        # Warn if clipped data is very small
        if bt_clipped.shape[0] < 100 or bt_clipped.shape[1] < 100:
            logger.warning(
                "Clipped data is small (%s), this may result in missing zoom levels",
                bt_clipped.shape,
            )

        del bt_reproj
        gc.collect()

        # Normalize and Colorize
        coords_x = bt_clipped["x"]
        coords_y = bt_clipped["y"]
        r, g, b, a = normalize_and_colorize(bt_clipped, vmin, vmax, color_palette)
        del bt_clipped
        gc.collect()

        # Create RGBA
        rgb = build_rgba_data_array(r, g, b, a, coords_x, coords_y, product_name)
        del r, g, b, a
        gc.collect()

        # Save
        output_path = output_dir / f"{image_id}.tif"
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
