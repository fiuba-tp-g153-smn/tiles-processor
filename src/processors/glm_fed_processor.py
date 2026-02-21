"""GLM Flash Extent Density (FED) processor."""

import asyncio
import gc
import logging
import uuid
from pathlib import Path

from config import Config
from factories import create_minio_client
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor, ShutdownRequested
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from services.processing_steps import (
    build_rgba_data_array,
    compute_flash_extent_density,
    normalize_and_colorize,
    run_gdal2tiles,
)

logger = logging.getLogger(__name__)


class GlmFedProcessor(ImageProcessor):
    """
    Processor for GLM Flash Extent Density.

    Pipeline:
    1. Load all GLM-L2-LCFA files in 10-min window (from directory, not single file)
    2. Extract flash lat/lon coordinates
    3. Bin into 0.02° grid (2D histogram)
    4. Colorize with LIGHTNING_PALETTE
    5. Generate GeoTIFF (already in EPSG:4326, no reprojection needed)
    6. Generate tiles with gdal2tiles
    7. Upload to MinIO

    Key Difference from GOES Processors:
    - Input is a DIRECTORY of files, not a single NetCDF file
    - No georeferencing step (FED is already in lat/lon)
    - No brightness temperature (flash counts instead)
    """

    GDAL_PROCESSES = 2
    ZOOM_LEVELS = "3-7"

    def __init__(self, config: Config):
        super().__init__(config)
        self._minio_client = create_minio_client(config)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Execute the GLM FED processing pipeline."""
        logger.info("[GLM-FED] Starting processing for %s", work_unit.image_id)

        # For GLM, downloaded_file_path is a DIRECTORY containing multiple L2-LCFA files
        data_dir = Path(downloaded_file_path)
        if not data_dir.exists() or not data_dir.is_dir():
            raise FileNotFoundError(f"GLM data directory not found: {data_dir}")

        # Setup work directory
        band_dir = self._get_band_dir(work_unit)
        image_stem = work_unit.image_id
        work_dir = self._ensure_dir(band_dir / image_stem)
        geotiff_dir = self._ensure_dir(work_dir / "geotiff")
        tiles_dir = self._ensure_dir(work_dir / "tiles")

        fed_data = None

        try:
            # 1. Compute FED grid from all files
            self._check_shutdown()
            logger.info("Step 1: Computing FED grid")
            glm_files = sorted(data_dir.glob("OR_GLM-L2-LCFA_*.nc"))

            if not glm_files:
                raise FileNotFoundError(f"No GLM-L2-LCFA files found in {data_dir}")

            logger.info("Found %d GLM L2-LCFA files in window", len(glm_files))

            fed_data = await asyncio.to_thread(
                compute_flash_extent_density,
                glm_files,
                work_unit.bounds,
                grid_resolution=0.02,
            )

            # 2. Generate and upload
            await self._generate_and_upload(fed_data, geotiff_dir, tiles_dir, work_unit)

        except ShutdownRequested:
            logger.info("Shutdown requested, aborting GLM FED processing")
            raise
        except Exception as e:
            logger.error("GLM FED processing failed for %s: %s", work_unit.image_id, e)
            raise
        finally:
            self._cleanup_directory(work_dir)
            gc.collect()

    async def _generate_and_upload(  # pylint: disable=too-many-locals
        self, fed_data, geotiff_dir, tiles_dir, work_unit
    ):
        """Generate GeoTIFF, tiles, and upload to S3."""
        band_config = work_unit.band_config
        color_palette = GenerateGeoTIFFFilesService.LIGHTNING_PALETTE

        # 2. GeoTIFF Generation
        self._check_shutdown()
        logger.info("Step 2: GeoTIFF Generation")

        # Normalize and colorize
        r, g, b, a = normalize_and_colorize(
            fed_data,
            vmin=band_config.vmin,
            vmax=band_config.vmax,
            color_palette=color_palette,
        )

        # Build RGBA array
        rgba = build_rgba_data_array(
            r,
            g,
            b,
            a,
            coords_x=fed_data.coords["x"],
            coords_y=fed_data.coords["y"],
            product_name=band_config.product_name,
        )

        # Clean up intermediate arrays
        del fed_data, r, g, b, a
        gc.collect()

        # Write GeoTIFF
        geotiff_path = geotiff_dir / f"{work_unit.image_id}.tif"
        tmp_geotiff_path = geotiff_dir / f"{uuid.uuid4()}.tif"

        try:
            rgba.rio.to_raster(tmp_geotiff_path, driver="GTiff", compress="LZW")
            tmp_geotiff_path.rename(geotiff_path)
            logger.info("GeoTIFF written: %s", geotiff_path)
        except Exception:
            if tmp_geotiff_path.exists():
                tmp_geotiff_path.unlink()
            raise

        del rgba
        gc.collect()

        # 3. Tile Generation
        self._check_shutdown()
        logger.info("Step 3: Tile Generation")
        tiles_output_dir = await asyncio.to_thread(
            run_gdal2tiles,
            geotiff_path,
            tiles_dir,
            zoom_levels=self.ZOOM_LEVELS,
            processes=self.GDAL_PROCESSES,
        )

        # 4. Upload to MinIO
        # pylint: disable=duplicate-code
        self._check_shutdown()
        logger.info("Step 4: Upload to MinIO")
        s3_prefix = f"{band_config.s3_prefix}/{geotiff_path.stem}"

        await self._minio_client.ensure_bucket_exists()
        await self._minio_client.upload_directory(tiles_output_dir, s3_prefix)

        logger.info("Processing complete: %s", s3_prefix)

        # Cleanup intermediate files
        self._cleanup_file(geotiff_path)
        self._cleanup_directory(tiles_output_dir)
        gc.collect()
        # pylint: enable=duplicate-code

        logger.info("[GLM-FED] Processing complete for %s", work_unit.image_id)
