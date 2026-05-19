"""GLM FED / TOE / MFA processor — consumes pre-gridded CG_GLM-L2-GLMF windows."""

import asyncio
import gc
import json
import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import xarray as xr

from config import Config
from factories import create_s3_client
from models.band_config import BandConfig, get_band_config
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor, ShutdownRequested
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from services.glm_aggregation import aggregate_glm_window, reproject_to_latlon
from services.processing_steps import (
    build_rgba_data_array,
    fill_missing_tiles,
    normalize_and_colorize,
    run_gdal2tiles,
    save_as_cog,
)

logger = logging.getLogger(__name__)


class GlmFedProcessor(ImageProcessor):
    """Processor for GLM FED, TOE and MFA from pre-gridded 1-minute CG_GLM-L2-GLMF files.

    Pipeline:
      1. List the N 1-minute CG_GLM-L2-GLMF files copied by the data source.
      2. Aggregate them into a single N-minute window via :func:`aggregate_glm_window`
         (FED/TOE summed, MFA min'd, GOES GEOS projection preserved).
      3. For each enabled product (FED always, TOE/MFA via feature flags):
           a. Reproject to EPSG:4326 clipped to ``work_unit.bounds``.
           b. Save raw float32 COG.
           c. Apply ``log10`` pre-transform and colorize with the LogNorm-ready
              palette (``log10(vmin)``/``log10(vmax)`` go to the linear normaliser).
           d. Write GeoTIFF → gdal2tiles → fill transparent gaps → upload.
    """

    GDAL_PROCESSES = 2
    ZOOM_LEVELS = "3-7"

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config)

    def _setup_work_dirs(self, work_unit: WorkUnit) -> dict[str, Path]:
        """Per-product subdirs to keep FED/TOE/MFA outputs from colliding."""
        band_dir = self._get_band_dir(work_unit)
        work_dir = self._ensure_dir(band_dir / work_unit.image_id)
        return {
            "work_dir": work_dir,
            "fed_geotiff": self._ensure_dir(work_dir / "fed" / "geotiff"),
            "fed_tiles": self._ensure_dir(work_dir / "fed" / "tiles"),
            "toe_geotiff": self._ensure_dir(work_dir / "toe" / "geotiff"),
            "toe_tiles": self._ensure_dir(work_dir / "toe" / "tiles"),
            "mfa_geotiff": self._ensure_dir(work_dir / "mfa" / "geotiff"),
            "mfa_tiles": self._ensure_dir(work_dir / "mfa" / "tiles"),
        }

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        logger.info("[GLM] Starting processing for %s", work_unit.image_id)

        data_dir = Path(downloaded_file_path)
        if not data_dir.exists() or not data_dir.is_dir():
            raise FileNotFoundError(f"GLM data directory not found: {data_dir}")

        window_start, window_end = self._window_range(work_unit)
        glm_files = sorted(data_dir.glob("CG_GLM-L2-GLMF-*.nc"))
        if not glm_files:
            raise FileNotFoundError(f"No CG_GLM-L2-GLMF files found in {data_dir}")
        logger.info(
            "Aggregating %d 1-minute files in window %s..%s",
            len(glm_files),
            window_start.isoformat(),
            window_end.isoformat(),
        )

        dirs = self._setup_work_dirs(work_unit)
        try:
            self._check_shutdown()
            aggregated = await asyncio.to_thread(
                aggregate_glm_window,
                glm_files,
                window_start,
                window_end,
                self.config.GLM_ACCUM_MINUTES,
            )
            try:
                await self._process_variable(
                    aggregated,
                    var_name="flash_extent_density",
                    band_config=get_band_config("glm_folder_fed"),
                    geotiff_dir=dirs["fed_geotiff"],
                    tiles_dir=dirs["fed_tiles"],
                    work_unit=work_unit,
                )

                if self.config.ENABLE_GLM_TOE:
                    await self._process_variable(
                        aggregated,
                        var_name="total_energy",
                        band_config=get_band_config("glm_folder_toe"),
                        geotiff_dir=dirs["toe_geotiff"],
                        tiles_dir=dirs["toe_tiles"],
                        work_unit=work_unit,
                    )

                if self.config.ENABLE_GLM_MFA:
                    await self._process_variable(
                        aggregated,
                        var_name="minimum_flash_area",
                        band_config=get_band_config("glm_folder_mfa"),
                        geotiff_dir=dirs["mfa_geotiff"],
                        tiles_dir=dirs["mfa_tiles"],
                        work_unit=work_unit,
                    )
            finally:
                aggregated.close()
                del aggregated
                gc.collect()
        except ShutdownRequested:
            logger.info("Shutdown requested, aborting GLM processing")
            raise
        except Exception as exc:
            logger.error("GLM processing failed for %s: %s", work_unit.image_id, exc)
            raise
        finally:
            self._cleanup_directory(dirs["work_dir"])
            gc.collect()

    def _window_range(self, work_unit: WorkUnit) -> tuple[datetime, datetime]:
        """Extract the aggregation window covered by this work unit."""
        manifest = json.loads(work_unit.source_uri)
        window_start = datetime.fromisoformat(manifest["window_start"])
        window_end = window_start + timedelta(minutes=self.config.GLM_ACCUM_MINUTES)
        return window_start, window_end

    async def _process_variable(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        aggregated: xr.Dataset,
        *,
        var_name: str,
        band_config: BandConfig,
        geotiff_dir: Path,
        tiles_dir: Path,
        work_unit: WorkUnit,
    ) -> None:
        """Reproject one aggregated variable to lat/lon and run it through the tile pipeline."""
        self._check_shutdown()
        logger.info("[GLM] %s: reproject to EPSG:4326", band_config.band_id)
        product_data = await asyncio.to_thread(
            reproject_to_latlon,
            aggregated,
            var_name,
            work_unit.bounds,
            self.config.GLM_RESOLUTION_DEG,
        )
        try:
            await self._generate_and_upload(
                product_data, geotiff_dir, tiles_dir, work_unit, band_config
            )
        finally:
            del product_data
            gc.collect()

    async def _generate_and_upload(  # pylint: disable=too-many-locals,too-many-arguments,too-many-positional-arguments
        self,
        product_data: xr.DataArray,
        geotiff_dir: Path,
        tiles_dir: Path,
        work_unit: WorkUnit,
        band_config: BandConfig,
    ) -> None:
        """Write COG (native units) + GeoTIFF/tiles (log-domain colorization)."""
        color_palette = GenerateGeoTIFFFilesService.get_palette(
            band_config.palette_name
        )

        # 1. COG — raw float32 in native units (FED counts, TOE Joules, MFA km²).
        self._check_shutdown()
        logger.info("[GLM] %s: COG", band_config.band_id)
        cog_path = await asyncio.to_thread(
            save_as_cog, product_data, geotiff_dir, work_unit.image_id
        )
        cog_key = f"{band_config.s3_cog_prefix}/{work_unit.image_id}.tif"

        # 2. GeoTIFF in log-domain so the linear normalize_and_colorize +
        #    linearly-sampled palette reproduce matplotlib.LogNorm output.
        self._check_shutdown()
        logger.info("[GLM] %s: log-domain GeoTIFF", band_config.band_id)
        log_data = _log_clip(product_data, band_config.vmin, band_config.vmax)
        r, g, b, a = normalize_and_colorize(
            log_data,
            vmin=float(np.log10(band_config.vmin)),
            vmax=float(np.log10(band_config.vmax)),
            color_palette=color_palette,
        )
        rgba = build_rgba_data_array(
            r,
            g,
            b,
            a,
            coords_x=log_data.coords["x"],
            coords_y=log_data.coords["y"],
            product_name=band_config.product_name,
        )
        del log_data, r, g, b, a
        gc.collect()

        geotiff_path = geotiff_dir / f"{work_unit.image_id}.tif"
        tmp_geotiff_path = geotiff_dir / f"{uuid.uuid4()}.tif"
        try:
            rgba.rio.to_raster(tmp_geotiff_path, driver="GTiff", compress="LZW")
            tmp_geotiff_path.rename(geotiff_path)
            logger.info("GeoTIFF written: %s", geotiff_path)
        except Exception:
            tmp_geotiff_path.unlink(missing_ok=True)
            raise
        del rgba
        gc.collect()

        # 3. Tiles.
        self._check_shutdown()
        logger.info("[GLM] %s: gdal2tiles", band_config.band_id)
        tiles_output_dir = await asyncio.to_thread(
            run_gdal2tiles,
            geotiff_path,
            tiles_dir,
            zoom_levels=self.ZOOM_LEVELS,
            processes=self.GDAL_PROCESSES,
        )

        self._check_shutdown()
        await asyncio.to_thread(
            fill_missing_tiles,
            tiles_output_dir,
            work_unit.bounds,
            self.ZOOM_LEVELS,
        )

        # 4. Uploads.
        # pylint: disable=duplicate-code
        self._check_shutdown()
        s3_prefix = f"{band_config.s3_tiles_prefix}/{geotiff_path.stem}"
        logger.info("[GLM] %s: upload tiles to %s", band_config.band_id, s3_prefix)
        await self._s3_client.ensure_bucket_exists()
        self._check_shutdown()
        await self._s3_client.upload_directory(tiles_output_dir, s3_prefix)

        self._check_shutdown()
        logger.info("[GLM] %s: upload COG to %s", band_config.band_id, cog_key)
        cog_uploaded = await self._s3_client.upload_file(cog_key, cog_path)
        if not cog_uploaded:
            logger.warning(
                "COG upload failed for %s (key=%s); continuing with tiles only",
                work_unit.image_id,
                cog_key,
            )

        self._cleanup_file(geotiff_path)
        self._cleanup_directory(tiles_output_dir)
        self._cleanup_file(cog_path)
        gc.collect()
        # pylint: enable=duplicate-code

        logger.info("[GLM] %s done: %s", band_config.band_id, s3_prefix)
        self._check_shutdown()


def _log_clip(da: xr.DataArray, vmin: float, vmax: float) -> xr.DataArray:
    """Mask sub-vmin to NaN, clamp above-vmax to vmax, return base-10 log.

    Reproduces matplotlib's ``LogNorm(vmin, vmax, clip=False)`` semantics: values
    strictly below ``vmin`` are treated as missing data so downstream
    colorization renders them transparent (``normalize_and_colorize`` maps NaN
    to ``alpha=0``). Values above ``vmax`` are kept and clamped to ``vmax`` so
    they receive the palette's ceiling color rather than going transparent.
    """
    masked = da.where(da >= vmin)
    clipped = masked.clip(max=vmax)
    return np.log10(clipped)
