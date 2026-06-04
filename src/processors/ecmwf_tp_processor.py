"""ECMWF total precipitation processor: full subprocess pipeline for a single 6h accumulation window ending at hour_end."""

import asyncio
import gc
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
import pandas as pd

import xarray as xr

from config import Config
from factories import create_s3_client
from models.ecmwf_config import ECMWF_TP_CONFIG, WINDOW_HOURS
from models.ecmwf_tp_palettes import PRECIPITATION_COLORS, PRECIPITATION_THRESHOLDS
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor
from services.processing_steps import (
    build_rgba_data_array,
    fill_missing_tiles,
    prewarp_to_mercator_grid,
    run_gdal2tiles,
    save_as_cog,
    threshold_colorize,
)

logger = logging.getLogger(__name__)

_MAX_ZOOM = 7
_ZOOM_LEVELS = f"3-{_MAX_ZOOM}"
_GDAL_PROCESSES = 2


class EcmwfTotalPrecipitationProcessor(ImageProcessor):
    """
    Subprocess processor for a single ECMWF 6h precipitation window ending at hour_end.

    Reads the cached GRIB, computes the precipitation differential for the
    6h window that ends at hour_end (accumulating the previous 6 hours), generates
    a COG (raw mm values) and colorized tiles, then uploads both to S3/SeaweedFS.
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config, with_ttl=config.SEAWEEDFS_ECMWF_TTL)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Execute the full processing pipeline for a 6h window ending at hour_end."""
        meta = json.loads(work_unit.source_uri)
        forecast_time = datetime.fromisoformat(meta["forecast_time"])
        hour_end: int = meta["hour_end"]
        hour_start = hour_end - WINDOW_HOURS
        forecast_ts = _fmt_ts(forecast_time)

        logger.info(
            "[ECMWF-TP] Processing window %s (hours %d-%d)",
            work_unit.image_id,
            hour_start,
            hour_end,
        )

        grib_path = Path(downloaded_file_path)
        if not grib_path.exists():
            raise FileNotFoundError(f"GRIB file not found: {grib_path}")

        work_dir = self._ensure_dir(self._get_band_dir(work_unit) / work_unit.image_id)
        geotiff_dir = self._ensure_dir(work_dir / "geotiff")
        tiles_dir = self._ensure_dir(work_dir / "tiles")

        with self._time_stage("load"):
            clipped = await asyncio.to_thread(
                self._load_and_prepare,
                grib_path,
                hour_start,
                hour_end,
                work_unit.bounds,
            )
        self._check_shutdown()

        cog_path, geotiff_path = await asyncio.to_thread(
            self._generate_outputs, clipped, geotiff_dir, work_unit.image_id
        )
        del clipped
        gc.collect()
        self._check_shutdown()

        with self._time_stage("prewarp"):
            prewarped_path = await asyncio.to_thread(
                prewarp_to_mercator_grid, geotiff_path, geotiff_dir, _MAX_ZOOM
            )
        self._check_shutdown()

        with self._time_stage("tiling"):
            tiles_output_dir = await asyncio.to_thread(
                run_gdal2tiles, prewarped_path, tiles_dir, _ZOOM_LEVELS, _GDAL_PROCESSES
            )
            await asyncio.to_thread(
                fill_missing_tiles, tiles_output_dir, work_unit.bounds, _ZOOM_LEVELS
            )
        self._check_shutdown()

        with self._time_stage("upload"):
            await self._upload(cog_path, tiles_output_dir, forecast_ts, work_unit)

        # Cleanup intermediate files (WorkHandler cleans up the parent work_dir)
        self._cleanup_file(geotiff_path)
        self._cleanup_file(prewarped_path)
        self._cleanup_file(cog_path)
        self._cleanup_directory(tiles_output_dir)

    # ------------------------------------------------------------------
    # Internal helpers (run in thread via asyncio.to_thread)
    # ------------------------------------------------------------------

    def _load_and_prepare(
        self, grib_path: Path, hour_start: int, hour_end: int, bounds: dict
    ) -> xr.DataArray:
        """Read GRIB, compute differential, reproject and clip."""
        import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

        logger.info("[ECMWF-TP] Step 1: Reading GRIB %s", grib_path.name)
        ds = xr.open_dataset(
            grib_path,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": ECMWF_TP_CONFIG.parameter}},
        )
        tp_var = ds["tp"]

        logger.info(
            "[ECMWF-TP] Step 2: Computing precipitation differential (hours %d-%d)",
            hour_start,
            hour_end,
        )
        if hour_start == 0:
            # step=0 not present; tp is accumulated from t=0, so tp(hour_end) IS the window total
            precip_diff = tp_var.sel(step=pd.Timedelta(hours=hour_end)) * 1000.0
        else:
            tp_s = tp_var.sel(step=pd.Timedelta(hours=hour_start))
            tp_e = tp_var.sel(step=pd.Timedelta(hours=hour_end))
            precip_diff = (tp_e - tp_s) * 1000.0  # convert from meters to millimeters

        del tp_var, ds
        gc.collect()

        precip_diff.attrs["long_name"] = "Total Precipitation"
        precip_diff.attrs["units"] = "mm"
        precip_diff.attrs.pop("grid_mapping", None)

        # Rename spatial dims for rioxarray compatibility
        rename_map = {}
        if "latitude" in precip_diff.dims:
            rename_map["latitude"] = "y"
        if "longitude" in precip_diff.dims:
            rename_map["longitude"] = "x"
        if rename_map:
            precip_diff = precip_diff.rename(rename_map)

        precip_diff.rio.write_crs("EPSG:4326", inplace=True)
        precip_diff.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        logger.info("[ECMWF-TP] Step 3: Reprojecting and clipping")
        reproj = precip_diff.rio.reproject("EPSG:4326", resolution=None)
        reproj.rio.write_nodata(float("nan"), inplace=True)
        del precip_diff
        gc.collect()

        clipped = reproj.rio.clip_box(
            minx=bounds["minx"],
            miny=bounds["miny"],
            maxx=bounds["maxx"],
            maxy=bounds["maxy"],
        )
        del reproj
        gc.collect()
        return clipped

    def _generate_outputs(
        self, clipped: xr.DataArray, geotiff_dir: Path, image_id: str
    ) -> tuple[Path, Path]:
        """Generate COG and colorized GeoTIFF from the already-reprojected data."""
        logger.info("[ECMWF-TP] Step 4: Generating COG")
        with self._time_stage("cog"):
            cog_path = save_as_cog(clipped, geotiff_dir, image_id)

        logger.info("[ECMWF-TP] Step 5: Generating colorized GeoTIFF")
        with self._time_stage("geotiff"):
            geotiff_path = self._colorize_and_save(clipped, geotiff_dir, image_id)
        return cog_path, geotiff_path

    def _colorize_and_save(
        self, clipped: xr.DataArray, output_dir: Path, image_id: str
    ) -> Path:
        """Normalize, colorize, and write an RGBA GeoTIFF."""
        import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

        coords_x = clipped["x"]
        coords_y = clipped["y"]

        r, g, b, a = threshold_colorize(
            clipped,
            PRECIPITATION_THRESHOLDS,
            PRECIPITATION_COLORS,
        )
        gc.collect()

        rgba = build_rgba_data_array(
            r, g, b, a, coords_x, coords_y, "Total Precipitation"
        )
        del r, g, b, a
        gc.collect()

        output_path = output_dir / f"{image_id}.tif"
        tmp_path = output_dir / f"{uuid.uuid4()}.tif"
        try:
            rgba.rio.to_raster(tmp_path)
            tmp_path.rename(output_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        finally:
            del rgba
            gc.collect()

        return output_path

    async def _upload(
        self,
        cog_path: Path,
        tiles_output_dir: Path,
        forecast_ts: str,
        work_unit: WorkUnit,
    ) -> None:
        """Upload COG and tiles to S3."""
        cog_key = f"{ECMWF_TP_CONFIG.cog_prefix}/{forecast_ts}/{work_unit.image_id}.tif"
        tiles_prefix = (
            f"{ECMWF_TP_CONFIG.tiles_prefix}/{forecast_ts}/{work_unit.image_id}"
        )

        self._check_shutdown()
        logger.info("[ECMWF-TP] Step 6a: Uploading COG → %s", cog_key)
        uploaded = await self._s3_client.upload_file(cog_key, cog_path)
        if not uploaded:
            logger.warning(
                "[ECMWF-TP] COG upload failed for %s; continuing", work_unit.image_id
            )

        self._check_shutdown()
        logger.info("[ECMWF-TP] Step 6b: Uploading tiles → %s", tiles_prefix)
        await self._s3_client.upload_directory(tiles_output_dir, tiles_prefix)

        logger.info("[ECMWF-TP] Upload complete: %s", work_unit.image_id)


def _fmt_ts(dt: datetime) -> str:
    """Format datetime as YYYYMMDDTHHmmZ."""
    return dt.strftime("%Y%m%dT%H%MZ")
