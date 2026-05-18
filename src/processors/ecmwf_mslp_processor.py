"""ECMWF mean sea level pressure processor: COG + isobars GeoJSON per period-end timestamp."""

import asyncio
import gc
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import xarray as xr

from config import Config
from factories import create_s3_client
from models.ecmwf_config import ECMWF_MSLP_CONFIG
from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor
from services.contouring import extract_isolines, smooth_field, write_geojson
from services.processing_steps import save_as_cog

logger = logging.getLogger(__name__)

_PA_TO_HPA = 100.0
_ISOBAR_STEP_HPA = 5.0


class EcmwfMslpProcessor(ImageProcessor):
    """
    Subprocess processor for a single ECMWF mean sea level pressure timestamp.

    Reads the cached GRIB, extracts the `msl` field at the requested period-end
    step, converts Pa → hPa, reprojects/clips, and produces:
      * a Cloud Optimized GeoTIFF with the raw pressure field, and
      * a GeoJSON of isobars (multiples of 5 hPa, simplified geometry).
    Both are uploaded to S3/SeaweedFS.
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config, with_ttl=config.SEAWEEDFS_ECMWF_TTL)
        self._smoothing_sigma = config.ECMWF_MSLP_SMOOTHING_SIGMA
        self._isobar_simplify_tolerance = config.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Execute the full processing pipeline for one period-end MSLP timestamp."""
        meta = json.loads(work_unit.source_uri)
        forecast_time = datetime.fromisoformat(meta["forecast_time"])
        hour_end: int = meta["hour_end"]
        forecast_ts = _fmt_ts(forecast_time)

        logger.info(
            "[ECMWF-MSLP] Processing timestamp %s (T+%dh)",
            work_unit.image_id,
            hour_end,
        )

        grib_path = Path(downloaded_file_path)
        if not grib_path.exists():
            raise FileNotFoundError(f"GRIB file not found: {grib_path}")

        work_dir = self._ensure_dir(self._get_band_dir(work_unit) / work_unit.image_id)
        output_dir = self._ensure_dir(work_dir / "outputs")

        clipped = await asyncio.to_thread(
            self._load_and_prepare, grib_path, hour_end, work_unit.bounds
        )
        self._check_shutdown()

        cog_path, geojson_path = await asyncio.to_thread(
            self._generate_outputs, clipped, output_dir, work_unit.image_id
        )
        del clipped
        gc.collect()
        self._check_shutdown()

        await self._upload(cog_path, geojson_path, forecast_ts, work_unit)

        self._cleanup_file(cog_path)
        self._cleanup_file(geojson_path)

    # ------------------------------------------------------------------
    # Internal helpers (run in thread via asyncio.to_thread)
    # ------------------------------------------------------------------

    def _load_and_prepare(
        self, grib_path: Path, hour_end: int, bounds: dict
    ) -> xr.DataArray:
        """Read GRIB, select step, convert Pa→hPa, reproject and clip."""
        import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

        logger.info("[ECMWF-MSLP] Step 1: Reading GRIB %s", grib_path.name)
        ds = xr.open_dataset(
            grib_path,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {"shortName": ECMWF_MSLP_CONFIG.parameter}
            },
        )
        msl_var = ds["msl"]

        logger.info(
            "[ECMWF-MSLP] Step 2: Selecting step T+%dh and converting Pa→hPa",
            hour_end,
        )
        msl_step = msl_var.sel(step=pd.Timedelta(hours=hour_end))
        msl_hpa = msl_step / _PA_TO_HPA

        del msl_var, msl_step, ds
        gc.collect()

        msl_hpa.attrs["long_name"] = "Mean Sea Level Pressure"
        msl_hpa.attrs["units"] = "hPa"
        msl_hpa.attrs.pop("grid_mapping", None)

        rename_map = {}
        if "latitude" in msl_hpa.dims:
            rename_map["latitude"] = "y"
        if "longitude" in msl_hpa.dims:
            rename_map["longitude"] = "x"
        if rename_map:
            msl_hpa = msl_hpa.rename(rename_map)

        msl_hpa.rio.write_crs("EPSG:4326", inplace=True)
        msl_hpa.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        logger.info("[ECMWF-MSLP] Step 3: Reprojecting and clipping")
        reproj = msl_hpa.rio.reproject("EPSG:4326", resolution=None)
        reproj.rio.write_nodata(float("nan"), inplace=True)
        del msl_hpa
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
        self, clipped: xr.DataArray, output_dir: Path, image_id: str
    ) -> tuple[Path, Path]:
        """Generate the COG and the simplified-isobars GeoJSON."""
        logger.info("[ECMWF-MSLP] Step 4: Generating COG")
        cog_path = save_as_cog(clipped, output_dir, image_id)

        logger.info(
            "[ECMWF-MSLP] Step 5: Smoothing field (sigma=%.2f) and extracting isobars",
            self._smoothing_sigma,
        )
        smoothed = smooth_field(clipped, sigma=self._smoothing_sigma)
        features = extract_isolines(
            smoothed,
            step=_ISOBAR_STEP_HPA,
            simplify_tolerance=self._isobar_simplify_tolerance,
            value_property="pressure_hpa",
        )
        logger.info("[ECMWF-MSLP] Extracted %d isobar features", len(features))

        geojson_path = output_dir / f"{image_id}.json"
        write_geojson(features, geojson_path)

        del smoothed, features
        gc.collect()
        return cog_path, geojson_path

    async def _upload(
        self,
        cog_path: Path,
        geojson_path: Path,
        forecast_ts: str,
        work_unit: WorkUnit,
    ) -> None:
        """Upload COG and GeoJSON to S3."""
        cog_key = (
            f"{ECMWF_MSLP_CONFIG.cog_prefix}/{forecast_ts}/{work_unit.image_id}.tif"
        )
        # geojson_prefix is required for MSLP; assert helps type-checkers and surfaces config errors.
        assert ECMWF_MSLP_CONFIG.geojson_prefix is not None
        geojson_key = f"{ECMWF_MSLP_CONFIG.geojson_prefix}/{forecast_ts}/{work_unit.image_id}.json"

        self._check_shutdown()
        logger.info("[ECMWF-MSLP] Step 6a: Uploading COG → %s", cog_key)
        cog_uploaded = await self._s3_client.upload_file(cog_key, cog_path)
        if not cog_uploaded:
            logger.warning(
                "[ECMWF-MSLP] COG upload failed for %s; continuing", work_unit.image_id
            )

        self._check_shutdown()
        logger.info("[ECMWF-MSLP] Step 6b: Uploading GeoJSON → %s", geojson_key)
        geojson_uploaded = await self._s3_client.upload_file(geojson_key, geojson_path)
        if not geojson_uploaded:
            logger.warning(
                "[ECMWF-MSLP] GeoJSON upload failed for %s; continuing",
                work_unit.image_id,
            )

        logger.info("[ECMWF-MSLP] Upload complete: %s", work_unit.image_id)


def _fmt_ts(dt: datetime) -> str:
    """Format datetime as YYYYMMDDTHHmmZ."""
    return dt.strftime("%Y%m%dT%H%MZ")
