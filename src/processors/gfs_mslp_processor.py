"""GFS mean sea level pressure processor: COG + isobars + 1000/500 thickness."""

import asyncio
import gc
import logging
from pathlib import Path

import xarray as xr

from config import Config
from factories import create_s3_client
from models.gfs_config import (
    GFS_MSLP_CONFIG,
    HIGHLIGHTED_THICKNESS_M,
    ISOBAR_STEP_HPA,
    PA_TO_HPA,
    THICKNESS_LEVELS_HPA,
    THICKNESS_STEP_M,
)
from models.gfs_step import GfsStepContext
from models.work_unit import WorkUnit
from processors.contour_processor import ContourProcessor
from services.contouring import write_geojson
from services.gfs_field import load_field, load_levels
from services.processing_steps import save_as_cog

logger = logging.getLogger(__name__)

_LOG = f"[{GFS_MSLP_CONFIG.log_prefix}]"


class GfsMslpProcessor(ContourProcessor):
    """Renders one forecast step of the sea-level-pressure product.

    Emits three artifacts from the cached GRIB:
      * a COG of the pressure field in hPa,
      * isobars every 3 hPa,
      * 1000/500 thickness contours every 60 gpm, with the four air-mass
        markers the SMN charts single out (5280/5400/5580/5700) flagged for the
        frontend to style differently.
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Run the full pipeline for one (cycle, step)."""
        step = GfsStepContext.from_work_unit(work_unit, downloaded_file_path)

        logger.info(
            "%s Processing %s (T+%dh)", _LOG, work_unit.image_id, step.step_hours
        )
        output_dir = self._ensure_dir(
            self._get_band_dir(work_unit) / work_unit.image_id / "outputs"
        )

        with self._time_stage("load"):
            pressure, thickness = await asyncio.to_thread(
                self._load, step.grib_path, work_unit.bounds
            )
        self._check_shutdown()

        outputs = await asyncio.to_thread(
            self._generate_outputs, pressure, thickness, output_dir, work_unit.image_id
        )
        del pressure, thickness
        gc.collect()
        self._check_shutdown()

        with self._time_stage("upload"):
            await self._upload(outputs, step.cycle_ts, work_unit)

        for path in outputs.values():
            self._cleanup_file(path)

    def _load(self, grib_path: Path, bounds: dict) -> tuple[xr.DataArray, xr.DataArray]:
        """Read MSLET as hPa and the 1000/500 thickness as gpm."""
        logger.info("%s Reading GRIB %s", _LOG, grib_path.name)
        pressure = load_field(grib_path, "mslet", bounds) / PA_TO_HPA
        pressure.attrs["long_name"] = "Mean Sea Level Pressure"
        pressure.attrs["units"] = "hPa"

        top_hpa, base_hpa = THICKNESS_LEVELS_HPA
        heights = load_levels(grib_path, "gh", bounds, THICKNESS_LEVELS_HPA)
        thickness = heights[top_hpa] - heights[base_hpa]
        thickness.attrs["long_name"] = f"{base_hpa}/{top_hpa} hPa Thickness"
        thickness.attrs["units"] = "gpm"

        del heights
        gc.collect()
        return pressure, thickness

    def _generate_outputs(
        self,
        pressure: xr.DataArray,
        thickness: xr.DataArray,
        output_dir: Path,
        image_id: str,
    ) -> dict[str, Path]:
        """Write the COG and both GeoJSON overlays."""
        with self._time_stage("cog"):
            cog_path = save_as_cog(pressure, output_dir, image_id)

        with self._time_stage("geojson"):
            isobars = self._contour(pressure, ISOBAR_STEP_HPA, "pressure_hpa")
            thickness_lines = _flag_highlights(
                self._contour(thickness, THICKNESS_STEP_M, "thickness_gpm"),
                "thickness_gpm",
            )
            logger.info(
                "%s Extracted %d isobars and %d thickness contours",
                _LOG,
                len(isobars),
                len(thickness_lines),
            )
            isobars_path = write_geojson(
                isobars, output_dir / f"{image_id}_isobars.json"
            )
            thickness_path = write_geojson(
                thickness_lines, output_dir / f"{image_id}_thickness.json"
            )

        return {"cog": cog_path, "isobars": isobars_path, "thickness": thickness_path}

    async def _upload(
        self, outputs: dict[str, Path], cycle_ts: str, work_unit: WorkUnit
    ) -> None:
        """Upload the COG and both overlays."""
        image_id = work_unit.image_id
        targets = {
            f"{GFS_MSLP_CONFIG.cog_prefix}/{cycle_ts}/{image_id}.tif": outputs["cog"],
            f"{GFS_MSLP_CONFIG.geojson_prefix}/{cycle_ts}/{image_id}_isobars.json": (
                outputs["isobars"]
            ),
            f"{GFS_MSLP_CONFIG.geojson_prefix}/{cycle_ts}/{image_id}_thickness.json": (
                outputs["thickness"]
            ),
        }
        for key, path in targets.items():
            self._check_shutdown()
            logger.info("%s Uploading → %s", _LOG, key)
            if not await self._s3_client.upload_file(key, path):
                logger.warning("%s Upload failed for %s; continuing", _LOG, key)
        logger.info("%s Upload complete: %s", _LOG, image_id)


def _flag_highlights(features: list[dict], value_property: str) -> list[dict]:
    """Mark the air-mass thickness contours the SMN charts draw in colour."""
    for feature in features:
        value = feature["properties"][value_property]
        feature["properties"]["highlight"] = value in HIGHLIGHTED_THICKNESS_M
    return features
