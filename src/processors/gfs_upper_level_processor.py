"""GFS upper-level processor: one class serving both 500 hPa and 250 hPa."""

import asyncio
import gc
import logging
from pathlib import Path

import numpy as np
import xarray as xr

from config import Config
from factories import create_s3_client
from models.gfs_config import (
    GEOPOTENTIAL_STEP_M,
    ISOTHERM_STEP_C,
    POINT_QUERY_GEOPOTENTIAL,
    POINT_QUERY_TEMPERATURE,
    GfsProductConfig,
    get_gfs_product_config_by_band,
    primary_cog_key,
    secondary_cog_key,
)
from models.gfs_palettes import wind_palette
from models.gfs_step import GfsStepContext
from models.units import KELVIN_TO_CELSIUS, MS_TO_KNOTS
from models.work_unit import WorkUnit
from processors.contour_processor import ContourProcessor
from services.contouring import extract_barbs_tiled, write_geojson
from services.gfs_field import coordinate_mesh, load_field
from services.processing_steps import (
    build_rgba_data_array,
    fill_missing_tiles,
    prewarp_to_mercator_grid,
    run_gdal2tiles,
    save_as_cog,
    save_rgba_geotiff,
    threshold_colorize,
)

logger = logging.getLogger(__name__)

_GDAL_PROCESSES = 2  # zoom range from settings.json via config.GFS_ZOOM
_LEVEL_WITH_THERMAL_AND_BARBS = 500

_SECONDARY_COG_FIELDS = {
    "height": POINT_QUERY_GEOPOTENTIAL,
    "temperature": POINT_QUERY_TEMPERATURE,
}


class GfsUpperLevelProcessor(ContourProcessor):
    """Renders one forecast step of an isobaric-level product.

    Emits, per step:
      * a COG of wind speed in knots and colourised tiles,
      * geopotential height contours every 60 gpm,
      * a secondary point-query COG per scalar field the level carries
        (geopotential always, temperature at 500 hPa),
      * (500 hPa only) isotherms every 5 C and wind barbs.
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Run the full pipeline for one (cycle, step, level)."""
        product = get_gfs_product_config_by_band(work_unit.band_id)
        step = GfsStepContext.from_work_unit(work_unit, downloaded_file_path)

        logger.info(
            "[%s] Processing %s (T+%dh)",
            product.log_prefix,
            work_unit.image_id,
            step.step_hours,
        )
        raster_dir, tiles_dir = self._prepare_dirs(work_unit)

        with self._time_stage("load"):
            fields = await asyncio.to_thread(
                self._load, step.grib_path, product, work_unit.bounds
            )
        self._check_shutdown()

        cog_path, secondary_cogs, rgba_path, overlays = await asyncio.to_thread(
            self._generate_outputs, fields, product, raster_dir, work_unit.image_id
        )
        del fields
        gc.collect()
        self._check_shutdown()

        tiles_output_dir = await self._build_tiles(
            rgba_path, raster_dir, tiles_dir, work_unit.bounds
        )

        with self._time_stage("upload"):
            await self._upload(
                cog_path,
                secondary_cogs,
                tiles_output_dir,
                overlays,
                product,
                step.cycle_ts,
                work_unit,
            )

        self._cleanup_file(rgba_path)
        self._cleanup_file(cog_path)
        for path in secondary_cogs.values():
            self._cleanup_file(path)
        for path in overlays.values():
            if path.is_dir():
                self._cleanup_directory(path)
            else:
                self._cleanup_file(path)
        self._cleanup_directory(tiles_output_dir)

    def _prepare_dirs(self, work_unit: WorkUnit) -> tuple[Path, Path]:
        """Scratch directories for the raster stage and the tile pyramid."""
        work_dir = self._get_band_dir(work_unit) / self._work_dir_leaf(work_unit)
        return (
            self._ensure_dir(work_dir / "raster"),
            self._ensure_dir(work_dir / "tiles"),
        )

    def _load(
        self, grib_path: Path, product: GfsProductConfig, bounds: dict
    ) -> dict[str, xr.DataArray]:
        """Read every field this product needs at its level."""
        level = product.level_hpa
        logger.info("[%s] Reading GRIB %s", product.log_prefix, grib_path.name)

        u_wind = load_field(grib_path, "u", bounds, level_hpa=level)
        v_wind = load_field(grib_path, "v", bounds, level_hpa=level)
        speed = _wind_speed_knots(u_wind, v_wind)

        fields: dict[str, xr.DataArray] = {
            "speed": speed,
            "height": load_field(grib_path, "gh", bounds, level_hpa=level),
        }
        if level == _LEVEL_WITH_THERMAL_AND_BARBS:
            fields["temperature"] = (
                load_field(grib_path, "t", bounds, level_hpa=level) - KELVIN_TO_CELSIUS
            )
            fields["u"] = u_wind
            fields["v"] = v_wind
        else:
            del u_wind, v_wind
            gc.collect()
        return fields

    def _generate_outputs(
        self,
        fields: dict[str, xr.DataArray],
        product: GfsProductConfig,
        output_dir: Path,
        image_id: str,
    ) -> tuple[Path, dict[str, Path], Path, dict[str, Path]]:
        """Write both kinds of COG, the colourised raster and every overlay."""
        with self._time_stage("cog"):
            cog_path = save_as_cog(fields["speed"], output_dir, image_id)
            secondary_cogs = _save_secondary_cogs(fields, output_dir, image_id)

        with self._time_stage("geotiff"):
            rgba_path = self._colorize(fields["speed"], product, output_dir, image_id)

        with self._time_stage("geojson"):
            overlays = self._build_overlays(fields, product, output_dir, image_id)

        return cog_path, secondary_cogs, rgba_path, overlays

    def _build_overlays(
        self,
        fields: dict[str, xr.DataArray],
        product: GfsProductConfig,
        output_dir: Path,
        image_id: str,
    ) -> dict[str, Path]:
        """Geopotential contours, plus isotherms and barbs at 500 hPa."""
        overlays = {
            "heights": write_geojson(
                self._contour(fields["height"], GEOPOTENTIAL_STEP_M, "height_gpm"),
                output_dir / f"{image_id}_heights.json",
            )
        }
        if "temperature" in fields:
            overlays["isotherms"] = write_geojson(
                self._contour(fields["temperature"], ISOTHERM_STEP_C, "temp_c"),
                output_dir / f"{image_id}_isotherms.json",
            )
        if "u" in fields:
            overlays["barbs"] = self._write_barbs(fields, output_dir, image_id)
        logger.info("[%s] Overlays: %s", product.log_prefix, sorted(overlays))
        return overlays

    def _write_barbs(
        self, fields: dict[str, xr.DataArray], output_dir: Path, image_id: str
    ) -> Path:
        """Write one GeoJSON per barb tile and return the directory holding them.

        `extract_barbs_tiled` buckets the features by (zoom, x, y); each bucket
        becomes its own `{z}/{x}/{y}.json`.

        The returned path is a directory, which is how `_upload` tells it apart
        from the single-file overlays.
        """
        lon_2d, lat_2d = coordinate_mesh(fields["u"])
        tiled = extract_barbs_tiled(
            u_ms=fields["u"].values,
            v_ms=fields["v"].values,
            lon_2d=lon_2d,
            lat_2d=lat_2d,
            zoom_strides=self.config.GFS_BARB_STRIDES,
        )
        root = self._ensure_dir(output_dir / f"{image_id}_barbs")
        for (zoom, tile_x, tile_y), features in tiled.items():
            tile_dir = self._ensure_dir(root / str(zoom) / str(tile_x))
            write_geojson(features, tile_dir / f"{tile_y}.json")
        logger.info("[GFS] Wrote %d barb tile(s)", len(tiled))
        return root

    def _upsample(self, speed: xr.DataArray, product: GfsProductConfig) -> xr.DataArray:
        """Interpolate the wind field onto a fine grid before colourising.

        GFS is 0.25 degrees, about 22 km at our latitudes — roughly what one tile
        pixel covers at zoom 2. Shading that natively and cutting tiles to zoom 7
        would just replicate each cell across ~23x23 pixels, so the colour bands
        render as hard staircases. Interpolating first turns them into smooth
        curves, which is exactly why `EcmwfTotalPrecipitationProcessor` does the
        same thing for its precipitation thresholds.

        Only the tiles use this; the COG keeps the native grid so downstream
        consumers still get real model values.
        """
        from rasterio.enums import (  # pylint: disable=import-outside-toplevel
            Resampling,
        )

        resolution = self.config.GFS_TILE_SMOOTHING_RESOLUTION_DEG
        if resolution <= 0:
            return speed

        logger.info(
            "[%s] Upsampling wind field to %.4f deg (bilinear) for smooth tiles",
            product.log_prefix,
            resolution,
        )
        upsampled = speed.rio.reproject(
            speed.rio.crs, resolution=resolution, resampling=Resampling.bilinear
        )
        upsampled.rio.write_nodata(float("nan"), inplace=True)
        return upsampled

    def _colorize(
        self,
        speed: xr.DataArray,
        product: GfsProductConfig,
        output_dir: Path,
        image_id: str,
    ) -> Path:
        """Shade the wind field with this product's palette."""
        import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

        speed = self._upsample(speed, product)
        thresholds, colors = wind_palette(product.product_id)
        red, green, blue, alpha = threshold_colorize(speed, thresholds, colors)
        gc.collect()

        rgba = build_rgba_data_array(
            red, green, blue, alpha, speed["x"], speed["y"], "Wind Speed"
        )
        del red, green, blue, alpha
        gc.collect()

        try:
            return save_rgba_geotiff(rgba, output_dir / f"{image_id}_rgba.tif")
        finally:
            del rgba
            gc.collect()

    async def _build_tiles(
        self, rgba_path: Path, raster_dir: Path, tiles_dir: Path, bounds: dict
    ) -> Path:
        """Prewarp then cut tiles, same as the other model products."""
        with self._time_stage("prewarp"):
            prewarped = await asyncio.to_thread(
                prewarp_to_mercator_grid,
                rgba_path,
                raster_dir,
                self.config.GFS_ZOOM.max_zoom,
            )
        self._check_shutdown()

        with self._time_stage("tiling"):
            tiles_output_dir = await asyncio.to_thread(
                run_gdal2tiles,
                prewarped,
                tiles_dir,
                self.config.GFS_ZOOM.spec,
                _GDAL_PROCESSES,
            )
            await asyncio.to_thread(
                fill_missing_tiles,
                tiles_output_dir,
                bounds,
                self.config.GFS_ZOOM.spec,
            )
        self._check_shutdown()
        self._cleanup_file(prewarped)
        return tiles_output_dir

    async def _upload(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        cog_path: Path,
        secondary_cogs: dict[str, Path],
        tiles_output_dir: Path,
        overlays: dict[str, Path],
        product: GfsProductConfig,
        cycle_ts: str,
        work_unit: WorkUnit,
    ) -> None:
        """Upload both kinds of COG, the tile pyramid and every overlay."""
        image_id = work_unit.image_id
        await self._upload_secondary_cogs(secondary_cogs, product, cycle_ts, image_id)
        await self._upload_overlays(overlays, product, cycle_ts, image_id)

        self._check_shutdown()
        tiles_prefix = f"{product.tiles_prefix}/{cycle_ts}/{image_id}"
        logger.info("[%s] Uploading tiles → %s", product.log_prefix, tiles_prefix)
        await self._s3_client.upload_directory(tiles_output_dir, tiles_prefix)

        self._check_shutdown()
        cog_key = primary_cog_key(product, cycle_ts, image_id)
        logger.info("[%s] Uploading COG → %s", product.log_prefix, cog_key)
        if not await self._s3_client.upload_file(cog_key, cog_path):
            logger.warning(
                "[%s] COG upload failed for %s; continuing",
                product.log_prefix,
                image_id,
            )
        logger.info("[%s] Upload complete: %s", product.log_prefix, image_id)

    async def _upload_secondary_cogs(
        self,
        secondary_cogs: dict[str, Path],
        product: GfsProductConfig,
        cycle_ts: str,
        image_id: str,
    ) -> None:
        """Upload one point-query COG per variable, each in its own sub-prefix."""
        for variable, path in secondary_cogs.items():
            self._check_shutdown()
            key = secondary_cog_key(product, cycle_ts, variable, image_id)
            logger.info("[%s] Uploading %s COG → %s", product.log_prefix, variable, key)
            if not await self._s3_client.upload_file(key, path):
                logger.warning(
                    "[%s] COG upload failed for %s; continuing",
                    product.log_prefix,
                    key,
                )

    async def _upload_overlays(
        self,
        overlays: dict[str, Path],
        product: GfsProductConfig,
        cycle_ts: str,
        image_id: str,
    ) -> None:
        """Upload every vector overlay, barb directories included."""
        for name, path in overlays.items():
            self._check_shutdown()
            base = f"{product.geojson_prefix}/{cycle_ts}/{image_id}_{name}"
            if path.is_dir():
                count = await self._s3_client.upload_directory(path, base)
                logger.info(
                    "[%s] Uploaded %d barb tile(s) → %s",
                    product.log_prefix,
                    count,
                    base,
                )
                continue

            key = f"{base}.json"
            logger.info("[%s] Uploading overlay → %s", product.log_prefix, key)
            if not await self._s3_client.upload_file(key, path):
                logger.warning(
                    "[%s] Overlay upload failed for %s; continuing",
                    product.log_prefix,
                    key,
                )


def _save_secondary_cogs(
    fields: dict[str, xr.DataArray], output_dir: Path, image_id: str
) -> dict[str, Path]:
    """Point-query COGs for whichever scalar fields this level carries.

    Keyed by the variable name the S3 key uses, so the caller never has to know
    the `_load` field names.
    """
    return {
        variable: save_as_cog(fields[key], output_dir, f"{image_id}_{variable}")
        for key, variable in _SECONDARY_COG_FIELDS.items()
        if key in fields
    }


def _wind_speed_knots(u_wind: xr.DataArray, v_wind: xr.DataArray) -> xr.DataArray:
    """Wind magnitude in knots."""
    speed = xr.apply_ufunc(np.hypot, u_wind, v_wind) * MS_TO_KNOTS
    speed = speed.rio.write_crs(u_wind.rio.crs)
    speed.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    speed.rio.write_nodata(float("nan"), inplace=True)
    speed.attrs["long_name"] = "Wind Speed"
    speed.attrs["units"] = "kt"
    return speed
