"""
Weather Radar processor - converts H5 polar data to XYZ tiles.

Enhanced version that generates full-size radar images using polar coordinates
and SMN color palettes, similar to operational radar visualization.

Processing pipeline:
1. Read H5 file with PyART (SINARAME format)
2. Extract polar data (range, azimuth) for each sweep
3. Apply SMN color palettes to create RGBA data
4. Convert polar coordinates to geographic (lat/lon) grid
5. Create RGBA GeoTIFF with proper georeferencing
6. Generate XYZ tiles with gdal2tiles
7. Upload tiles to S3
"""

import gc
import shutil
import subprocess
import uuid
from logging import getLogger
from pathlib import Path
import numpy as np
import rasterio
import xarray as xr
from rasterio.transform import from_bounds
from rasterio.crs import CRS  # pylint: disable=no-name-in-module

from config import Config
from exceptions import UnprocessableInputError
from factories import create_s3_client
from models.work_unit import WorkUnit
from models.radar_config import get_radar_product_config, parse_radar_filename
from processors.base_processor import ImageProcessor
from models.radar_palettes import get_palette, mask_radar_data
from processors.base_processor import ImageProcessor

logger = getLogger(__name__)


def _nearest_azimuth_indices(azimuths: np.ndarray, theta: np.ndarray) -> np.ndarray:
    """Index of the nearest azimuth ray for each angle in ``theta`` (degrees).

    Azimuth is circular, so the nearest ray to an angle is always one of the two
    rays bracketing it in sorted order (predecessor/successor, wrapping across
    0/360). We binary-search the insertion point and keep whichever bracket is
    closer by circular distance. This is an exact replacement for a per-angle
    ``argmin`` over circular distance, but runs in O(M log N) instead of O(M N)
    (M = number of angles, N = number of rays) and is fully vectorized.

    The returned indices reference the original (unsorted) ``azimuths`` array, so
    callers can keep indexing the polar field with the radar's native ray order.
    """
    azimuths = np.asarray(azimuths, dtype=float)
    order = np.argsort(azimuths)
    az_sorted = azimuths[order]
    n_rays = len(azimuths)

    pos = np.searchsorted(az_sorted, theta)
    left = (pos - 1) % n_rays
    right = pos % n_rays

    dist_left = np.abs(az_sorted[left] - theta)
    dist_left = np.minimum(dist_left, 360.0 - dist_left)
    dist_right = np.abs(az_sorted[right] - theta)
    dist_right = np.minimum(dist_right, 360.0 - dist_right)

    return order[np.where(dist_right < dist_left, right, left)]


class RadarProcessor(ImageProcessor):
    """
    Processor for weather radar imagery (SINARAME H5 format).

    Generates full-size radar images using polar coordinate projection,
    matching SMN operational visualization standards.
    """

    # Processing parameters
    ZOOM_LEVELS = "4-9"
    GDAL_PROCESSES = 2
    SWEEPS = (0, 1, 2)  # Elevation indices to process

    # Geographic extent (adjust based on radar range)
    # These will be calculated dynamically based on actual radar range
    MAX_RANGE_KM = 240  # Maximum radar range in km

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config)

    async def process(  # pylint: disable=too-many-locals
        self, downloaded_file_path: str, work_unit: WorkUnit
    ) -> None:
        """Execute the full radar processing pipeline."""
        logger.info(
            "[RADAR] Starting processing for %s",
            work_unit.image_id,
        )

        h5_path = Path(downloaded_file_path)
        if not h5_path.exists():
            raise FileNotFoundError(f"Radar file not found: {h5_path}")

        # Parse filename to get product info
        original_filename = Path(work_unit.source_uri).name
        parsed = parse_radar_filename(original_filename)
        product_id = parsed["variable"]
        product_config = get_radar_product_config(product_id)

        # Setup work directories
        work_dir = Path(self.config.TMP_DIR) / "radar" / work_unit.image_id
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Read radar data with PyART
            self._check_shutdown()
            with self._time_stage("read"):
                radar = self._read_radar(h5_path)

            # Get field name from radar object
            field_name = self._get_field_name(radar, product_id)

            # Get color palette for this product
            palette = get_palette(product_id)

            # Process each elevation sweep
            for sweep_idx in self.SWEEPS:
                self._check_shutdown()

                if sweep_idx >= radar.nsweeps:
                    logger.warning(
                        "[RADAR] Sweep %d not available (only %d sweeps)",
                        sweep_idx,
                        radar.nsweeps,
                    )
                    continue

                elevation = radar.fixed_angle["data"][sweep_idx]
                logger.info(
                    "[RADAR] Processing sweep %d (elevation %.1f°)",
                    sweep_idx,
                    elevation,
                )

                # Output subdirectory for this sweep
                sweep_id = f"{work_unit.image_id}_elev{sweep_idx}"
                sweep_dir = work_dir / f"elev{sweep_idx}"
                sweep_dir.mkdir(parents=True, exist_ok=True)

                # Extract polar data for this sweep
                with self._time_stage("extract"):
                    polar_data = self._extract_polar_data(
                        radar, sweep_idx, field_name, product_id
                    )

                # Compute cartesian mapping once (shared between COG and GeoTIFF)
                with self._time_stage("mapping"):
                    mapping = self._compute_cartesian_mapping(
                        polar_data["ranges"],
                        polar_data["azimuths"],
                        polar_data["radar_lat"],
                        polar_data["radar_lon"],
                    )
                range_idx, azimuth_idx, outside_range, bounds, grid_size = mapping

                # Create COG (raw float32 field values) before colorizing
                cog_path = sweep_dir / f"{sweep_id}_cog.tif"
                with self._time_stage("cog"):
                    self._save_polar_cog(
                        polar_data["data"],
                        range_idx,
                        azimuth_idx,
                        outside_range,
                        bounds,
                        grid_size,
                        cog_path,
                    )

                # Create GeoTIFF (RGBA, colorized) using the same mapping
                geotiff_path = sweep_dir / f"{sweep_id}.tif"
                with self._time_stage("geotiff"):
                    self._polar_to_geotiff_with_mapping(
                        polar_data,
                        mapping,
                        geotiff_path,
                        palette,
                    )

                del polar_data
                gc.collect()

                # Generate tiles
                self._check_shutdown()
                tiles_dir = sweep_dir / "tiles"
                with self._time_stage("tiling"):
                    self._generate_tiles(geotiff_path, tiles_dir)

                # Upload tiles to S3
                self._check_shutdown()
                elevation_id = f"elev{sweep_idx}"
                s3_prefix = (
                    f"{product_config.s3_tiles_prefix}/{parsed['radar_id']}/{parsed['variable']}/"
                    f"{elevation_id}/{parsed['timestamp']}"
                )
                with self._time_stage("upload"):
                    await self._upload_tiles(tiles_dir, s3_prefix)

                # Upload COG to storage
                self._check_shutdown()
                cog_key = (
                    f"{product_config.s3_cog_prefix}/{parsed['radar_id']}/{parsed['variable']}/"
                    f"{elevation_id}/{parsed['timestamp']}.tif"
                )
                with self._time_stage("upload"):
                    cog_uploaded = await self._s3_client.upload_file(cog_key, cog_path)
                if not cog_uploaded:
                    logger.warning(
                        "[RADAR] COG upload failed for %s (key=%s); continuing",
                        work_unit.image_id,
                        cog_key,
                    )

                logger.info(
                    "[RADAR] Completed sweep %d → %s",
                    sweep_idx,
                    s3_prefix,
                )

            logger.info("[RADAR] Completed processing %s", work_unit.image_id)

        finally:
            # import time
            # time.sleep(60)
            if work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)

    def _read_radar(self, h5_path: Path):
        """Read H5 radar file using PyART (SINARAME HDF5 format)."""
        import pyart  # pylint: disable=import-outside-toplevel

        logger.info("[RADAR] Reading %s", h5_path.name)

        # Use SINARAME reader for Argentine radar files
        try:
            radar = pyart.aux_io.read_sinarame_h5(str(h5_path))
        except ValueError as exc:
            # Some RMA scans (notably dual-pol KDP) carry sweeps with different
            # range geometry (rstart/rscale); pyart's single global range array
            # can't represent them and raises "... changes between sweeps". This
            # is a real scan-strategy data shape, not corruption — skip it
            # cleanly (no retry/DLQ) rather than crash. Re-raise any other
            # ValueError so genuine read bugs still surface as errors.
            if "changes between sweeps" in str(exc):
                raise UnprocessableInputError(
                    f"Incompatible sweep range geometry for {h5_path.name}: {exc}"
                ) from exc
            raise

        logger.info(
            "[RADAR] Fields: %s, Sweeps: %d, Center: (%.4f, %.4f)",
            list(radar.fields.keys()),
            radar.nsweeps,
            radar.latitude["data"][0],
            radar.longitude["data"][0],
        )

        return radar

    def _get_field_name(self, radar, product_id: str) -> str:
        """
        Get PyART field name for the product.

        Maps product IDs to actual field names in the radar object.
        """
        # Common field name mappings
        field_mappings = {
            "DBZH": ["reflectivity", "reflectivity_horizontal", "total_power"],
            "ZH": ["reflectivity", "reflectivity_horizontal"],
            "TH": ["total_power", "reflectivity"],
            "VRAD": ["velocity"],
            "WRAD": ["spectrum_width"],
            "RHOHV": ["cross_correlation_ratio"],
            "ZDR": ["differential_reflectivity"],
            "KDP": ["specific_differential_phase"],
            "PHIDP": ["differential_phase"],
        }

        available_fields = list(radar.fields.keys())

        # Try to find matching field
        if product_id in field_mappings:
            for candidate in field_mappings[product_id]:
                if candidate in available_fields:
                    logger.info("[RADAR] Mapped %s → %s", product_id, candidate)
                    return candidate

        # Fallback: use first available field
        if available_fields:
            field_name = available_fields[0]
            logger.warning(
                "[RADAR] No mapping for %s, using first field: %s",
                product_id,
                field_name,
            )
            return field_name

        raise ValueError(f"No fields found in radar data for {product_id}")

    def _extract_polar_data(
        self, radar, sweep_idx: int, field_name: str, product_id: str
    ) -> dict:
        """
        Extract polar coordinate data for a sweep.

        Returns dict with:
        - data: masked array of radar values
        - ranges: 1D array of range gates (meters)
        - azimuths: 1D array of azimuth angles (degrees)
        - radar_lat: radar latitude
        - radar_lon: radar longitude
        """
        # Get raw data
        raw_data = radar.get_field(sweep_idx, field_name)

        # Apply product-specific masking
        data = mask_radar_data(raw_data, product_id)

        # Get coordinate arrays
        ranges = radar.range["data"]  # meters
        azimuths = radar.get_azimuth(sweep_idx)  # degrees

        # Radar location
        radar_lat = float(radar.latitude["data"][0])
        radar_lon = float(radar.longitude["data"][0])

        logger.info(
            "[RADAR] Polar data: %d rays × %d gates, range: %.0f-%.0f m",
            data.shape[0],
            data.shape[1],
            ranges.min(),
            ranges.max(),
        )

        return {
            "data": data,
            "ranges": ranges,
            "azimuths": azimuths,
            "radar_lat": radar_lat,
            "radar_lon": radar_lon,
        }

    def _compute_cartesian_mapping(  # pylint: disable=too-many-locals
        self,
        ranges: np.ndarray,
        azimuths: np.ndarray,
        radar_lat: float,
        radar_lon: float,
    ) -> tuple:
        """Compute cartesian grid indices shared between COG and GeoTIFF generation.

        Returns:
            (range_idx, azimuth_idx, outside_range, bounds, grid_size) where:
            - range_idx: flattened indices into the range dimension
            - azimuth_idx: flattened indices into the azimuth dimension
            - outside_range: flattened boolean mask (True = outside radar range)
            - bounds: (min_lon, max_lon, min_lat, max_lat)
            - grid_size: side length of the square cartesian grid
        """
        max_range_m = ranges.max()
        max_range_deg_lat = (max_range_m / 1000.0) / 111.0
        max_range_deg_lon = (max_range_m / 1000.0) / (
            111.0 * abs(np.cos(np.radians(radar_lat)))
        )

        min_lon = radar_lon - max_range_deg_lon
        max_lon = radar_lon + max_range_deg_lon
        min_lat = radar_lat - max_range_deg_lat
        max_lat = radar_lat + max_range_deg_lat

        range_res = ranges[1] - ranges[0] if len(ranges) > 1 else 1000
        pixels_per_range = int(2 * max_range_m / range_res)
        grid_size = min(2000, max(1000, pixels_per_range))

        logger.info(
            "[RADAR] Creating %dx%d grid for %.0f km range",
            grid_size,
            grid_size,
            max_range_m / 1000,
        )

        lons = np.linspace(min_lon, max_lon, grid_size)
        lats = np.linspace(min_lat, max_lat, grid_size)
        lon_grid, lat_grid = np.meshgrid(lons, lats)

        dx = (lon_grid - radar_lon) * 111000 * np.cos(np.radians(radar_lat))
        dy = (lat_grid - radar_lat) * 111000

        r_grid = np.sqrt(dx**2 + dy**2)
        theta_grid = np.degrees(np.arctan2(dx, dy)) % 360

        range_idx = np.searchsorted(ranges, r_grid.ravel())
        range_idx = np.clip(range_idx, 0, len(ranges) - 1)

        # Nearest azimuth ray per grid cell, vectorized (see _nearest_azimuth_indices).
        azimuth_idx = _nearest_azimuth_indices(azimuths, theta_grid.ravel())

        outside_range = r_grid.ravel() > max_range_m
        bounds = (min_lon, max_lon, min_lat, max_lat)
        return range_idx, azimuth_idx, outside_range, bounds, grid_size

    def _save_polar_cog(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        field_data: np.ma.MaskedArray,
        range_idx: np.ndarray,
        azimuth_idx: np.ndarray,
        outside_range: np.ndarray,
        bounds: tuple,
        grid_size: int,
        output_path: Path,
    ) -> None:
        """Project raw float32 field values to cartesian grid and save as COG."""
        import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

        values = field_data[azimuth_idx, range_idx].astype(np.float32)
        if np.ma.is_masked(values):
            values = np.ma.filled(values, fill_value=np.nan)

        grid_float = values.reshape(grid_size, grid_size)
        grid_float[outside_range.reshape(grid_size, grid_size)] = np.nan

        min_lon, max_lon, min_lat, max_lat = bounds
        lons = np.linspace(min_lon, max_lon, grid_size)
        lats = np.linspace(min_lat, max_lat, grid_size)

        da = xr.DataArray(
            np.flipud(grid_float),
            dims=["y", "x"],
            coords={"x": lons, "y": lats[::-1]},
        )
        da.rio.write_crs("EPSG:4326", inplace=True)
        da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
        da.rio.write_nodata(np.nan, inplace=True)

        tmp_path = output_path.parent / f"{uuid.uuid4()}.tif"
        try:
            da.rio.to_raster(tmp_path, driver="COG", compress="DEFLATE", predictor=3)
            tmp_path.rename(output_path)
            logger.info("[RADAR] COG written: %s", output_path.name)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def _polar_to_geotiff_with_mapping(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        polar_data: dict,
        mapping: tuple,
        output_path: Path,
        palette,
    ) -> None:
        """Colorize polar data and project to cartesian RGBA GeoTIFF using precomputed mapping."""
        logger.info("[RADAR] Creating GeoTIFF: %s", output_path.name)

        data = polar_data["data"]
        range_idx, azimuth_idx, outside_range, bounds, grid_size = mapping

        cmap, norm = palette.create_colormap()
        normalized = norm(data)
        rgba = cmap(normalized)
        rgba_uint8 = (rgba * 255).astype(np.uint8)

        rgba_uint8[:, :, 3] = 255
        if np.ma.is_masked(data):
            rgba_uint8[data.mask, 3] = 0

        below_min = data < palette.vmin
        rgba_uint8[below_min, 3] = 0

        grid_rgba = np.zeros((grid_size, grid_size, 4), dtype=np.uint8)

        for band in range(4):
            values = rgba_uint8[:, :, band][azimuth_idx, range_idx]
            grid_rgba[:, :, band] = values.reshape(grid_size, grid_size)

        grid_rgba[outside_range.reshape(grid_size, grid_size), 3] = 0

        min_lon, max_lon, min_lat, max_lat = bounds
        nrows, ncols = grid_rgba.shape[:2]
        transform = from_bounds(min_lon, min_lat, max_lon, max_lat, ncols, nrows)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(
            output_path,
            "w",
            driver="GTiff",
            height=nrows,
            width=ncols,
            count=4,
            dtype=np.uint8,
            crs=CRS.from_epsg(4326),
            transform=transform,
            compress="lzw",
        ) as dst:
            for i in range(4):
                dst.write(np.flipud(grid_rgba[:, :, i]), i + 1)

        logger.info(
            "[RADAR] GeoTIFF created: %.2f°-%.2f° lon, %.2f°-%.2f° lat",
            min_lon,
            max_lon,
            min_lat,
            max_lat,
        )

    def _generate_tiles(self, geotiff_path: Path, tiles_dir: Path) -> None:
        """Generate XYZ tiles using gdal2tiles."""
        logger.info("[RADAR] Generating tiles in %s", tiles_dir)

        tiles_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            "gdal2tiles.py",
            "-p",
            "mercator",
            "-z",
            self.ZOOM_LEVELS,
            "-w",
            "none",
            "--resampling=near",
            f"--processes={self.GDAL_PROCESSES}",
            "--xyz",
            "--tiledriver=WEBP",
            "--webp-lossless",
            str(geotiff_path),
            str(tiles_dir),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if result.returncode != 0:
            logger.error("[RADAR] gdal2tiles error: %s", result.stderr)
            raise RuntimeError(f"gdal2tiles failed: {result.stderr}")

        logger.info("[RADAR] Tiles generated successfully")

    async def _upload_tiles(self, tiles_dir: Path, s3_prefix: str) -> None:
        """Upload generated tiles to S3."""
        logger.info("[RADAR] Uploading tiles to %s", s3_prefix)

        count = await self._s3_client.upload_directory(tiles_dir, s3_prefix)

        logger.info("[RADAR] Uploaded %d files to S3", count)
