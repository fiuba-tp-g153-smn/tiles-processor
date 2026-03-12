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
from logging import getLogger
from pathlib import Path
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS  # pylint: disable=no-name-in-module

from config import Config
from factories import create_s3_client
from models.work_unit import WorkUnit
from models.radar_config import parse_radar_filename
from processors.base_processor import ImageProcessor
from models.radar_palettes import get_palette, mask_radar_data

logger = getLogger(__name__)


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
        self._s3_client = create_s3_client(config, with_ttl=False)

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

        # Setup work directories
        work_dir = Path(self.config.TMP_DIR) / "radar" / work_unit.image_id
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Read radar data with PyART
            self._check_shutdown()
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
                polar_data = self._extract_polar_data(
                    radar, sweep_idx, field_name, product_id
                )

                # Create GeoTIFF from polar data
                geotiff_path = sweep_dir / f"{sweep_id}.tif"
                self._polar_to_geotiff(
                    polar_data, radar, sweep_idx, geotiff_path, palette
                )

                del polar_data
                gc.collect()

                # Generate tiles
                self._check_shutdown()
                tiles_dir = sweep_dir / "tiles"
                self._generate_tiles(geotiff_path, tiles_dir)

                # Upload to S3
                self._check_shutdown()
                s3_prefix = (
                    f"radar/{parsed['radar_id']}/{parsed['variable']}/"
                    f"{parsed['timestamp']}_elev{sweep_idx}"
                )
                await self._upload_tiles(tiles_dir, s3_prefix)

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
        radar = pyart.aux_io.read_sinarame_h5(str(h5_path))

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

    def _polar_to_geotiff(  # pylint: disable=too-many-locals
        self,
        polar_data: dict,
        radar,
        sweep_idx: int,
        output_path: Path,
        palette,
    ) -> None:
        """
        Convert polar radar data to georeferenced RGBA GeoTIFF.

        This method creates a cartesian grid and properly georeferences it,
        similar to the approach in main.py but outputting GeoTIFF instead of PNG.
        """
        logger.info("[RADAR] Creating GeoTIFF: %s", output_path.name)

        data = polar_data["data"]
        ranges = polar_data["ranges"]
        azimuths = polar_data["azimuths"]
        radar_lat = polar_data["radar_lat"]
        radar_lon = polar_data["radar_lon"]

        # Create colormap and normalization
        cmap, norm = palette.create_colormap()

        # Normalize data and apply colormap
        normalized = norm(data)
        rgba = cmap(normalized)
        rgba_uint8 = (rgba * 255).astype(np.uint8)

        # Set alpha channel: opaque for valid data, transparent for masked
        rgba_uint8[:, :, 3] = 255
        if np.ma.is_masked(data):
            rgba_uint8[data.mask, 3] = 0

        # Also make data below minimum value transparent
        below_min = data < palette.vmin
        rgba_uint8[below_min, 3] = 0

        # Convert polar to cartesian coordinates
        # This creates a grid in lat/lon space
        grid_data, bounds = self._polar_to_cartesian_grid(
            rgba_uint8, ranges, azimuths, radar_lat, radar_lon
        )

        nrows, ncols = grid_data.shape[:2]
        min_lon, max_lon, min_lat, max_lat = bounds

        # Create geotransform
        transform = from_bounds(min_lon, min_lat, max_lon, max_lat, ncols, nrows)

        # Write GeoTIFF
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
                # Flip vertically for proper georeferencing
                dst.write(np.flipud(grid_data[:, :, i]), i + 1)

        logger.info(
            "[RADAR] GeoTIFF created: %.2f°-%.2f° lon, %.2f°-%.2f° lat",
            min_lon,
            max_lon,
            min_lat,
            max_lat,
        )

    def _polar_to_cartesian_grid(  # pylint: disable=too-many-locals
        self,
        rgba_data: np.ndarray,
        ranges: np.ndarray,
        azimuths: np.ndarray,
        radar_lat: float,
        radar_lon: float,
    ) -> tuple:
        """
        Convert polar data to cartesian grid in geographic coordinates.

        Returns:
            - grid_data: (height, width, 4) RGBA array
            - bounds: (min_lon, max_lon, min_lat, max_lat)
        """
        # Calculate grid bounds based on actual radar range
        max_range_m = ranges.max()
        max_range_deg_lat = (max_range_m / 1000.0) / 111.0
        max_range_deg_lon = (max_range_m / 1000.0) / (
            111.0 * abs(np.cos(np.radians(radar_lat)))
        )

        min_lon = radar_lon - max_range_deg_lon
        max_lon = radar_lon + max_range_deg_lon
        min_lat = radar_lat - max_range_deg_lat
        max_lat = radar_lat + max_range_deg_lat

        # Create output grid
        # Use resolution based on range resolution
        range_res = ranges[1] - ranges[0] if len(ranges) > 1 else 1000
        pixels_per_range = int(2 * max_range_m / range_res)
        grid_size = min(2000, max(1000, pixels_per_range))

        logger.info(
            "[RADAR] Creating %dx%d grid for %.0f km range",
            grid_size,
            grid_size,
            max_range_m / 1000,
        )

        # Create coordinate grids
        lons = np.linspace(min_lon, max_lon, grid_size)
        lats = np.linspace(min_lat, max_lat, grid_size)
        lon_grid, lat_grid = np.meshgrid(lons, lats)

        # Convert geographic coordinates to radar-relative coordinates
        # Simple flat-earth approximation (sufficient for radar range)
        dx = (lon_grid - radar_lon) * 111000 * np.cos(np.radians(radar_lat))
        dy = (lat_grid - radar_lat) * 111000

        # Convert to polar
        r_grid = np.sqrt(dx**2 + dy**2)
        theta_grid = np.degrees(np.arctan2(dx, dy)) % 360

        # Create output grid
        grid_rgba = np.zeros((grid_size, grid_size, 4), dtype=np.uint8)

        # Interpolate from polar data to cartesian grid
        # For each cartesian pixel, find nearest polar coordinate
        nrays, ngates = rgba_data.shape[:2]

        # Find indices in polar data
        range_idx = np.searchsorted(ranges, r_grid.ravel())
        range_idx = np.clip(range_idx, 0, ngates - 1)

        # Azimuth matching (find nearest ray)
        azimuth_idx = np.zeros_like(theta_grid.ravel(), dtype=int)
        for i, theta in enumerate(theta_grid.ravel()):
            # Find nearest azimuth
            diff = np.abs(azimuths - theta)
            diff = np.minimum(diff, 360 - diff)  # Handle wraparound
            azimuth_idx[i] = np.argmin(diff)

        # Extract values from polar data
        for band in range(4):
            values = rgba_data[:, :, band][azimuth_idx, range_idx]
            grid_rgba[:, :, band] = values.reshape(grid_size, grid_size)

        # Mask values outside radar range
        outside_range = r_grid > ranges.max()
        grid_rgba[outside_range, 3] = 0  # Transparent

        return grid_rgba, (min_lon, max_lon, min_lat, max_lat)

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
