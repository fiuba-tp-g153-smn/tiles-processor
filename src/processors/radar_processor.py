"""
Weather Radar processor - converts H5 polar data to XYZ tiles.

Processing pipeline:
1. Read H5 file with PyART (SINARAME format)
2. Convert polar coordinates to cartesian grid
3. Apply colormap and create RGBA GeoTIFF
4. Generate XYZ tiles with gdal2tiles
5. Upload tiles to MinIO
"""

import gc
import shutil
import subprocess
from logging import getLogger
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import matplotlib.colors as mcolors

from config import Config
from factories import create_minio_client
from models.work_unit import WorkUnit
from models.radar_config import (
    parse_radar_filename,
    get_radar_product_config,
    RadarProductConfig,
)
from processors.base_processor import ImageProcessor, ShutdownRequested

logger = getLogger(__name__)


class RadarProcessor(ImageProcessor):
    """
    Processor for weather radar imagery (SINARAME H5 format).

    Converts polar radar data to georeferenced tiles for web visualization.
    Processes multiple elevation sweeps per file.
    """

    # Processing parameters
    GRID_RESOLUTION = 500  # meters per pixel
    MAX_RANGE = 240_000  # 240 km range
    ZOOM_LEVELS = "4-10"
    GDAL_PROCESSES = 2
    SWEEPS = (0, 1, 2)  # Elevation indices to process

    def __init__(self, config: Config):
        super().__init__(config)
        self._minio_client = create_minio_client(config)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
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
        product_config = get_radar_product_config(parsed["variable"])

        # Setup work directories
        work_dir = Path(self.config.TMP_DIR) / "radar" / work_unit.image_id
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Read radar data with PyART
            self._check_shutdown()
            radar = self._read_radar(h5_path)

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

                # Convert to grid
                grid = self._radar_to_grid(radar, sweep_idx)

                # Create GeoTIFF
                geotiff_path = sweep_dir / f"{sweep_id}.tif"
                self._grid_to_geotiff(
                    grid, geotiff_path, product_config, parsed["variable"]
                )
                del grid
                gc.collect()

                # Generate tiles
                self._check_shutdown()
                tiles_dir = sweep_dir / "tiles"
                self._generate_tiles(geotiff_path, tiles_dir)

                # Upload to MinIO
                # Path format: radar/{radar_id}/{product}/{timestamp}_elev{N}/
                # This matches the producer's tileset detection logic
                self._check_shutdown()
                s3_prefix = f"radar/{parsed['radar_id']}/{parsed['variable']}/{parsed['timestamp']}_elev{sweep_idx}"
                await self._upload_tiles(tiles_dir, s3_prefix)

                logger.info(
                    "[RADAR] Completed sweep %d → %s",
                    sweep_idx,
                    s3_prefix,
                )

            logger.info("[RADAR] Completed processing %s", work_unit.image_id)

        finally:
            # Cleanup work directory
            if work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)

    def _read_radar(self, h5_path: Path):
        """Read H5 radar file using PyART (SINARAME HDF5 format)."""
        import pyart  # pylint: disable=import-outside-toplevel

        logger.info("[RADAR] Reading %s", h5_path.name)
        # Use SINARAME reader - Argentine radar files use SINARAME format
        radar = pyart.aux_io.read_sinarame_h5(str(h5_path))
        logger.info(
            "[RADAR] Fields: %s, Sweeps: %d, Center: (%.4f, %.4f)",
            list(radar.fields.keys()),
            radar.nsweeps,
            radar.latitude["data"][0],
            radar.longitude["data"][0],
        )
        return radar

    def _radar_to_grid(self, radar, sweep: int):
        """Convert polar radar data to cartesian grid."""
        import pyart  # pylint: disable=import-outside-toplevel

        logger.info("[RADAR] Converting to grid (sweep=%d)", sweep)

        grid_size = int(2 * self.MAX_RANGE / self.GRID_RESOLUTION)
        field_name = list(radar.fields.keys())[0]

        grid = pyart.map.grid_from_radars(
            (radar,),
            grid_shape=(1, grid_size, grid_size),
            grid_limits=(
                (0, 10000),
                (-self.MAX_RANGE, self.MAX_RANGE),
                (-self.MAX_RANGE, self.MAX_RANGE),
            ),
            fields=[field_name],
            weighting_function="Barnes2",
            gridding_algo="map_gates_to_grid",
        )
        return grid

    def _grid_to_geotiff(
        self,
        grid,
        output_path: Path,
        product_config: RadarProductConfig,
        variable: str,
    ) -> None:
        """Save grid as colorized RGBA GeoTIFF."""
        logger.info("[RADAR] Creating GeoTIFF: %s", output_path.name)

        field_name = list(grid.fields.keys())[0]
        data = grid.fields[field_name]["data"][0]
        data = np.ma.filled(data, np.nan).astype(np.float32)

        # Get geographic bounds from grid
        lon = grid.point_longitude["data"][0]
        lat = grid.point_latitude["data"][0]
        min_lon, max_lon = float(lon.min()), float(lon.max())
        min_lat, max_lat = float(lat.min()), float(lat.max())

        nrows, ncols = data.shape

        # Create colormap from product config
        cmap, norm, min_val = self._create_colormap(product_config)

        # Apply discrete colormap
        indices = norm(data)
        rgba = cmap(indices)
        rgba_uint8 = (rgba * 255).astype(np.uint8)

        # Alpha: opaque where valid data >= min_val, transparent elsewhere
        rgba_uint8[:, :, 3] = 255
        mask = np.isnan(data) | (data < min_val)
        rgba_uint8[mask, 3] = 0

        # Write GeoTIFF
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
                dst.write(np.flipud(rgba_uint8[:, :, i]), i + 1)

        logger.info(
            "[RADAR] GeoTIFF bounds: (%.2f, %.2f) - (%.2f, %.2f)",
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        )

    def _create_colormap(self, product_config: RadarProductConfig):
        """Create discrete colormap from product config colors."""
        colors_list = product_config.colors
        values = [c[0] for c in colors_list]
        colors = [np.array(c[1][:3]) / 255.0 for c in colors_list]

        # BoundaryNorm: each value range gets a fixed color, no interpolation
        boundaries = values + [values[-1] + 5]
        norm = mcolors.BoundaryNorm(boundaries, len(colors))
        cmap = mcolors.ListedColormap(colors)

        return cmap, norm, min(values)

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
            "--tiledriver=WEBP",
            str(geotiff_path),
            str(tiles_dir),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logger.error("[RADAR] gdal2tiles error: %s", result.stderr)
            raise RuntimeError(f"gdal2tiles failed: {result.stderr}")

        logger.info("[RADAR] Tiles generated successfully")

    async def _upload_tiles(self, tiles_dir: Path, s3_prefix: str) -> None:
        """Upload generated tiles to MinIO."""
        logger.info("[RADAR] Uploading tiles to %s", s3_prefix)

        count = await self._minio_client.upload_directory(tiles_dir, s3_prefix)
        logger.info("[RADAR] Uploaded %d files to MinIO", count)
