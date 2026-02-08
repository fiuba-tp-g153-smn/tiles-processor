"""
XYZ Web Tile Generation Service.

This service generates XYZ map tiles from GeoTIFF files using GDAL's
gdal2tiles.py utility. The tiles are compatible with Leaflet and other
web mapping libraries.

Tile Specifications:
    - Format: WEBP (compressed, supports transparency)
    - Zoom levels: 3-7 (continental to regional scale)
    - Profile: XYZ (OSM/Slippy map standard, Y=0 at top)
    - Structure: {output_dir}/{image_name}_tiles/{z}/{x}/{y}.webp

Concurrency Control:
    - MAX_CONCURRENT_TILES: Limits parallel tile generation (default: 2)
    - GDAL_PROCESSES: Processes per gdal2tiles job (default: 2)
    - Total CPU utilization: MAX_CONCURRENT_TILES * GDAL_PROCESSES

Atomic Operations:
    Tiles are generated in a temporary UUID-named directory, then atomically
    renamed to the final destination. If a tile directory already exists,
    it is deleted before the rename (overwrite behavior).

File Overwrites:
    Existing tile directories with the same name are completely replaced.
    This ensures consistency and prevents stale tiles from accumulating.
"""

import logging
from pathlib import Path

from services.concurrent_runner import run_concurrently
from services.processing_steps import run_gdal2tiles

logger = logging.getLogger(__name__)


class GenerateTilesService:  # pylint: disable=too-few-public-methods
    """
    Generates XYZ web map tiles from GeoTIFF files.

    This service runs gdal2tiles.py on each input GeoTIFF to create a directory
    of tiles compatible with Leaflet, OpenLayers, and similar web mapping libraries.

    Attributes:
        MAX_CONCURRENT_TILES: Maximum GeoTIFFs processed in parallel (default: 2)
        GDAL_PROCESSES: gdal2tiles processes per job (default: 2)

    Args:
        geotiff_files: List of Path objects pointing to input GeoTIFF files
        output_dir: Base directory for tile output

    Output Structure:
        {output_dir}/
            {geotiff_stem}_tiles/
                {z}/                # Zoom level directories
                    {x}/            # X coordinate directories
                        {y}.webp    # Individual tiles

    gdal2tiles Command:
        gdal2tiles.py -z 3-7 -w none --xyz --tiledriver=WEBP --processes=2 input.tif output/

    Concurrency:
        Uses run_concurrently to limit concurrent tile generation, preventing
        CPU/memory exhaustion when processing many images. Each gdal2tiles job
        itself uses multiple processes for internal parallelism.

    Error Handling:
        - Non-zero return from gdal2tiles raises RuntimeError
        - Temporary directories are cleaned up on failure
        - Existing tile directories are removed before atomic rename
    """

    # Limit concurrent tile generation to avoid CPU/memory saturation
    MAX_CONCURRENT_TILES = 2
    # Processes per gdal2tiles job
    GDAL_PROCESSES = 2

    def __init__(self, geotiff_files: list[Path], output_dir: Path):
        self._geotiff_files = geotiff_files
        self._output_dir = output_dir

    async def run(self):
        """Generate tiles for all GeoTIFF files with bounded concurrency."""
        self._output_dir.mkdir(parents=True, exist_ok=True)

        items = {path.name: path for path in self._geotiff_files}
        await run_concurrently(
            items=items,
            worker_fn=lambda _name, path: run_gdal2tiles(
                path, self._output_dir, processes=self.GDAL_PROCESSES
            ),
            max_concurrency=self.MAX_CONCURRENT_TILES,
            task_name="Tile generation",
        )
