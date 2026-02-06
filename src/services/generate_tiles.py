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

import asyncio
import logging
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


class GenerateTilesService:
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
        Uses asyncio.Semaphore to limit concurrent tile generation, preventing
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

    def __init__(self, geotiff_files: List[Path], output_dir: Path):
        self._geotiff_files = geotiff_files
        self._output_dir = output_dir
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_TILES)

    async def run(self):
        """
        Async Concurrency Pattern: Semaphore + to_thread + gather.

        Tile generation uses subprocess calls to gdal2tiles.py, which is both
        CPU-intensive and spawns its own child processes (--processes=2).

        Concurrency Design:
            - MAX_CONCURRENT_TILES=2: Only 2 gdal2tiles jobs run at once
            - GDAL_PROCESSES=2: Each gdal2tiles job uses 2 internal processes
            - Total parallelism: 2 * 2 = 4 CPU cores utilized

        Why lower concurrency than other services:
            - gdal2tiles is already multi-process internally
            - Each job writes many small tile files (I/O intensive)
            - subprocess.run blocks until gdal2tiles completes
            - Memory usage per job is moderate but tile I/O can saturate disk
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        tasks = []

        # Schedule all tile generation tasks
        for geotiff_path in self._geotiff_files:
            tasks.append(self._generate_tiles_with_limit(geotiff_path))

        # Execute with semaphore-controlled parallelism
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect and report failures
        failed = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed.append((self._geotiff_files[i].name, result))

        if failed:
            for name, err in failed:
                logger.error(f"Tile generation failed for {name}: {err}")
            raise RuntimeError(
                f"Tile generation failed for {len(failed)}/{len(tasks)} files"
            )

    async def _generate_tiles_with_limit(self, geotiff_path: Path):
        """
        Semaphore-bounded task wrapper.

        The semaphore (MAX_CONCURRENT_TILES=2) prevents too many gdal2tiles
        processes from running simultaneously. Since gdal2tiles itself uses
        multiprocessing (--processes=2), we keep the outer limit low.
        """
        async with self._semaphore:
            # Run subprocess in thread pool to avoid blocking event loop
            await asyncio.to_thread(self._generate_tiles, geotiff_path)

    def _generate_tiles(self, geotiff_path: Path):
        # 1. Define tile output directory
        # Structure: output_dir / <geotiff_name>_tiles
        tiles_output_dir = self._output_dir / f"{geotiff_path.stem}_tiles"

        # Temporary directory for atomic operation
        tmp_tiles_dir = self._output_dir / str(uuid.uuid4())
        tmp_tiles_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 2. Run gdal2tiles
            cmd = [
                "gdal2tiles.py",
                "-z",
                "3-7",
                "-w",
                "none",  # No web viewer needed
                "--xyz",  # Use XYZ tile scheme (OSM/Slippy map standard) instead of TMS
                "--tiledriver=WEBP",
                f"--processes={self.GDAL_PROCESSES}",
                str(geotiff_path),
                str(tmp_tiles_dir),
            ]

            logger.info(f"Generating tiles for {geotiff_path.name}...")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
                check=False,  # Error handled manually
            )

            if result.returncode != 0:
                logger.error(f"gdal2tiles failed: {result.stderr}")
                raise RuntimeError(f"gdal2tiles failed for {geotiff_path.name}")

            # 3. Atomically move tiles to final destination
            if tiles_output_dir.exists():
                shutil.rmtree(tiles_output_dir)

            tmp_tiles_dir.rename(tiles_output_dir)
            logger.info(f"Tiles generated successfully: {tiles_output_dir}")

        except Exception as e:
            logger.error(f"Error generating tiles for {geotiff_path.name}: {e}")
            if tmp_tiles_dir.exists():
                shutil.rmtree(tmp_tiles_dir)
            raise
