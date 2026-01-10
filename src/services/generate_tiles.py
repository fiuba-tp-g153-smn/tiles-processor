import asyncio
import logging
import os
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


class GenerateTilesService:
    # Limit concurrent tile generation to avoid CPU/memory saturation
    MAX_CONCURRENT_TILES = 2
    # Processes per gdal2tiles job
    GDAL_PROCESSES = 2

    def __init__(self, geotiff_files: List[Path], output_dir: Path):
        self._geotiff_files = geotiff_files
        self._output_dir = output_dir
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_TILES)

    async def run(self):
        self._output_dir.mkdir(parents=True, exist_ok=True)
        tasks = []

        for geotiff_path in self._geotiff_files:
            tasks.append(self._generate_tiles_with_limit(geotiff_path))

        await asyncio.gather(*tasks)

    async def _generate_tiles_with_limit(self, geotiff_path: Path):
        async with self._semaphore:
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
                "leaflet",
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
