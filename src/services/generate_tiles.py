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
    def __init__(self, geotiff_files: List[Path], output_dir: Path):
        self._geotiff_files = geotiff_files
        self._output_dir = output_dir

    async def run(self):
        self._output_dir.mkdir(parents=True, exist_ok=True)
        tasks = []

        for geotiff_path in self._geotiff_files:
            tasks.append(asyncio.to_thread(self._generate_tiles, geotiff_path))

        await asyncio.gather(*tasks)

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
                "--processes=2",  # Adjust based on available cores per job
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
