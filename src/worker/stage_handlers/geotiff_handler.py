"""GeoTIFF stage handler - creates colorized RGBA GeoTIFF files."""

import gc
import logging
import pickle
import uuid
from pathlib import Path
from typing import Tuple

import numpy as np
import xarray as xr

from config import Config
from models.work_unit import WorkUnit
from services.generate_geotiff_files import GenerateGeoTIFFFilesService
from worker.stage_handlers.base_handler import BaseStageHandler

logger = logging.getLogger(__name__)


class GeoTIFFHandler(BaseStageHandler):
    """
    Handler for the GEOTIFF stage.

    Creates colorized RGBA GeoTIFF files from brightness temperature data.
    Includes reprojection, clipping, and color palette application.

    Input: work_unit.paths.temp_data
    Output: work_unit.paths.geotiff (path to GeoTIFF file)
    """

    def __init__(self, config: Config):
        super().__init__(config)

    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """Generate GeoTIFF from brightness temperature data."""
        logger.info(f"[GEOTIFF] Starting for {work_unit.image_id}")

        if not work_unit.paths.temp_data:
            raise ValueError("temp_data path is required for GEOTIFF stage")

        # Prepare output directory
        geotiff_dir = self._ensure_dir(self._get_band_dir(work_unit) / "geotiff")

        # Load brightness temperature data
        temp_path = Path(work_unit.paths.temp_data)
        if not temp_path.exists():
            raise FileNotFoundError(f"Temp file not found: {temp_path}")

        with open(temp_path, "rb") as f:
            bt_data = pickle.load(f)

        # Get band configuration
        band_config = work_unit.band_config

        # Get color palette based on band
        if band_config.palette_name == "WATER_VAPOR_PALETTE":
            color_palette = GenerateGeoTIFFFilesService.WATER_VAPOR_PALETTE
        else:
            color_palette = GenerateGeoTIFFFilesService.CLOUD_TOPS_PALETTE

        # Generate GeoTIFF (synchronous, wrapped for consistency)
        import asyncio

        output_path = await asyncio.to_thread(
            self._generate_geotiff,
            bt_data,
            geotiff_dir,
            work_unit.image_id,
            work_unit.bounds,
            band_config.vmin,
            band_config.vmax,
            band_config.product_name,
            color_palette,
        )

        # Clean up
        del bt_data
        gc.collect()

        logger.info(f"[GEOTIFF] Saved to {output_path}")

        # Update work unit
        work_unit.paths.geotiff = str(output_path)

        return work_unit

    def _generate_geotiff(
        self,
        bt_data: xr.DataArray,
        output_dir: Path,
        image_id: str,
        bounds: dict,
        vmin: float,
        vmax: float,
        product_name: str,
        color_palette: list,
    ) -> Path:
        """Generate a colorized RGBA GeoTIFF from brightness temperature data."""
        # Remove grid_mapping if present
        if "grid_mapping" in bt_data.attrs:
            del bt_data.attrs["grid_mapping"]

        # 1. Reproject to EPSG:4326
        bt_reproj = bt_data.rio.reproject("EPSG:4326")

        # Fix nodata value before clipping
        bt_reproj = bt_reproj.rio.write_nodata(np.nan, inplace=False)

        # 2. Clip to configured bounds
        bt_clipped = bt_reproj.rio.clip_box(
            minx=bounds["minx"],
            miny=bounds["miny"],
            maxx=bounds["maxx"],
            maxy=bounds["maxy"],
        )

        del bt_reproj
        gc.collect()

        # Get coordinates for later use
        coords_x = bt_clipped["x"]
        coords_y = bt_clipped["y"]

        # 3. Normalize and apply color palette
        r, g, b, a = self._normalize_with_palette(bt_clipped, vmin, vmax, color_palette)

        del bt_clipped
        gc.collect()

        # 4. Create RGBA DataArray
        rgb = xr.DataArray(
            np.stack([r, g, b, a]),
            dims=["band", "y", "x"],
            coords={"band": [1, 2, 3, 4], "x": coords_x, "y": coords_y},
            name=product_name,
        )

        del r, g, b, a
        gc.collect()

        # Set CRS and spatial dims
        rgb.rio.write_crs("EPSG:4326", inplace=True)
        rgb.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        # 5. Save to GeoTIFF with atomic write
        # Extract stem from image_id (remove .nc if present)
        stem = Path(image_id).stem
        output_path = output_dir / f"{stem}.tif"
        tmp_path = output_dir / f"{uuid.uuid4()}.tif"

        try:
            rgb.rio.to_raster(tmp_path)
            tmp_path.rename(output_path)
        except Exception as e:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        del rgb
        gc.collect()

        return output_path

    def _normalize_with_palette(
        self,
        array: xr.DataArray,
        vmin: float,
        vmax: float,
        color_palette: list,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Normalize array and apply color palette. Returns R, G, B, A arrays."""
        arr = np.asarray(
            array.values if hasattr(array, "values") else array, dtype=np.float32
        )

        nan_mask = np.isnan(arr)

        # Create alpha channel: 0 where NaN, 255 otherwise
        alpha = np.where(nan_mask, 0, 255).astype(np.uint8)

        # Normalize to [0, 1]
        normalized = (arr - vmin) / (vmax - vmin)
        normalized = np.clip(normalized, 0, 1)
        normalized = np.nan_to_num(normalized, nan=0.0)
        del arr

        # Convert to palette indices
        indices = (normalized * 255).astype(np.uint8)
        del normalized

        # Build RGB palette lookup table
        rgb_palette = np.zeros((256, 3), dtype=np.uint8)
        for i, hex_color in enumerate(color_palette):
            hex_color = hex_color.lstrip("#")
            rgb_palette[i, 0] = int(hex_color[0:2], 16)
            rgb_palette[i, 1] = int(hex_color[2:4], 16)
            rgb_palette[i, 2] = int(hex_color[4:6], 16)

        # Apply palette
        colored = rgb_palette[indices]
        del indices

        # Set NaN areas to first palette color
        colored[nan_mask] = rgb_palette[0]
        del nan_mask
        gc.collect()

        return colored[..., 0], colored[..., 1], colored[..., 2], alpha
