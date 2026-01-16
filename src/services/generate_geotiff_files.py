"""
GeoTIFF Generation Service.

This service creates colorized RGBA GeoTIFF files from brightness temperature
data for web visualization. It handles reprojection, clipping, and color mapping.

Processing Steps:
    1. Reproject from GOES geostationary to EPSG:4326 (lat/lon)
    2. Clip to configured bounding box (reduces file size significantly)
    3. Normalize temperature values to 0-255 range
    4. Apply color palette lookup to create RGB values
    5. Create alpha channel (transparent for NaN/no-data)
    6. Save as RGBA GeoTIFF with atomic write pattern

Color Palettes:
    - CLOUD_TOPS_PALETTE: Grayscale → Red (256 colors) for Band 13
      Cold cloud tops (low temps) appear red, warm surfaces appear gray
    - WATER_VAPOR_PALETTE: Maroon → Blue (256 colors, SMN style) for Band 9
      Dry air (low temps) appears maroon, moist air appears blue

Atomic Writes:
    Files are written to a temporary UUID-named file, then atomically renamed
    to the final destination. This prevents corrupted files if processing fails.

File Overwrites:
    If a file with the same name exists, it is atomically replaced.
    GOES filenames include timestamps, so same-name = same data (idempotent).
"""

import asyncio
import gc
import logging
import uuid
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import xarray as xr
import rioxarray

from config import Config

# Note: Ensure you have rioxarray installed to use the rio accessor

logger = logging.getLogger(__name__)


class GenerateGeoTIFFFilesService:
    """
    Generates colorized RGBA GeoTIFF files from brightness temperature data.

    This service takes brightness temperature DataArrays and creates web-ready
    GeoTIFF files with custom color palettes for visualization.

    Processing pipeline:
        1. Remove grid_mapping attribute (can cause issues with rioxarray)
        2. Reproject to EPSG:4326 (WGS84 lat/lon)
        3. Clip to configured bounds (from config.get_bounds())
        4. Normalize temperatures to [vmin, vmax] → [0, 255]
        5. Apply color palette via index lookup
        6. Create alpha channel (255=opaque, 0=transparent for NaN)
        7. Stack into RGBA DataArray
        8. Write to GeoTIFF with atomic rename

    Args:
        brightness_temperatures: Dict mapping filenames to temperature DataArrays
        output_dir: Directory for output GeoTIFF files
        color_palette: List of 256 hex color strings (default: CLOUD_TOPS_PALETTE)
        vmin: Minimum temperature for normalization (default: 183.15K = -90°C)
        vmax: Maximum temperature for normalization (default: 323.15K = +50°C)
        product_name: Name for the output DataArray (default: "Cloud_Tops")

    Returns:
        List of Path objects pointing to generated GeoTIFF files

    Memory Management:
        Explicit gc.collect() and del statements are used throughout to
        manage memory when processing large satellite imagery arrays.
        This is critical for processing multiple images without memory exhaustion.
    """

    # Default bounding box (Argentina with margin)
    DEFAULT_BOUNDS = {
        "minx": -90.0,  # 90°W - Pacific (includes Chile, Peru)
        "miny": -60.0,  # 60°S - Further south (ocean/Antarctica)
        "maxx": -30.0,  # 30°W - Middle of Atlantic
        "maxy": -15.0,  # 15°S - North (Bolivia/Brazil)
    }

    # Palette for Band 9 - Water Vapor (Mid-Level Water Vapor)
    # Temperature range: -112.15°C to 56.85°C
    WATER_VAPOR_PALETTE = [
        "#ffffff",
        "#ffffff",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#000032",
        "#616161",
        "#757575",
        "#757575",
        "#898989",
        "#9d9d9d",
        "#9d9d9d",
        "#b0b0b0",
        "#c4c4c4",
        "#c4c4c4",
        "#d8d8d8",
        "#ececec",
        "#ececec",
        "#ffffff",
        "#4f4f50",
        "#4f4f50",
        "#636347",
        "#ffffff",
        "#ffffff",
        "#9b9b2d",
        "#acac25",
        "#acac25",
        "#bdbd1e",
        "#cece16",
        "#cece16",
        "#dede0f",
        "#efef07",
        "#efef07",
        "#ffff00",
        "#ef0000",
        "#ef0000",
        "#d50000",
        "#c80000",
        "#c80000",
        "#bc0000",
        "#af0000",
        "#af0000",
        "#a30000",
        "#960000",
        "#960000",
        "#890000",
        "#7d0000",
        "#7d0000",
        "#700000",
        "#640000",
        "#640000",
        "#00ee00",
        "#00de00",
        "#00de00",
        "#00cf00",
        "#00bf00",
        "#00bf00",
        "#00b000",
        "#00a100",
        "#00a100",
        "#009100",
        "#008200",
        "#008200",
        "#007300",
        "#006400",
        "#006400",
        "#0000ff",
        "#0000ef",
        "#0000ef",
        "#0000e0",
        "#0000d0",
        "#0000d0",
        "#0000c1",
        "#0000c1",
        "#0000b1",
        "#0000a2",
        "#0000a2",
        "#000092",
        "#000083",
        "#000083",
        "#000073",
        "#000064",
        "#000064",
        "#4d7fb1",
        "#5587b9",
        "#5587b9",
        "#5d8fc1",
        "#6496c8",
        "#6496c8",
        "#ebebeb",
        "#e2e2e2",
        "#e2e2e2",
        "#d9d9d9",
        "#d1d1d1",
        "#d1d1d1",
        "#c8c8c8",
        "#c0c0c0",
        "#c0c0c0",
        "#b7b7b7",
        "#aeaeae",
        "#aeaeae",
        "#a6a6a6",
        "#9d9d9d",
        "#9d9d9d",
        "#959595",
        "#8c8c8c",
        "#8c8c8c",
        "#848484",
        "#7d7d7d",
        "#7d7d7d",
        "#737373",
        "#646464",
        "#646464",
        "#525252",
        "#434343",
        "#434343",
        "#383838",
        "#2d2d2d",
        "#2d2d2d",
        "#232323",
        "#4b0000",
        "#4b0000",
        "#651300",
        "#7f2500",
        "#7f2500",
        "#993700",
        "#b24900",
        "#b24900",
        "#cc5b00",
        "#e66d00",
        "#e66d00",
        "#ff7f00",
        "#cb0000",
        "#cb0000",
        "#b60000",
        "#a00000",
        "#a00000",
        "#8b0000",
        "#750000",
        "#750000",
        "#600000",
        "#4b0000",
        "#4b0000",
        "#221313",
        "#471d1d",
        "#471d1d",
        "#6c2626",
        "#913030",
        "#913030",
        "#b63939",
        "#db4242",
        "#db4242",
        "#ff4b4b",
        "#c8c800",
        "#c8c800",
        "#c4c400",
        "#c1c100",
        "#c1c100",
        "#bdbd00",
        "#bdbd00",
        "#baba00",
        "#b6b600",
        "#b6b600",
        "#b3b300",
        "#b0b000",
        "#b0b000",
        "#acac00",
        "#a9a900",
        "#a9a900",
        "#a5a500",
        "#a2a200",
        "#a2a200",
        "#9e9e00",
        "#9b9b00",
        "#9b9b00",
        "#989800",
        "#949400",
        "#949400",
        "#919100",
        "#8d8d00",
        "#8d8d00",
        "#8a8a00",
        "#878700",
        "#878700",
        "#838300",
        "#808000",
        "#808000",
        "#7c7c00",
        "#797900",
        "#797900",
        "#757500",
        "#727200",
        "#727200",
        "#6f6f00",
        "#6b6b00",
        "#6b6b00",
        "#686800",
        "#646400",
        "#646400",
        "#616100",
        "#5d5d00",
        "#5d5d00",
        "#5a5a00",
        "#575700",
        "#575700",
        "#535300",
        "#505000",
        "#505000",
        "#4c4c00",
        "#494900",
        "#494900",
        "#4b4b00",
        "#000000",
        "#000000",
        "#000000",
        "#000000",
        "#000000",
        "#000000",
        "#000000",
        "#000000",
        "#313100",
        "#2d2d00",
        "#2d2d00",
        "#2a2a00",
        "#262600",
        "#262600",
        "#232300",
        "#1f1f00",
        "#1f1f00",
        "#1c1c00",
        "#181800",
        "#181800",
        "#151500",
        "#111100",
        "#111100",
        "#0e0e00",
        "#0a0a00",
        "#0a0a00",
        "#070700",
        "#030300",
        "#030300",
        "#000000",
        "#000000",
    ]

    # Palette for Band 13 - Cloud Tops
    CLOUD_TOPS_PALETTE = [
        "#ffffff",
        "#f2f2f2",
        "#e5e5e5",
        "#d7d7d7",
        "#cacaca",
        "#bcbcbc",
        "#afafaf",
        "#a2a2a2",
        "#949494",
        "#878787",
        "#797979",
        "#6c6c6c",
        "#5e5e5e",
        "#515151",
        "#444444",
        "#363636",
        "#292929",
        "#1b1b1b",
        "#000000",
        "#110000",
        "#220000",
        "#330000",
        "#440000",
        "#550000",
        "#660000",
        "#770000",
        "#880000",
        "#990000",
        "#aa0000",
        "#bb0000",
        "#cc0000",
        "#dd0000",
        "#ee0000",
        "#ff0b00",
        "#ff1600",
        "#ff2100",
        "#ff2c00",
        "#ff3700",
        "#ff4200",
        "#ff4d00",
        "#ff5800",
        "#ff6300",
        "#ff6e00",
        "#ff7900",
        "#ff8500",
        "#ff9000",
        "#ff9b00",
        "#ffa600",
        "#ffb100",
        "#ffbc00",
        "#ffc700",
        "#ffd200",
        "#ffdd00",
        "#ffe800",
        "#fff300",
        "#f0ff00",
        "#e0ff00",
        "#d0ff00",
        "#c0ff00",
        "#b0ff00",
        "#a0ff00",
        "#90ff00",
        "#80ff00",
        "#70ff00",
        "#60ff00",
        "#50ff00",
        "#40ff00",
        "#30ff00",
        "#20ff00",
        "#10ff00",
        "#00f007",
        "#00e00e",
        "#00d015",
        "#00c01c",
        "#00b023",
        "#00a02b",
        "#009032",
        "#008039",
        "#007040",
        "#006047",
        "#00504f",
        "#004056",
        "#00305d",
        "#002064",
        "#00106b",
        "#000b79",
        "#00177f",
        "#002286",
        "#002e8c",
        "#003992",
        "#004599",
        "#00519f",
        "#005ca5",
        "#0068ac",
        "#0073b2",
        "#007fb9",
        "#008bbf",
        "#0096c5",
        "#00a2cc",
        "#00add2",
        "#00b9d8",
        "#00c5df",
        "#00d0e5",
        "#00dceb",
        "#00e7f2",
        "#00f3f8",
        "#fafafa",
        "#f8f8f8",
        "#f7f7f7",
        "#f5f5f5",
        "#f3f3f3",
        "#f2f2f2",
        "#f0f0f0",
        "#eeeeee",
        "#ededed",
        "#ebebeb",
        "#e9e9e9",
        "#e8e8e8",
        "#e6e6e6",
        "#e4e4e4",
        "#e2e2e2",
        "#e1e1e1",
        "#dfdfdf",
        "#dddddd",
        "#dcdcdc",
        "#dadada",
        "#d8d8d8",
        "#d7d7d7",
        "#d5d5d5",
        "#d3d3d3",
        "#d2d2d2",
        "#d0d0d0",
        "#cecece",
        "#cdcdcd",
        "#cbcbcb",
        "#c9c9c9",
        "#c8c8c8",
        "#c6c6c6",
        "#c4c4c4",
        "#c3c3c3",
        "#c1c1c1",
        "#bfbfbf",
        "#bebebe",
        "#bcbcbc",
        "#bababa",
        "#b9b9b9",
        "#b7b7b7",
        "#b5b5b5",
        "#b4b4b4",
        "#b2b2b2",
        "#b0b0b0",
        "#aeaeae",
        "#adadad",
        "#ababab",
        "#a9a9a9",
        "#a8a8a8",
        "#a6a6a6",
        "#a4a4a4",
        "#a3a3a3",
        "#a1a1a1",
        "#9f9f9f",
        "#9e9e9e",
        "#9c9c9c",
        "#9a9a9a",
        "#999999",
        "#979797",
        "#959595",
        "#949494",
        "#929292",
        "#909090",
        "#8f8f8f",
        "#8d8d8d",
        "#8b8b8b",
        "#8a8a8a",
        "#888888",
        "#868686",
        "#858585",
        "#838383",
        "#818181",
        "#808080",
        "#7e7e7e",
        "#7c7c7c",
        "#7a7a7a",
        "#797979",
        "#777777",
        "#757575",
        "#747474",
        "#727272",
        "#707070",
        "#6f6f6f",
        "#6d6d6d",
        "#6b6b6b",
        "#6a6a6a",
        "#686868",
        "#666666",
        "#656565",
        "#636363",
        "#616161",
        "#606060",
        "#5e5e5e",
        "#5c5c5c",
        "#5b5b5b",
        "#595959",
        "#575757",
        "#565656",
        "#545454",
        "#525252",
        "#515151",
        "#4f4f4f",
        "#4d4d4d",
        "#4b4b4b",
        "#4a4a4a",
        "#484848",
        "#464646",
        "#454545",
        "#434343",
        "#414141",
        "#404040",
        "#3e3e3e",
        "#3c3c3c",
        "#3b3b3b",
        "#393939",
        "#373737",
        "#363636",
        "#343434",
        "#323232",
        "#313131",
        "#2f2f2f",
        "#2d2d2d",
        "#2c2c2c",
        "#2a2a2a",
        "#282828",
        "#272727",
        "#252525",
        "#232323",
        "#222222",
        "#202020",
        "#1e1e1e",
        "#1d1d1d",
        "#1b1b1b",
        "#191919",
        "#171717",
        "#161616",
        "#141414",
        "#121212",
        "#111111",
        "#0f0f0f",
        "#0d0d0d",
        "#0c0c0c",
        "#0a0a0a",
        "#080808",
        "#070707",
        "#050505",
        "#030303",
        "#020202",
        "#000000",
    ]

    def __init__(
        self,
        brightness_temperatures: Dict[str, xr.DataArray],
        output_dir: Path,
        config: Config,
        color_palette: List[str] = None,
        vmin: float = 183.15,
        vmax: float = 323.15,
        product_name: str = "Cloud_Tops",
        max_concurrency: int = 4,
    ):
        self._brightness_temperatures = brightness_temperatures
        self._output_dir = output_dir
        self._config = config
        self._color_palette = color_palette or self.CLOUD_TOPS_PALETTE
        self._vmin = vmin
        self._vmax = vmax
        self._product_name = product_name
        self._max_concurrency = max_concurrency

    async def run(self) -> List[Path]:
        """
        Async Concurrency Pattern: Semaphore + to_thread + gather.

        Same pattern as ComputeBrightnessTemperaturesService, optimized for
        CPU-bound GeoTIFF generation (reprojection, clipping, colorization).

        Why this pattern works well for GeoTIFF generation:
            - _generate_geotiff is CPU-intensive (numpy, rioxarray operations)
            - Memory usage is significant per file (~100MB+ during reprojection)
            - Semaphore(4) balances parallelism vs memory consumption
            - Thread pool allows event loop to remain responsive

        Key async components:
            - bounded_generation: Wrapper that acquires semaphore before execution
            - asyncio.to_thread: Offloads blocking I/O and CPU work to threads
            - asyncio.gather: Coordinates all tasks, collects results/exceptions
        """
        import logging

        logger = logging.getLogger(__name__)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        tasks = []
        file_names = []

        # Limit concurrent GeoTIFF generation to control memory usage
        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def bounded_generation(file_name, dataset):
            # Semaphore ensures only max_concurrency tasks run simultaneously
            async with semaphore:
                # Thread pool execution prevents event loop blocking during
                # heavy numpy/rioxarray operations (reproject, clip, colorize)
                return await asyncio.to_thread(
                    self._generate_geotiff, file_name, dataset
                )

        # Schedule all tasks (semaphore controls actual concurrency)
        for file_name, dataset in self._brightness_temperatures.items():
            file_names.append(file_name)
            tasks.append(bounded_generation(file_name, dataset))

        # Run all tasks, collecting exceptions rather than failing fast
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Partition results into successes and failures
        successful = []
        failed = []

        for file_name, result in zip(file_names, results):
            if isinstance(result, Exception):
                failed.append((file_name, result))
            else:
                successful.append(result)

        # Aggregate error reporting for better debugging
        if failed:
            for name, err in failed:
                logger.error(f"GeoTIFF generation failed for {name}: {err}")
            raise RuntimeError(
                f"GeoTIFF generation failed for {len(failed)}/{len(tasks)} files"
            )

        return successful

    def _generate_geotiff(self, file_name: str, c13_data: xr.DataArray) -> Path:
        # Remove grid_mapping if present
        if "grid_mapping" in c13_data.attrs:
            del c13_data.attrs["grid_mapping"]

        # 1. Reproject to EPSG:4326
        # Use rioxarray's reproject method. Ensure rioxarray is installed and imported through xarray accessor
        c13_reproj = c13_data.rio.reproject("EPSG:4326")

        # Fix nodata value before clipping (original value is too large for float32)
        c13_reproj = c13_reproj.rio.write_nodata(np.nan, inplace=False)

        # 2. Clip to configured bounds to reduce processing area
        bounds = self._config.get_bounds()
        c13_clipped = c13_reproj.rio.clip_box(
            minx=bounds["minx"],
            miny=bounds["miny"],
            maxx=bounds["maxx"],
            maxy=bounds["maxy"],
        )

        # Free memory from full reprojection
        del c13_reproj
        gc.collect()

        # Get coordinates for later use
        coords_x = c13_clipped["x"]
        coords_y = c13_clipped["y"]

        # 3. Normalize and apply custom palette (Cloud Tops logic)
        # vmin=183.15, vmax=323.15 from legacy code
        r, g, b, a = self._normalize_with_custom_palette(
            c13_clipped, vmin=self._vmin, vmax=self._vmax
        )

        # Free memory
        del c13_clipped
        gc.collect()

        # 3. Create RGBA DataArray
        rgb = xr.DataArray(
            np.stack([r, g, b, a]),
            dims=["band", "y", "x"],
            coords={"band": [1, 2, 3, 4], "x": coords_x, "y": coords_y},
            name=self._product_name,
        )

        # Free memory
        del r, g, b, a
        # 4. Set CRS and spatial dims
        rgb.rio.write_crs("EPSG:4326", inplace=True)
        rgb.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)

        # 5. Save to GeoTIFF
        # Construct output path, ensuring .tif extension
        output_filename = f"{Path(file_name).stem}.tif"
        output_path = self._output_dir / output_filename

        # Atomic write
        tmp_output_path = self._output_dir / f"{str(uuid.uuid4())}.tif"
        try:
            rgb.rio.to_raster(tmp_output_path)
            tmp_output_path.rename(output_path)
            logger.info(f"Generated GeoTIFF: {output_path}")
        except Exception as e:
            logger.error(f"Failed to generate GeoTIFF for {file_name}: {e}")
            if tmp_output_path.exists():
                tmp_output_path.unlink()
            raise

        del rgb
        gc.collect()

        return output_path

    def _normalize_with_custom_palette(
        self, array: xr.DataArray, vmin: float, vmax: float
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Normalize an array and apply the custom color palette.
        Returns: R, G, B, A (uint8 arrays)
        """
        arr = np.asarray(
            array.values if hasattr(array, "values") else array, dtype=np.float32
        )

        nan_mask = np.isnan(arr)

        # Create alpha channel: 0 where NaN, 255 otherwise
        alpha = np.where(nan_mask, 0, 255).astype(np.uint8)

        normalized = (arr - vmin) / (vmax - vmin)
        normalized = np.clip(normalized, 0, 1)
        normalized = np.nan_to_num(normalized, nan=0.0)
        del arr

        indices = (normalized * 255).astype(np.uint8)
        del normalized

        rgb_palette = np.zeros((256, 3), dtype=np.uint8)
        for i, hex_color in enumerate(self._color_palette):
            hex_color = hex_color.lstrip("#")
            rgb_palette[i, 0] = int(hex_color[0:2], 16)
            rgb_palette[i, 1] = int(hex_color[2:4], 16)
            rgb_palette[i, 2] = int(hex_color[4:6], 16)

        colored = rgb_palette[indices]
        del indices

        # We don't strictly need to set colored[nan_mask] to a specific color
        # because alpha will be 0, but keeping it black/white is fine.
        colored[nan_mask] = rgb_palette[0]
        del nan_mask
        gc.collect()

        # Extract channels
        red = colored[..., 0]
        green = colored[..., 1]
        blue = colored[..., 2]

        return red, green, blue, alpha
