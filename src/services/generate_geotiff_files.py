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

import gc
import logging
import uuid
from pathlib import Path

import numpy as np
import xarray as xr

from config import Config
from services.concurrent_runner import run_concurrently
from services.processing_steps import build_rgba_data_array, normalize_and_colorize

logger = logging.getLogger(__name__)


def _interpolate_palette(  # pylint: disable=too-many-locals
    control_points: list[tuple[int, int, int, int]],
) -> list[str]:
    """Build a 256-entry hex palette by linearly interpolating between control points.

    Args:
        control_points: List of (index, r, g, b) tuples, index in 0–255.
                        Must start at index 0 and end at index 255.
    """
    result = []
    for i in range(256):
        for j in range(len(control_points) - 1):
            idx0, r0, g0, b0 = control_points[j]
            idx1, r1, g1, b1 = control_points[j + 1]
            if idx0 <= i <= idx1:
                t = (i - idx0) / (idx1 - idx0) if idx1 != idx0 else 0.0
                r = round(r0 + t * (r1 - r0))
                g = round(g0 + t * (g1 - g0))
                b = round(b0 + t * (b1 - b0))
                result.append(f"#{r:02x}{g:02x}{b:02x}")
                break
    return result


class GenerateGeoTIFFFilesService:  # pylint: disable=too-few-public-methods
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
        "minx": -110.0,  # 110°W - Pacific (includes Chile, Peru)
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

    # Visible band grayscale palette (black → white, 256 entries)
    # Low reflectance (surface/ocean) = dark, high reflectance (clouds) = white
    VISIBLE_PALETTE = [f"#{i:02x}{i:02x}{i:02x}" for i in range(256)]

    # Palette for GLM Flash Extent Density (FED)
    # Range: 0–256 flashes per grid cell
    # Ticks: 1, 2, 4, 8, 16, 32, 64, 128, 256
    FED_PALETTE = _interpolate_palette(
        [
            (0, 0, 0, 139),  # 1 flash (Index 0): Dark navy
            (1, 0, 0, 255),  # 2 flashes (Index 1): Blue
            (3, 0, 191, 255),  # 4 flashes (Index 3): Light blue
            (7, 0, 255, 0),  # 8 flashes (Index 7): Green
            (15, 173, 255, 47),  # 16 flashes (Index 15): Green-yellow
            (31, 255, 255, 0),  # 32 flashes (Index 31): Yellow
            (63, 255, 165, 0),  # 64 flashes (Index 63): Orange
            (127, 255, 0, 0),  # 128 flashes (Index 127): Red
            (255, 255, 255, 255),  # 256 flashes (Index 255): White
        ]
    )

    # Backward-compat alias — use FED_PALETTE in new code
    LIGHTNING_PALETTE = FED_PALETTE

    # Palette for GLM Total Optical Energy (TOE)
    # Range: 0–1500 fJ per grid cell
    # Ticks: 1, 5, 10, 25, 50, 100, 500, 1500 fJ
    TOE_PALETTE = _interpolate_palette(
        [
            (0, 75, 0, 130),  # 1-5 fJ (Index 0): Dark purple
            (1, 0, 0, 128),  # 10 fJ (Index 1): Dark blue
            (4, 0, 0, 255),  # 25 fJ (Index 4): Blue
            (8, 255, 105, 180),  # 50 fJ (Index 8): Hot Pink
            (17, 255, 0, 255),  # 100 fJ (Index 17): Magenta
            (85, 255, 165, 0),  # 500 fJ (Index 85): Orange
            (170, 255, 255, 0),  # 1000 fJ (Index 170): Bright yellow
            (255, 255, 255, 255),  # 1500 fJ (Index 255): White
        ]
    )

    # Palette for GLM Minimum Flash Area (MFA)
    # Range: 0–3000 km² per grid cell
    # Ticks: ~60, 120, 300, 600, 1200, 2000, 3000 km²
    MFA_PALETTE = _interpolate_palette(
        [
            (0, 255, 255, 0),  # ~0 km²:    Yellow (placeholder; 0→NaN→transparent)
            (5, 255, 255, 0),  # ~60 km²:   Bright Yellow (strong updraft)
            (10, 255, 200, 0),  # ~120 km²:  Yellow-Orange
            (26, 0, 255, 0),  # ~300 km²:  Green
            (51, 0, 128, 255),  # ~600 km²:  Light Blue
            (102, 0, 0, 255),  # ~1200 km²: Blue
            (170, 128, 0, 255),  # ~2000 km²: Purple
            (255, 255, 0, 255),  # ~3000 km²: Magenta
        ]
    )

    @classmethod
    def get_palette(cls, name: str) -> list[str]:
        """Look up a color palette by its BandConfig palette_name string."""
        palettes = {
            "FED_PALETTE": cls.FED_PALETTE,
            "TOE_PALETTE": cls.TOE_PALETTE,
            "MFA_PALETTE": cls.MFA_PALETTE,
            "LIGHTNING_PALETTE": cls.FED_PALETTE,
            "CLOUD_TOPS_PALETTE": cls.CLOUD_TOPS_PALETTE,
            "WATER_VAPOR_PALETTE": cls.WATER_VAPOR_PALETTE,
            "VISIBLE_PALETTE": cls.VISIBLE_PALETTE,
        }
        if name not in palettes:
            raise ValueError(f"Unknown palette '{name}'. Valid: {list(palettes)}")
        return palettes[name]

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        brightness_temperatures: dict[str, xr.DataArray],
        output_dir: Path,
        config: Config,
        color_palette: list[str] | None = None,
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

    async def run(self) -> list[Path]:
        """Generate GeoTIFF files with bounded concurrency."""
        self._output_dir.mkdir(parents=True, exist_ok=True)

        results = await run_concurrently(
            items=self._brightness_temperatures,
            worker_fn=self._generate_geotiff,
            max_concurrency=self._max_concurrency,
            task_name="GeoTIFF generation",
        )
        return list(results.values())

    def _generate_geotiff(  # pylint: disable=too-many-locals
        self, file_name: str, c13_data: xr.DataArray
    ) -> Path:
        # Lazy import to reduce idle memory footprint (registers .rio accessor)
        import rioxarray  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import

        # Remove grid_mapping if present
        if "grid_mapping" in c13_data.attrs:
            del c13_data.attrs["grid_mapping"]

        # 1. Reproject to EPSG:4326
        # Use rioxarray's reproject method (requires rioxarray import above)
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
        r, g, b, a = normalize_and_colorize(
            c13_clipped, self._vmin, self._vmax, self._color_palette
        )

        # Free memory
        del c13_clipped
        gc.collect()

        # 4. Create RGBA DataArray
        rgb = build_rgba_data_array(r, g, b, a, coords_x, coords_y, self._product_name)
        del r, g, b, a

        # 5. Save to GeoTIFF
        # Construct output path, ensuring .tif extension
        output_filename = f"{Path(file_name).stem}.tif"
        output_path = self._output_dir / output_filename

        # Atomic write
        tmp_output_path = self._output_dir / f"{str(uuid.uuid4())}.tif"
        try:
            rgb.rio.to_raster(tmp_output_path)
            tmp_output_path.rename(output_path)
            logger.info("Generated GeoTIFF: %s", output_path)
        except Exception as e:
            logger.error("Failed to generate GeoTIFF for %s: %s", file_name, e)
            if tmp_output_path.exists():
                tmp_output_path.unlink()
            raise

        del rgb
        gc.collect()

        return output_path
