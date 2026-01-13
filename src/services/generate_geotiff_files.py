import asyncio
import gc
import logging
import uuid
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import xarray as xr
import rioxarray

# Note: Ensure you have rioxarray installed to use the rio accessor

logger = logging.getLogger(__name__)


class GenerateGeoTIFFFilesService:
    # Argentina bounding box (with some margin)
    ARGENTINA_BOUNDS = {
        "minx": -90.0,  # 90°W - Pacífico (incluye Chile, Perú)
        "miny": -60.0,  # 60°S - Más al sur (océano/Antártida)
        "maxx": -30.0,  # 30°W - Medio del Atlántico
        "maxy": -15.0,  # 15°S - Norte (Bolivia/Brasil)
    }

    # Paleta para Canal 9 - Water Vapor (Vapor de Agua Niveles Medios)
    # Escala: -90°C (183.15K) a +50°C (323.15K)
    # EXACTAMENTE según SMN: amarillo → verde → cyan → azul → violeta → rojo → naranja → marrón → GRIS
    WATER_VAPOR_PALETTE = [
        # -90°C a -85°C: Amarillo brillante (muy raro) (índices 0-8)
        "#ffff00", "#fffc00", "#fff900", "#fff600", "#fff300", "#fff000", "#ffed00", "#ffea00",
        "#ffe700",
        # -85°C a -75°C: Verde brillante (índices 9-26)
        "#e0ff00", "#d0ff00", "#c0ff00", "#b0ff00", "#a0ff00", "#90ff00", "#80ff00", "#70ff00",
        "#60ff00", "#50ff00", "#40ff00", "#30ff00", "#20ff00", "#10ff00", "#00ff00", "#00ff10",
        "#00ff20", "#00ff30",
        # -75°C a -65°C: Verde a cyan (índices 27-44)
        "#00ff40", "#00ff50", "#00ff60", "#00ff70", "#00ff80", "#00ff90", "#00ffa0", "#00ffb0",
        "#00ffc0", "#00ffd0", "#00ffe0", "#00fff0", "#00ffff", "#00f0ff", "#00e0ff", "#00d0ff",
        "#00c0ff", "#00b0ff",
        # -65°C a -55°C: Cyan a azul claro (índices 45-62)
        "#00a0ff", "#0090ff", "#0080ff", "#0070ff", "#0060ff", "#0050ff", "#0040ff", "#0030ff",
        "#0020ff", "#0010ff", "#0000ff", "#1010ff", "#2020ff", "#3030ff", "#4040ff", "#5050ff",
        "#6060ff", "#7070ff",
        # -55°C a -45°C: Azul a azul oscuro (índices 63-80)
        "#6060f0", "#5555e5", "#4a4ad0", "#4040c0", "#3535b0", "#2a2aa0", "#202090", "#151580",
        "#101070", "#0a0a60", "#050550", "#000050", "#000048", "#000040", "#000038", "#000030",
        "#000538", "#000a40",
        # -45°C a -35°C: Azul oscuro a violeta (índices 81-98)
        "#000f48", "#001450", "#051958", "#0a1e60", "#0f2368", "#142870", "#192d78", "#1e3280",
        "#233788", "#283c90", "#2d4198", "#3246a0", "#5050a8", "#6060b0", "#7070b8", "#8080c0",
        "#9090c8", "#a000a0",
        # -35°C a -25°C: Violeta a rojo oscuro (índices 99-116)
        "#950095", "#8a008a", "#800080", "#750075", "#6a006a", "#600060", "#550555", "#500a50",
        "#4b0f4b", "#461446", "#411941", "#3c1e3c", "#372337", "#322832", "#2d2d2d", "#282020",
        "#401010", "#500000",
        # -25°C a -15°C: Rojo oscuro a rojo (índices 117-134)
        "#600000", "#700000", "#800000", "#900000", "#a00000", "#b00000", "#c00000", "#d00000",
        "#e00000", "#f00000", "#ff0000", "#ff0a00", "#ff1400", "#ff1e00", "#ff2800", "#ff3200",
        "#ff3c00", "#ff4600",
        # -15°C a -5°C: Rojo a naranja (índices 135-152)
        "#ff5000", "#ff5a00", "#ff6400", "#ff6e00", "#ff7800", "#ff8200", "#ff8c00", "#ff9600",
        "#ffa000", "#ffaa00", "#ffb400", "#ffbe00", "#ffc800", "#ffd200", "#ffdc00", "#ffe600",
        "#fff000", "#fffa00",
        # -5°C a 5°C: Naranja a marrón (índices 153-170)
        "#f0e600", "#e0d200", "#d0be00", "#c0aa00", "#b09600", "#a08200", "#906e00", "#805a00",
        "#704600", "#603200", "#503c32", "#404640", "#30504e", "#285a5a", "#206060", "#186666",
        "#106c6c", "#087272",
        # 5°C a 15°C: Marrón-verde a gris oscuro (índices 171-188)
        "#107878", "#187e7e", "#208484", "#288a8a", "#309090", "#389696", "#409c9c", "#48a2a2",
        "#50a8a8", "#58aeae", "#606060", "#606060", "#606060", "#626262", "#646464", "#666666",
        "#686868", "#6a6a6a",
        # 15°C a 30°C: Gris (LA MAYORÍA) (índices 189-224)
        "#6c6c6c", "#6e6e6e", "#707070", "#727272", "#747474", "#767676", "#787878", "#7a7a7a",
        "#7c7c7c", "#7e7e7e", "#808080", "#828282", "#848484", "#868686", "#888888", "#8a8a8a",
        "#8c8c8c", "#8e8e8e", "#909090", "#929292", "#949494", "#969696", "#989898", "#9a9a9a",
        "#9c9c9c", "#9e9e9e", "#a0a0a0", "#a2a2a2", "#a4a4a4", "#a6a6a6", "#a8a8a8", "#aaaaaa",
        "#acacac", "#aeaeae", "#b0b0b0", "#b2b2b2",
        # 30°C a 50°C: Gris claro a blanco (índices 225-255)
        "#b4b4b4", "#b6b6b6", "#b8b8b8", "#bababa", "#bcbcbc", "#bebebe", "#c0c0c0", "#c2c2c2",
        "#c4c4c4", "#c6c6c6", "#c8c8c8", "#cacaca", "#cccccc", "#cecece", "#d0d0d0", "#d2d2d2",
        "#d4d4d4", "#d6d6d6", "#d8d8d8", "#dadada", "#dcdcdc", "#dedede", "#e0e0e0", "#e2e2e2",
        "#e4e4e4", "#e6e6e6", "#e8e8e8", "#eaeaea", "#ececec", "#eeeeee", "#f0f0f0",
    ]

    # Paleta para Canal 13 - Cloud Tops (Topes Nubosos)
    CLOUD_TOPS_PALETTE = [
        "#ffffff", "#f2f2f2", "#e5e5e5", "#d7d7d7", "#cacaca", "#bcbcbc", "#afafaf", "#a2a2a2",
        "#949494", "#878787", "#797979", "#6c6c6c", "#5e5e5e", "#515151", "#444444", "#363636",
        "#292929", "#1b1b1b", "#000000", "#110000", "#220000", "#330000", "#440000", "#550000",
        "#660000", "#770000", "#880000", "#990000", "#aa0000", "#bb0000", "#cc0000", "#dd0000",
        "#ee0000", "#ff0b00", "#ff1600", "#ff2100", "#ff2c00", "#ff3700", "#ff4200", "#ff4d00",
        "#ff5800", "#ff6300", "#ff6e00", "#ff7900", "#ff8500", "#ff9000", "#ff9b00", "#ffa600",
        "#ffb100", "#ffbc00", "#ffc700", "#ffd200", "#ffdd00", "#ffe800", "#fff300", "#f0ff00",
        "#e0ff00", "#d0ff00", "#c0ff00", "#b0ff00", "#a0ff00", "#90ff00", "#80ff00", "#70ff00",
        "#60ff00", "#50ff00", "#40ff00", "#30ff00", "#20ff00", "#10ff00", "#00f007", "#00e00e",
        "#00d015", "#00c01c", "#00b023", "#00a02b", "#009032", "#008039", "#007040", "#006047",
        "#00504f", "#004056", "#00305d", "#002064", "#00106b", "#000b79", "#00177f", "#002286",
        "#002e8c", "#003992", "#004599", "#00519f", "#005ca5", "#0068ac", "#0073b2", "#007fb9",
        "#008bbf", "#0096c5", "#00a2cc", "#00add2", "#00b9d8", "#00c5df", "#00d0e5", "#00dceb",
        "#00e7f2", "#00f3f8", "#fafafa", "#f8f8f8", "#f7f7f7", "#f5f5f5", "#f3f3f3", "#f2f2f2",
        "#f0f0f0", "#eeeeee", "#ededed", "#ebebeb", "#e9e9e9", "#e8e8e8", "#e6e6e6", "#e4e4e4",
        "#e2e2e2", "#e1e1e1", "#dfdfdf", "#dddddd", "#dcdcdc", "#dadada", "#d8d8d8", "#d7d7d7",
        "#d5d5d5", "#d3d3d3", "#d2d2d2", "#d0d0d0", "#cecece", "#cdcdcd", "#cbcbcb", "#c9c9c9",
        "#c8c8c8", "#c6c6c6", "#c4c4c4", "#c3c3c3", "#c1c1c1", "#bfbfbf", "#bebebe", "#bcbcbc",
        "#bababa", "#b9b9b9", "#b7b7b7", "#b5b5b5", "#b4b4b4", "#b2b2b2", "#b0b0b0", "#aeaeae",
        "#adadad", "#ababab", "#a9a9a9", "#a8a8a8", "#a6a6a6", "#a4a4a4", "#a3a3a3", "#a1a1a1",
        "#9f9f9f", "#9e9e9e", "#9c9c9c", "#9a9a9a", "#999999", "#979797", "#959595", "#949494",
        "#929292", "#909090", "#8f8f8f", "#8d8d8d", "#8b8b8b", "#8a8a8a", "#888888", "#868686",
        "#858585", "#838383", "#818181", "#808080", "#7e7e7e", "#7c7c7c", "#7a7a7a", "#797979",
        "#777777", "#757575", "#747474", "#727272", "#707070", "#6f6f6f", "#6d6d6d", "#6b6b6b",
        "#6a6a6a", "#686868", "#666666", "#656565", "#636363", "#616161", "#606060", "#5e5e5e",
        "#5c5c5c", "#5b5b5b", "#595959", "#575757", "#565656", "#545454", "#525252", "#515151",
        "#4f4f4f", "#4d4d4d", "#4b4b4b", "#4a4a4a", "#484848", "#464646", "#454545", "#434343",
        "#414141", "#404040", "#3e3e3e", "#3c3c3c", "#3b3b3b", "#393939", "#373737", "#363636",
        "#343434", "#323232", "#313131", "#2f2f2f", "#2d2d2d", "#2c2c2c", "#2a2a2a", "#282828",
        "#272727", "#252525", "#232323", "#222222", "#202020", "#1e1e1e", "#1d1d1d", "#1b1b1b",
        "#191919", "#171717", "#161616", "#141414", "#121212", "#111111", "#0f0f0f", "#0d0d0d",
        "#0c0c0c", "#0a0a0a", "#080808", "#070707", "#050505", "#030303", "#020202", "#000000",
    ]

   
    def __init__(
        self,
        brightness_temperatures: Dict[str, xr.DataArray],
        output_dir: Path,
        color_palette: List[str] = None,
        vmin: float = 183.15,
        vmax: float = 323.15,
        product_name: str = "Cloud_Tops",
    ):
        self._brightness_temperatures = brightness_temperatures
        self._output_dir = output_dir
        self._color_palette = color_palette or self.CLOUD_TOPS_PALETTE
        self._vmin = vmin
        self._vmax = vmax
        self._product_name = product_name

    async def run(self) -> List[Path]:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        tasks = []

        for file_name, dataset in self._brightness_temperatures.items():
            tasks.append(asyncio.to_thread(self._generate_geotiff, file_name, dataset))

        return await asyncio.gather(*tasks)

    def _generate_geotiff(self, file_name: str, c13_data: xr.DataArray) -> Path:
        # Remove grid_mapping if present
        if "grid_mapping" in c13_data.attrs:
            del c13_data.attrs["grid_mapping"]

        # 1. Reproject to EPSG:4326
        # Use rioxarray's reproject method. Ensure rioxarray is installed and imported through xarray accessor
        c13_reproj = c13_data.rio.reproject("EPSG:4326")

        # Fix nodata value before clipping (original value is too large for float32)
        c13_reproj = c13_reproj.rio.write_nodata(np.nan, inplace=False)

        # 2. Clip to Argentina bounds to reduce processing area
        c13_clipped = c13_reproj.rio.clip_box(
            minx=self.ARGENTINA_BOUNDS["minx"],
            miny=self.ARGENTINA_BOUNDS["miny"],
            maxx=self.ARGENTINA_BOUNDS["maxx"],
            maxy=self.ARGENTINA_BOUNDS["maxy"],
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
