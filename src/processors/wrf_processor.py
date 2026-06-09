"""WRF-ARG4K processor: NetCDF → RGBA tiles + COG + contour/barb GeoJSON → S3."""

import gc
import shutil
import subprocess
import uuid
import warnings
from contextlib import contextmanager
from logging import getLogger
from pathlib import Path

import matplotlib.colors as mcolors
import numpy as np
import rasterio
from rasterio.control import GroundControlPoint
from rasterio.crs import CRS  # pylint: disable=no-name-in-module
from rasterio.errors import NotGeoreferencedWarning

from config import Config
from factories import create_s3_client
from models.work_unit import WorkUnit
from models.wrf_config import (
    WrfContourConfig,
    WrfProductConfig,
    get_wrf_product_config,
    parse_wrf_filename,
)
from processors.base_processor import ImageProcessor
from services.contouring import (
    extract_barbs_tiled,
    extract_isolines_2d,
    smooth_array,
    write_geojson,
)

logger = getLogger(__name__)

MS_TO_KT = 1.94384

# ── Colormaps (SMN WRF-ARG4K reference palettes) ─────────────────────────────

_RADAR_COLORS = [
    "#3C426D", "#3C426D", "#3C426D", "#3C426D", "#3C426D",
    "#3D4E7B", "#3D4E7B", "#3D4E7B", "#3D5988", "#3D5988",
    "#3D5988", "#3C6596", "#3C6596", "#3C6596", "#3971A3",
    "#3971A3", "#3971A3", "#357DAF", "#357DAF", "#2F89BB",
    "#2F89BB", "#2897C6", "#2897C6", "#26A3D1", "#2BB0DA",
    "#53F337", "#4DE133", "#47D12F", "#40C02B", "#3AB027",
    "#34A022", "#2C891D", "#247217", "#EDEF3D", "#E1E439",
    "#D6DA34", "#CDD230", "#C0C62B", "#CEAD20", "#D69719",
    "#DB8115", "#EB0B2E", "#CB001B", "#C10015", "#B20009",
    "#9B0000", "#C2005F", "#D600A0", "#EA00EA", "#CB00CD",
    "#B300B7", "#9A00A0", "#FFFFFF", "#DFF6ED", "#C6F1E1",
    "#B7ECD8", "#A7ECCF", "#97E3C6", "#97E3C6", "#87DFBE",
    "#87DFBE", "#87DFBE", "#87DFBE",
]

_RADAR_BOUNDS = [
    -18.0, -16.5, -15.0, -13.5, -12.0, -10.5,  -9.0,  -7.5,
     -6.0,  -4.5,  -3.0,  -1.5,   0.0,   1.5,   3.0,   4.5,
      6.0,   7.5,   9.0,  10.5,  12.0,  13.5,  15.0,  16.5,
     18.0,  19.5,  21.0,  22.5,  24.0,  25.5,  27.0,  28.5,
     30.0,  31.5,  33.0,  34.5,  36.0,  37.5,  39.0,  40.5,
     42.0,  43.5,  45.0,  46.5,  48.0,  49.5,  51.0,  52.5,
     54.0,  55.5,  57.0,  58.5,  60.0,  61.5,  63.0,  64.5,
     66.0,  67.5,  69.0,  70.5,  72.0,  73.5,  75.0,  76.5,
]

_GUST_GRADIENT = [
    (179 / 255, 178 / 255, 170 / 255), (254 / 255, 231 / 255, 121 / 255),
    (254 / 255, 192 / 255,  61 / 255), (254 / 255, 160 / 255,   1 / 255),
    (254 / 255,  97 / 255,   1 / 255), (255 / 255,  50 / 255,   0 / 255),
    (225 / 255,  20 / 255,   0 / 255), (192 / 255,   0 / 255,   0 / 255),
]
_GUST_BOUNDS = [25, 30, 35, 40, 45, 50, 60, 70, 80]

_PP_COLORS = [
    (  0 / 255, 103 / 255,  54 / 255), ( 49 / 255, 161 / 255,  84 / 255),
    (119 / 255, 196 / 255, 121 / 255), (193 / 255, 228 / 255, 152 / 255),
    (254 / 255, 254 / 255, 156 / 255), (  5 / 255,  90 / 255, 141 / 255),
    ( 53 / 255, 143 / 255, 191 / 255), (166 / 255, 188 / 255, 218 / 255),
    (226 / 255, 225 / 255, 228 / 255), (166 / 255,  54 / 255,   3 / 255),
    (240 / 255, 104 / 255,  19 / 255), (253 / 255, 174 / 255, 107 / 255),
    (119 / 255,   0 / 255, 116 / 255), (197 / 255,  27 / 255, 138 / 255),
    (247 / 255, 104 / 255, 161 / 255), (251 / 255, 180 / 255, 185 / 255),
    ( 99 / 255,  99 / 255,  99 / 255), (187 / 255, 187 / 255, 187 / 255),
]
_PP_BOUNDS = [
    0.1, 1.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0,
    40.0, 50.0, 60.0, 80.0, 100.0, 120.0, 150.0, 180.0, 220.0, 260.0,
]

_CAPE_GRADIENT = [
    (178 / 255, 248 / 255, 169 / 255), (119 / 255, 243 / 255, 115 / 255),
    ( 55 / 255, 209 / 255,  60 / 255), (253 / 255, 248 / 255, 169 / 255),
    (253 / 255, 230 / 255, 120 / 255), (255 / 255, 192 / 255,  60 / 255),
    (255 / 255,  96 / 255,   0 / 255), (255 / 255,  50 / 255,   0 / 255),
    (224 / 255,  19 / 255,   0 / 255),
]
_CAPE_BOUNDS = [100, 250, 750, 1000, 1500, 2000, 2500, 3000, 3500]

_PW_COLORS = [
    (206 / 255, 207 / 255, 228 / 255), (165 / 255, 187 / 255, 216 / 255),
    (116 / 255, 168 / 255, 205 / 255), ( 53 / 255, 143 / 255, 191 / 255),
    (  5 / 255, 112 / 255, 176 / 255),
]
_PW_BOUNDS = [20, 30, 40, 50, 60, 70]

_Q_COLORS = [
    (133 / 255, 208 / 255, 246 / 255), (174 / 255, 239 / 255, 253 / 255),
    (198 / 255, 253 / 255, 253 / 255), (248 / 255, 253 / 255, 246 / 255),
    (198 / 255, 253 / 255, 168 / 255), (179 / 255, 248 / 255, 168 / 255),
    (253 / 255, 248 / 255, 168 / 255), (253 / 255, 230 / 255, 120 / 255),
    (253 / 255, 191 / 255,  60 / 255), (253 / 255, 159 / 255,   0 / 255),
    (253 / 255,  96 / 255,   0 / 255), (253 / 255,  50 / 255,   0 / 255),
    (223 / 255,  20 / 255,   0 / 255), (191 / 255,   0 / 255,   0 / 255),
    (164 / 255,   0 / 255,   0 / 255), (112 / 255,  96 / 255, 220 / 255),
    ( 72 / 255,  60 / 255, 200 / 255), ( 58 / 255,  39 / 255, 177 / 255),
    ( 45 / 255,  30 / 255, 165 / 255),
]
_Q_BOUNDS = list(range(0, 20))

_SHEAR_COLORS = [
    (253 / 255, 248 / 255, 168 / 255), (255 / 255, 192 / 255,  60 / 255),
    (255 / 255,  96 / 255,   0 / 255), (225 / 255,  20 / 255,   0 / 255),
]
_SHEAR_BOUNDS = [10, 20, 30, 40, 50]

_SHIP_COLORS = [
    (253 / 255, 248 / 255, 169 / 255), (255 / 255, 192 / 255,  60 / 255),
    (255 / 255,  96 / 255,   0 / 255), (225 / 255,  20 / 255,   0 / 255),
]
_SHIP_BOUNDS = [0.1, 1.0, 2.0, 3.0, 4.0]

# Manual script uses ListedColormap for V850 (each bound gets its own color band)
_V850_COLORS = [
    (223 / 255,  20 / 255,   0 / 255), (253 / 255,  50 / 255,   0 / 255),
    (253 / 255,  96 / 255,   0 / 255), (253 / 255, 159 / 255,   0 / 255),
    (253 / 255, 191 / 255,  60 / 255), (253 / 255, 248 / 255, 169 / 255),
]
_V850_BOUNDS = [-48, -44, -40, -36, -32, -28, -24]


_TRANSPARENT_RGBA = (1.0, 1.0, 1.0, 0.0)


def _listed(bounds: list, colors: list) -> tuple:
    cmap = mcolors.ListedColormap(colors)
    norm = mcolors.BoundaryNorm(bounds, ncolors=len(colors))
    return norm, cmap


def _gradient(bounds: list, colors: list) -> tuple:
    cmap = mcolors.LinearSegmentedColormap.from_list("c", colors)
    norm = mcolors.BoundaryNorm(bounds, ncolors=256)
    return norm, cmap


def _v850_palette() -> tuple:
    """V850 colormap: out-of-range values render transparent.

    Matches the manual script (`generar_wrf.py::plot_jet`) which shades
    only [-48, -24] kt; above -24 should be transparent (no positive jet
    rendered) and below -48 should be transparent too. ListedColormap's
    default behavior maps out-of-range to the first/last color, so we
    override ``set_over`` and ``set_under`` explicitly.
    """
    cmap = mcolors.ListedColormap(_V850_COLORS)
    cmap.set_over(_TRANSPARENT_RGBA)
    cmap.set_under(_TRANSPARENT_RGBA)
    norm = mcolors.BoundaryNorm(_V850_BOUNDS, ncolors=len(_V850_COLORS))
    return norm, cmap


_PALETTE: dict[str, tuple] = {
    "Colmax":              _listed(_RADAR_BOUNDS, _RADAR_COLORS),
    "Rafagas":             _gradient(_GUST_BOUNDS, _GUST_GRADIENT),
    "Campo900hPa":         _listed(_Q_BOUNDS, _Q_COLORS),
    "Precipitacion1h":     _listed(_PP_BOUNDS, _PP_COLORS),
    "MUCAPE":              _gradient(_CAPE_BOUNDS, _CAPE_GRADIENT),
    "AguaPrecipitable":    _listed(_PW_BOUNDS, _PW_COLORS),
    "JetCapasBajas":       _v850_palette(),
    "CortanteNivelesBajos": _listed(_SHEAR_BOUNDS, _SHEAR_COLORS),
    "CAPE_BRN":            _gradient(_CAPE_BOUNDS, _CAPE_GRADIENT),
    "Granizo":             _listed(_SHIP_BOUNDS, _SHIP_COLORS),
}


class WrfProcessor(ImageProcessor):
    """Processor for WRF-ARG4K model output (SMN Argentina).

    Pipeline per (product, forecast step):
        1. Load primary variable + extras (barb u/v, contour scalars).
        2. Apply unit conversions and product-specific masking.
        3. Build RGBA from primary; NaN pixels get either a topographic
           brown fill or a fully transparent fill, per product config.
        4. Write RGBA GeoTIFF (EPSG:4326, north-up) and feed gdal2tiles.
        5. Write float32 COG of the raw primary field.
        6. Extract contour LineStrings and wind-barb Points to GeoJSON.
        7. Upload tiles, COG, and GeoJSON layers to S3.
    """

    ZOOM_LEVELS = "4-6"
    GDAL_PROCESSES = 2
    BARB_SIMPLIFY_TOLERANCE = 0.0  # not applied to points
    CONTOUR_SIMPLIFY_TOLERANCE = 0.05

    def __init__(self, config: Config):
        super().__init__(config)
        self._s3_client = create_s3_client(config, with_ttl=config.SEAWEEDFS_WRF_TTL)

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """Execute full WRF processing pipeline for one product / forecast step."""
        product_id = work_unit.band_id.removeprefix("wrf_")
        product_config = get_wrf_product_config(product_id)

        f2d_path = Path(downloaded_file_path)
        if not f2d_path.exists():
            raise FileNotFoundError(f"WRF FIELD2D file not found: {f2d_path}")

        parsed = parse_wrf_filename(Path(work_unit.source_uri).name)
        init_tag = parsed["init_tag"]
        fxxx = parsed["fxxx"]

        work_dir = Path(self.config.TMP_DIR) / "wrf" / work_unit.image_id
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._check_shutdown()
            with self._time_stage("load"):
                payload = self._load_payload(
                    product_config, f2d_path, work_unit.source_uri
                )
            self._check_shutdown()

            with self._time_stage("rgba"):
                rgba = self._build_rgba(
                    product_id,
                    product_config,
                    payload["primary"],
                    payload["topo_nan_mask"],
                )
            # Two-stage raster output: (1) write a GCP-tagged TIFF that
            # carries each pixel's true Lambert (lon, lat); (2) warp it to a
            # regular EPSG:4326 grid via thin-plate spline. gdal2tiles then
            # produces tiles whose pixels align with the basemap and the
            # GeoJSON overlays — the same alignment the manual SMN script
            # gets via cartopy's projection-aware rendering.
            geotiff_path = work_dir / f"{work_unit.image_id}.tif"
            with self._time_stage("geotiff"):
                geotiff_gcp_path = work_dir / f"{work_unit.image_id}_gcp.tif"
                self._save_rgba_geotiff(
                    rgba, payload["lat"], payload["lon"], geotiff_gcp_path
                )
                del rgba
                gc.collect()
                self._check_shutdown()

                self._warp_to_epsg4326(
                    geotiff_gcp_path, geotiff_path, resampling="near"
                )
                geotiff_gcp_path.unlink(missing_ok=True)
            self._check_shutdown()

            # COG: same two-stage pipeline. Float field needs bilinear
            # resampling so NaN/finite transitions stay clean.
            cog_path = work_dir / f"{work_unit.image_id}_cog.tif"
            with self._time_stage("cog"):
                cog_gcp_path = work_dir / f"{work_unit.image_id}_cog_gcp.tif"
                self._save_float_geotiff_gcp(
                    payload["primary_cog"], payload["lat"], payload["lon"], cog_gcp_path
                )
                self._warp_to_epsg4326(
                    cog_gcp_path,
                    cog_path,
                    of="COG",
                    resampling="bilinear",
                    extra_creation_options=(
                        "COMPRESS=DEFLATE",
                        "PREDICTOR=3",
                        "BLOCKSIZE=512",
                    ),
                )
                cog_gcp_path.unlink(missing_ok=True)
            self._check_shutdown()

            # Secondary point-query COGs (wind magnitude + flagged contours).
            # Same float pipeline as the primary; one COG per secondary var.
            with self._time_stage("secondary_cog"):
                secondary_cog_paths = self._generate_secondary_cogs(
                    work_dir, work_unit.image_id, payload, product_config
                )
            self._check_shutdown()

            with self._time_stage("geojson"):
                geojson_paths = self._generate_geojson_layers(
                    work_dir, work_unit.image_id, payload, product_config
                )
            self._check_shutdown()

            tiles_dir = work_dir / "tiles"
            with self._time_stage("tiling"):
                self._generate_tiles(geotiff_path, tiles_dir)
            self._check_shutdown()

            with self._time_stage("upload"):
                await self._upload_outputs(
                    product_config=product_config,
                    init_tag=init_tag,
                    fxxx=fxxx,
                    tiles_dir=tiles_dir,
                    cog_path=cog_path,
                    secondary_cog_paths=secondary_cog_paths,
                    geojson_paths=geojson_paths,
                )

            logger.info(
                "[WRF] Completed %s (%s/%s)",
                work_unit.image_id,
                init_tag,
                fxxx,
            )

        finally:
            if work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def _load_payload(
        self,
        product_config: WrfProductConfig,
        f2d_path: Path,
        original_source_uri: str,
    ) -> dict:
        """Read primary, barb components, and contour scalars in one pass."""
        import netCDF4 as nc  # pylint: disable=import-outside-toplevel

        logger.info(
            "[WRF] Loading %s from %s", product_config.product_id, f2d_path.name
        )

        f2d = nc.Dataset(str(f2d_path))
        f3d = None

        # Open FIELD3D iff any consumer (primary, barbs, contours) needs it
        needs_f3d = product_config.needs_field3d or _any_field3d_extra(product_config)
        try:
            lat = np.array(f2d.variables["lat"][:], dtype=float)
            lon = np.array(f2d.variables["lon"][:], dtype=float)

            if needs_f3d:
                f3d_path = Path(original_source_uri.replace("FIELD2D", "FIELD3D"))
                if not f3d_path.exists():
                    raise FileNotFoundError(f"WRF FIELD3D file not found: {f3d_path}")
                f3d = nc.Dataset(str(f3d_path))

            primary = self._read_var(
                f2d, f3d,
                var=product_config.primary_var,
                level_hpa=product_config.primary_level_hpa,
                use_field3d=product_config.needs_field3d,
            )
            # Snapshot topographic NaN mask BEFORE threshold masking.
            # FIELD3D returns NaN where pressure level > surface pressure
            # (terrain too high — e.g. Andes for the 850/900 hPa level).
            # Threshold masking adds non-topographic NaN later; only the
            # original mask should get the brown nan_fill_color.
            topo_nan_mask = np.isnan(primary)
            primary_raw = primary
            primary = self._apply_primary_masking(product_config.product_id, primary)
            primary_cog = self._primary_cog_field(
                product_config.product_id, primary_raw, primary
            )

            barb_uv = None
            if product_config.barbs is not None:
                use_3d = product_config.barbs.level_hpa is not None
                barb_u = self._read_var(
                    f2d, f3d,
                    var=product_config.barbs.u_var,
                    level_hpa=product_config.barbs.level_hpa,
                    use_field3d=use_3d,
                )
                barb_v = self._read_var(
                    f2d, f3d,
                    var=product_config.barbs.v_var,
                    level_hpa=product_config.barbs.level_hpa,
                    use_field3d=use_3d,
                )
                barb_uv = (barb_u, barb_v)

            contour_data: dict[str, np.ndarray] = {}
            for contour in product_config.contours:
                use_3d = contour.level_hpa is not None
                arr = self._read_var(
                    f2d, f3d,
                    var=contour.var,
                    level_hpa=contour.level_hpa,
                    use_field3d=use_3d,
                )
                if contour.unit_conversion == "ms_to_kt":
                    arr = arr * MS_TO_KT
                if contour.topographic_clip:
                    arr = np.where((arr <= 0) | (arr > 500), np.nan, arr)
                contour_data[contour.name] = arr
        finally:
            f2d.close()
            if f3d is not None:
                f3d.close()

        return {
            "lat": lat,
            "lon": lon,
            "primary": primary,
            "primary_cog": primary_cog,
            "topo_nan_mask": topo_nan_mask,
            "barbs": barb_uv,
            "contours": contour_data,
        }

    @staticmethod
    def _read_var(
        f2d, f3d, *, var: str, level_hpa: float | None, use_field3d: bool
    ) -> np.ndarray:
        """Read a variable from FIELD2D or a pressure level from FIELD3D."""
        if use_field3d:
            if f3d is None:
                raise RuntimeError(f"FIELD3D required for variable '{var}'")
            plev = np.array(f3d.variables["plev"][:], dtype=float)
            assert level_hpa is not None
            level_idx = int(np.where(plev == level_hpa)[0][0])
            raw = f3d.variables[var][0, 0, level_idx, :, :]
        else:
            raw = f2d.variables[var][0, 0, :, :]

        if hasattr(raw, "filled"):
            return np.ma.filled(raw, np.nan).astype(float)
        return np.array(raw, dtype=float)

    @staticmethod
    def _primary_cog_field(
        product_id: str, primary_raw: np.ndarray, primary_masked: np.ndarray
    ) -> np.ndarray:
        """Float field written to the primary point-query COG.

        For every product this is the *masked* primary (identical bytes to the
        rendered raster) — no behavior change. The sole exception is
        CortanteNivelesBajos: its colour scale starts at 10 kt, so the raster
        masks <10 kt to NaN, but the point query should still report sub-10-kt
        shear magnitudes. Return the converted-but-unmasked field for that one
        product only. `shear_s1_s2` is in m s-1, hence the kt conversion here.
        """
        if product_id == "CortanteNivelesBajos":
            return primary_raw * MS_TO_KT
        return primary_masked

    @staticmethod
    def _apply_primary_masking(product_id: str, data: np.ndarray) -> np.ndarray:
        """Apply unit conversions and below-threshold masking to the primary field."""
        if product_id == "Colmax":
            return np.where(data < -18, np.nan, data)
        if product_id == "Rafagas":
            data = data * MS_TO_KT
            return np.where(data < 25, np.nan, data)
        if product_id == "Precipitacion1h":
            return np.where(data < 0.1, np.nan, data)
        if product_id in ("MUCAPE", "CAPE_BRN"):
            return np.where(data < 100, np.nan, data)
        if product_id == "AguaPrecipitable":
            return np.where(data < 20, np.nan, data)
        if product_id == "JetCapasBajas":
            # Manual script (`plot_jet`) does NOT apply a threshold — values
            # outside [-48, -24] kt are rendered transparent via the cmap's
            # set_over/set_under overrides. Threshold-masking here would mark
            # most of Argentina as NaN and the brown nan_fill_color would
            # paint the whole scene brown.
            return data * MS_TO_KT
        if product_id == "CortanteNivelesBajos":
            data = data * MS_TO_KT
            return np.where(data < 10, np.nan, data)
        if product_id == "Granizo":
            return np.where(data < 0.1, np.nan, data)
        return data

    # ------------------------------------------------------------------
    # Colorization
    # ------------------------------------------------------------------

    def _build_rgba(
        self,
        product_id: str,
        product_config: WrfProductConfig,
        data: np.ndarray,
        topo_nan_mask: np.ndarray,
    ) -> np.ndarray:
        """Apply palette and resolve NaN handling per product config.

        The brown ``nan_fill_color`` is only painted on topographic NaN
        (terrain above the requested pressure level). Threshold-masked
        NaN remains fully transparent so masked-out values do not paint
        the entire scene brown.
        """
        norm, cmap = _PALETTE[product_id]
        rgba = (cmap(norm(data)) * 255).astype(np.uint8)
        nan_mask = np.isnan(data)

        fill = product_config.nan_fill_color
        if fill is None:
            rgba[nan_mask, 3] = 0
        else:
            threshold_nan = nan_mask & ~topo_nan_mask
            rgba[threshold_nan, 3] = 0

            rgba[topo_nan_mask, 0] = fill[0]
            rgba[topo_nan_mask, 1] = fill[1]
            rgba[topo_nan_mask, 2] = fill[2]
            rgba[topo_nan_mask, 3] = 255
        return rgba

    # ------------------------------------------------------------------
    # Raster outputs
    # ------------------------------------------------------------------

    # GCP_STEP samples the curvilinear grid every N cells (in both axes) to
    # build the GCP set passed to ``gdalwarp -tps``. Thin-plate spline cost
    # grows superlinearly with GCP count; ~800 GCPs gives sub-pixel accuracy
    # against the SMN reference figures while keeping warp time around 5s.
    # Lower = denser GCPs (more accurate, slower). Higher = fewer GCPs
    # (faster, but corners can drift).
    GCP_STEP: int = 40

    # Output resolution forced on gdalwarp (degrees per pixel). 0.04° ≈ 4.4 km
    # which is close to WRF-ARG4K's native ~4 km grid. Without ``-tr`` GDAL
    # picks a finer resolution from the GCP density and inflates the output
    # raster, blowing up tile-generation time.
    WARP_PIXEL_SIZE_DEG: float = 0.04

    @staticmethod
    def _build_gcps(
        lat: np.ndarray, lon: np.ndarray, step: int
    ) -> list[GroundControlPoint]:
        """Build a sparse GCP grid from the curvilinear lat/lon arrays.

        Each GCP maps an array pixel ``(col=i, row=j)`` to the WGS84 coordinate
        ``(lon[j, i], lat[j, i])`` of that grid cell. Sampling every ``step``
        cells (with explicit corners + edges) is enough for ``gdalwarp -tps``
        to recover the Lambert→WGS84 transformation accurately.
        """
        nrows, ncols = lat.shape
        rows = list(range(0, nrows, step))
        if rows[-1] != nrows - 1:
            rows.append(nrows - 1)
        cols = list(range(0, ncols, step))
        if cols[-1] != ncols - 1:
            cols.append(ncols - 1)

        gcps: list[GroundControlPoint] = []
        for j in rows:
            for i in cols:
                lat_val = lat[j, i]
                lon_val = lon[j, i]
                if not (np.isfinite(lat_val) and np.isfinite(lon_val)):
                    continue
                gcps.append(
                    GroundControlPoint(
                        row=float(j),
                        col=float(i),
                        x=float(lon_val),
                        y=float(lat_val),
                    )
                )
        return gcps

    @staticmethod
    @contextmanager
    def _suppress_not_georeferenced_warning():
        """GCP-tagged writers open without a transform on purpose; mute the
        expected NotGeoreferencedWarning (GCPs are attached immediately after)."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", NotGeoreferencedWarning)
            yield

    @staticmethod
    def _save_rgba_geotiff(
        rgba: np.ndarray,
        lat: np.ndarray,
        lon: np.ndarray,
        output_path: Path,
    ) -> None:
        """Write RGBA uint8 GeoTIFF tagged with GCPs from the curvilinear grid.

        The tagged file is later warped to a regular EPSG:4326 raster by
        ``_warp_to_epsg4326`` so that gdal2tiles produces tiles aligned with
        the WGS84 web-map grid (and the GeoJSON overlays). Without GCPs, a
        cheap ``from_bounds`` approximation distorted pixel positions because
        the WRF Lambert grid is curvilinear, not a regular WGS84 grid.
        """
        nrows, ncols = rgba.shape[:2]
        gcps = WrfProcessor._build_gcps(lat, lon, WrfProcessor.GCP_STEP)
        tmp_path = output_path.parent / f"{uuid.uuid4()}.tif"
        try:
            with WrfProcessor._suppress_not_georeferenced_warning(), rasterio.open(
                tmp_path,
                "w",
                driver="GTiff",
                height=nrows,
                width=ncols,
                count=4,
                dtype=np.uint8,
                compress="lzw",
            ) as dst:
                dst.gcps = (gcps, CRS.from_epsg(4326))
                for i in range(4):
                    dst.write(rgba[:, :, i], i + 1)
            tmp_path.rename(output_path)
            logger.info("[WRF] GeoTIFF written (GCP-tagged): %s", output_path.name)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _save_float_geotiff_gcp(
        data: np.ndarray,
        lat: np.ndarray,
        lon: np.ndarray,
        output_path: Path,
    ) -> None:
        """Write a single-band float32 GeoTIFF tagged with GCPs.

        Used as an intermediate before warping to the final COG via
        ``_warp_to_epsg4326(of="COG")``. NaN is preserved as nodata so the
        warp + downstream point queries see masked pixels correctly.
        """
        nrows, ncols = data.shape
        gcps = WrfProcessor._build_gcps(lat, lon, WrfProcessor.GCP_STEP)
        tmp_path = output_path.parent / f"{uuid.uuid4()}.tif"
        try:
            with WrfProcessor._suppress_not_georeferenced_warning(), rasterio.open(
                tmp_path,
                "w",
                driver="GTiff",
                height=nrows,
                width=ncols,
                count=1,
                dtype="float32",
                compress="DEFLATE",
                predictor=3,
                nodata=float("nan"),
            ) as dst:
                dst.gcps = (gcps, CRS.from_epsg(4326))
                dst.write(data.astype(np.float32), 1)
            tmp_path.rename(output_path)
            logger.info("[WRF] Float GeoTIFF written (GCP-tagged): %s", output_path.name)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _warp_to_epsg4326(
        input_path: Path,
        output_path: Path,
        *,
        of: str = "GTiff",
        resampling: str = "near",
        extra_creation_options: tuple[str, ...] = (),
    ) -> None:
        """Reproject a GCP-tagged raster to a regular EPSG:4326 grid.

        Uses ``gdalwarp -tps`` (thin-plate spline) so the curvilinear Lambert
        layout is recovered from the GCP grid rather than from a 4-corner
        affine approximation. ``-r near`` is correct for the RGBA output;
        callers warping float fields should pass ``resampling="bilinear"``.
        """
        tmp_path = output_path.parent / f"{uuid.uuid4()}.tif"
        pixel_deg = WrfProcessor.WARP_PIXEL_SIZE_DEG
        cmd = [
            "gdalwarp",
            "-tps",
            "-t_srs",
            "EPSG:4326",
            "-tr",
            str(pixel_deg),
            str(pixel_deg),
            "-r",
            resampling,
            "-of",
            of,
            "-overwrite",
        ]
        for opt in extra_creation_options:
            cmd += ["-co", opt]
        cmd += [str(input_path), str(tmp_path)]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                logger.error("[WRF] gdalwarp error: %s", result.stderr)
                raise RuntimeError(f"gdalwarp failed: {result.stderr}")
            tmp_path.rename(output_path)
            logger.info("[WRF] Warped → %s: %s", of, output_path.name)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------
    # Secondary point-query COGs
    # ------------------------------------------------------------------

    def _generate_secondary_cogs(
        self,
        work_dir: Path,
        image_id: str,
        payload: dict,
        product_config: WrfProductConfig,
    ) -> dict[str, Path]:
        """Build one float COG per queryable secondary variable.

        Returns a map ``{variable_name: cog_path}`` uploaded later as
        ``{cog}/{init}/{F}.{variable_name}.tif``. Mirrors the primary COG
        pipeline (GCP-tagged float → tps warp → COG) so point queries align.
        """
        scalars = self._build_secondary_scalars(payload, product_config)
        outputs: dict[str, Path] = {}
        for variable, arr in scalars.items():
            cog_path = self._float_field_to_cog(
                arr, payload["lat"], payload["lon"], work_dir, f"{image_id}_{variable}"
            )
            outputs[variable] = cog_path
            logger.info("[WRF] Secondary COG '%s' built for %s", variable, image_id)
        return outputs

    @staticmethod
    def _build_secondary_scalars(
        payload: dict, product_config: WrfProductConfig
    ) -> dict[str, np.ndarray]:
        """Collect queryable secondary scalar fields keyed by variable name."""
        scalars: dict[str, np.ndarray] = {}

        if payload["barbs"] is not None and product_config.barbs is not None:
            u, v = payload["barbs"]
            # Barb components are in m/s; expose magnitude in knots (SMN unit).
            scalars[product_config.barbs.point_query_var] = (
                np.hypot(u, v) * MS_TO_KT
            )

        for contour in product_config.contours:
            if not contour.point_query:
                continue
            arr = payload["contours"].get(contour.name)
            if arr is not None:
                # Already unit-converted / clipped in _load_payload.
                scalars[contour.name] = arr

        return scalars

    def _float_field_to_cog(
        self,
        data: np.ndarray,
        lat: np.ndarray,
        lon: np.ndarray,
        work_dir: Path,
        name: str,
    ) -> Path:
        """Run the float field → GCP TIFF → EPSG:4326 COG pipeline."""
        gcp_path = work_dir / f"{name}_cog_gcp.tif"
        self._save_float_geotiff_gcp(data, lat, lon, gcp_path)
        cog_path = work_dir / f"{name}_cog.tif"
        self._warp_to_epsg4326(
            gcp_path,
            cog_path,
            of="COG",
            resampling="bilinear",
            extra_creation_options=(
                "COMPRESS=DEFLATE",
                "PREDICTOR=3",
                "BLOCKSIZE=512",
            ),
        )
        gcp_path.unlink(missing_ok=True)
        return cog_path

    # ------------------------------------------------------------------
    # Vector layers
    # ------------------------------------------------------------------

    def _generate_geojson_layers(
        self,
        work_dir: Path,
        image_id: str,
        payload: dict,
        product_config: WrfProductConfig,
    ) -> dict[str, Path]:
        """Produce one GeoJSON per contour and (optionally) one for barbs."""
        outputs: dict[str, Path] = {}
        lon = payload["lon"]
        lat = payload["lat"]

        for contour in product_config.contours:
            arr = payload["contours"].get(contour.name)
            if arr is None:
                continue
            features = self._build_contour_features(arr, lon, lat, contour)
            if not features:
                logger.info(
                    "[WRF] Skipping empty contour layer '%s' for %s",
                    contour.name,
                    image_id,
                )
                continue
            out_path = work_dir / f"{image_id}_{contour.name}.json"
            write_geojson(features, out_path)
            outputs[contour.name] = out_path
            logger.info(
                "[WRF] Contour '%s' written (%d features)",
                contour.name,
                len(features),
            )

        if payload["barbs"] is not None and product_config.barbs is not None:
            u, v = payload["barbs"]
            tiled = extract_barbs_tiled(u_ms=u, v_ms=v, lon_2d=lon, lat_2d=lat)
            for (zoom, tx, ty), feats in tiled.items():
                tile_dir = work_dir / "barbs" / str(zoom) / str(tx)
                tile_dir.mkdir(parents=True, exist_ok=True)
                write_geojson(feats, tile_dir / f"{ty}.json")
            if tiled:
                outputs["barbs_tiled_dir"] = work_dir / "barbs"
                logger.info("[WRF] Barb tiles written (%d GeoJSON tiles)", len(tiled))

        return outputs

    def _build_contour_features(
        self,
        arr: np.ndarray,
        lon: np.ndarray,
        lat: np.ndarray,
        contour: WrfContourConfig,
    ) -> list[dict]:
        smoothed = (
            smooth_array(arr, contour.smooth_sigma)
            if contour.smooth_sigma > 0
            else arr
        )
        return extract_isolines_2d(
            z=smoothed,
            x_2d=lon,
            y_2d=lat,
            levels=contour.levels,
            simplify_tolerance=self.CONTOUR_SIMPLIFY_TOLERANCE,
            value_property="value",
        )

    # ------------------------------------------------------------------
    # Tile generation and upload
    # ------------------------------------------------------------------

    def _generate_tiles(self, geotiff_path: Path, tiles_dir: Path) -> None:
        """Generate XYZ tiles via gdal2tiles."""
        tiles_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            "gdal2tiles.py",
            "-p", "mercator",
            "-z", self.ZOOM_LEVELS,
            "-w", "none",
            "--resampling=near",
            f"--processes={self.GDAL_PROCESSES}",
            "--xyz",
            "--tiledriver=WEBP",
            "--webp-lossless",
            str(geotiff_path),
            str(tiles_dir),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logger.error("[WRF] gdal2tiles error: %s", result.stderr)
            raise RuntimeError(f"gdal2tiles failed: {result.stderr}")
        logger.info("[WRF] Tiles generated: %s", tiles_dir)

    async def _upload_outputs(
        self,
        *,
        product_config: WrfProductConfig,
        init_tag: str,
        fxxx: str,
        tiles_dir: Path,
        cog_path: Path,
        secondary_cog_paths: dict[str, Path],
        geojson_paths: dict[str, Path],
    ) -> None:
        product_id = product_config.product_id
        tiles_prefix = (
            f"{product_config.s3_tiles_prefix}/{product_id}/{init_tag}/{fxxx}"
        )
        cog_prefix = f"{product_config.s3_cog_prefix}/{product_id}/{init_tag}/{fxxx}"
        cog_key = f"{cog_prefix}.tif"
        geojson_prefix = (
            f"{product_config.s3_geojson_prefix}/{product_id}/{init_tag}/{fxxx}"
        )

        count = await self._s3_client.upload_directory(tiles_dir, tiles_prefix)
        logger.info("[WRF] Uploaded %d tile files → %s", count, tiles_prefix)

        cog_uploaded = await self._s3_client.upload_file(cog_key, cog_path)
        if not cog_uploaded:
            logger.warning("[WRF] COG upload failed for %s", product_id)
        else:
            logger.info("[WRF] Uploaded COG → %s", cog_key)

        # Secondary point-query COGs: {cog}/{init}/{F}.{variable}.tif
        for variable, path in secondary_cog_paths.items():
            key = f"{cog_prefix}.{variable}.tif"
            if await self._s3_client.upload_file(key, path):
                logger.info("[WRF] Uploaded secondary COG → %s", key)
            else:
                logger.warning("[WRF] Secondary COG upload failed: %s", key)

        barbs_tiled_dir = geojson_paths.pop("barbs_tiled_dir", None)
        if barbs_tiled_dir is not None and Path(barbs_tiled_dir).is_dir():
            barb_prefix = f"{geojson_prefix}/barbs"
            count = await self._s3_client.upload_directory(
                Path(barbs_tiled_dir), barb_prefix
            )
            logger.info("[WRF] Uploaded %d barb tile(s) → %s", count, barb_prefix)

        for layer_name, path in geojson_paths.items():
            key = f"{geojson_prefix}/{layer_name}.json"
            ok = await self._s3_client.upload_file(key, path)
            if ok:
                logger.info("[WRF] Uploaded GeoJSON → %s", key)
            else:
                logger.warning("[WRF] GeoJSON upload failed: %s", key)


def _any_field3d_extra(product_config: WrfProductConfig) -> bool:
    """True if any barb or contour entry needs FIELD3D."""
    if product_config.barbs is not None and product_config.barbs.level_hpa is not None:
        return True
    return any(c.level_hpa is not None for c in product_config.contours)
