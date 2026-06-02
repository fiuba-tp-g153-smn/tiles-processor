"""WRF-ARG4K product configuration."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True, slots=True)
class WrfBarbsConfig:
    """Wind-barb overlay specification.

    Attributes:
        u_var: Eastward component variable name (m/s).
        v_var: Northward component variable name (m/s).
        level_hpa: Pressure level for FIELD3D vars; None means FIELD2D.
        stride: Subsampling factor in both grid axes.
        point_query_var: Variable key for the secondary point-query COG built
            from the barb magnitude (``sqrt(u^2 + v^2)``, converted to knots).
            The COG is uploaded as ``{cog}/{init}/{F}.{point_query_var}.tif``.
    """

    u_var: str
    v_var: str
    level_hpa: Optional[float] = None
    stride: int = 38
    point_query_var: str = "wind"


@dataclass(frozen=True, slots=True)
class WrfContourConfig:
    """Contour overlay specification (one isolines GeoJSON per entry).

    Attributes:
        name: Output filename suffix (e.g. "isobars", "shear"). The full path
            becomes ``{geojson_prefix}/{init}/{F}/{name}.json``.
        var: NetCDF variable name to contour.
        levels: Contour levels in display units (post unit conversion).
        smooth_sigma: Gaussian smoothing sigma applied before extraction
            (0 disables smoothing).
        level_hpa: Pressure level for FIELD3D vars; None means FIELD2D.
        unit_conversion: Either ``"ms_to_kt"`` or None.
        topographic_clip: If True, treats values <= 0 or > 500 as NaN
            before smoothing (used for Bulk Richardson Number).
        point_query: If True, also emit a secondary point-query COG from this
            contour's scalar field (same units as the contour). The COG is
            uploaded as ``{cog}/{init}/{F}.{name}.tif`` and exposed by the
            data-service secondary point endpoint.
    """

    name: str
    var: str
    levels: tuple[float, ...]
    smooth_sigma: float = 0.0
    level_hpa: Optional[float] = None
    unit_conversion: Optional[str] = None
    topographic_clip: bool = False
    point_query: bool = False


@dataclass(frozen=True, slots=True)
class WrfProductConfig:
    """Configuration for a WRF-ARG4K product."""

    product_id: str
    primary_var: str
    needs_field3d: bool
    s3_tiles_prefix: str
    s3_cog_prefix: str
    s3_geojson_prefix: str = "geojson/wrf"
    primary_level_hpa: Optional[float] = None
    nan_fill_color: Optional[tuple[int, int, int]] = None
    barbs: Optional[WrfBarbsConfig] = None
    contours: tuple[WrfContourConfig, ...] = field(default_factory=tuple)
    unit: str = ""
    long_name: str = ""


# Topographic brown background used by Campo900hPa and JetCapasBajas
_TOPO_BROWN = (139, 94, 60)


COLMAX_CONFIG = WrfProductConfig(
    product_id="Colmax",
    primary_var="mdbz",
    needs_field3d=False,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    unit="dBZ",
    long_name="Reflectividad máxima columna",
)

RAFAGAS_CONFIG = WrfProductConfig(
    product_id="Rafagas",
    primary_var="gust10",
    needs_field3d=False,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    barbs=WrfBarbsConfig(u_var="u10", v_var="v10"),
    contours=(
        WrfContourConfig(
            name="gust_threshold",
            var="gust10",
            levels=(35.0,),
            unit_conversion="ms_to_kt",
        ),
    ),
    unit="kt",
    long_name="Ráfagas 10m",
)

CAMPO900HPA_CONFIG = WrfProductConfig(
    product_id="Campo900hPa",
    primary_var="q",
    needs_field3d=True,
    primary_level_hpa=900.0,
    nan_fill_color=_TOPO_BROWN,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    barbs=WrfBarbsConfig(u_var="u", v_var="v", level_hpa=900.0),
    unit="g/kg",
    long_name="Humedad específica 900 hPa",
)

PRECIPITACION1H_CONFIG = WrfProductConfig(
    product_id="Precipitacion1h",
    primary_var="pp01H",
    needs_field3d=False,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    barbs=WrfBarbsConfig(u_var="u10", v_var="v10"),
    contours=(
        WrfContourConfig(
            name="slp",
            var="slp",
            # Restricted set requested by SMN — only the strategic isobars
            levels=(976.0, 984.0, 992.0, 1000.0, 1008.0, 1016.0),
            smooth_sigma=3.0,
            point_query=True,
        ),
    ),
    unit="mm",
    long_name="Precipitación acumulada 1h",
)

MUCAPE_CONFIG = WrfProductConfig(
    product_id="MUCAPE",
    primary_var="mcape",
    needs_field3d=False,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    contours=(
        WrfContourConfig(
            name="shear_850_500",
            var="shear_850_500",
            levels=(10.0, 20.0, 30.0, 40.0, 50.0),
            smooth_sigma=3.0,
            unit_conversion="ms_to_kt",
            point_query=True,
        ),
    ),
    unit="J/kg",
    long_name="CAPE máximo",
)

AGUA_PRECIPITABLE_CONFIG = WrfProductConfig(
    product_id="AguaPrecipitable",
    primary_var="pw",
    needs_field3d=False,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    unit="mm",
    long_name="Agua precipitable",
)

JET_CAPAS_BAJAS_CONFIG = WrfProductConfig(
    product_id="JetCapasBajas",
    primary_var="v",
    needs_field3d=True,
    primary_level_hpa=850.0,
    nan_fill_color=_TOPO_BROWN,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    barbs=WrfBarbsConfig(u_var="u", v_var="v", level_hpa=850.0),
    contours=(
        WrfContourConfig(
            name="shear_850_700",
            var="shear_850_700",
            levels=(6.0, 10.0, 14.0),
            smooth_sigma=3.0,
            unit_conversion="ms_to_kt",
            point_query=True,
        ),
    ),
    unit="kt",
    long_name="Jet en capas bajas (V850)",
)

CORTANTE_NIVELES_BAJOS_CONFIG = WrfProductConfig(
    product_id="CortanteNivelesBajos",
    primary_var="shear_s1_s2",
    needs_field3d=False,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    barbs=WrfBarbsConfig(u_var="shear_s1_s2_u", v_var="shear_s1_s2_v"),
    unit="kt",
    long_name="Cortante niveles bajos",
)

CAPE_BRN_CONFIG = WrfProductConfig(
    product_id="CAPE_BRN",
    primary_var="mcape",
    needs_field3d=False,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    contours=(
        WrfContourConfig(
            name="brn",
            var="brn",
            levels=(10.0, 45.0),
            smooth_sigma=2.0,
            topographic_clip=True,
            point_query=True,
        ),
    ),
    unit="J/kg",
    long_name="CAPE máximo + Bulk Richardson Number",
)

GRANIZO_CONFIG = WrfProductConfig(
    product_id="Granizo",
    primary_var="ship",
    needs_field3d=False,
    nan_fill_color=_TOPO_BROWN,
    s3_tiles_prefix="tiles/wrf",
    s3_cog_prefix="cog/wrf",
    contours=(
        WrfContourConfig(
            name="haildiammax",
            var="haildiammax",
            levels=(0.5, 3.0, 5.0),
            point_query=True,
        ),
    ),
    unit="",
    long_name="Granizo (SHIP + Hailcast)",
)

WRF_PRODUCT_CONFIGS: dict[str, WrfProductConfig] = {
    "Colmax": COLMAX_CONFIG,
    "Rafagas": RAFAGAS_CONFIG,
    "Campo900hPa": CAMPO900HPA_CONFIG,
    "Precipitacion1h": PRECIPITACION1H_CONFIG,
    "MUCAPE": MUCAPE_CONFIG,
    "AguaPrecipitable": AGUA_PRECIPITABLE_CONFIG,
    "JetCapasBajas": JET_CAPAS_BAJAS_CONFIG,
    "CortanteNivelesBajos": CORTANTE_NIVELES_BAJOS_CONFIG,
    "CAPE_BRN": CAPE_BRN_CONFIG,
    "Granizo": GRANIZO_CONFIG,
}


def get_wrf_product_config(product_id: str) -> WrfProductConfig:
    """Get WRF product configuration by ID."""
    if product_id not in WRF_PRODUCT_CONFIGS:
        raise ValueError(
            f"Unknown WRF product_id '{product_id}'. "
            f"Valid: {list(WRF_PRODUCT_CONFIGS.keys())}"
        )
    return WRF_PRODUCT_CONFIGS[product_id]


def parse_wrf_filename(filename: str) -> dict:
    """Parse WRF NetCDF filename into components.

    Expected format:
        WRF_ARG4K.FCST_L0_FIELD2D.01H.<INIT_TAG>.<FXXX>.M000.nc
    """
    parts = filename.split(".")
    if len(parts) < 7 or not parts[0].startswith("WRF_"):
        raise ValueError(f"Invalid WRF filename format: {filename}")

    fxxx = parts[4]
    if not fxxx.startswith("F") or not fxxx[1:].isdigit():
        raise ValueError(f"Invalid forecast step '{fxxx}' in filename: {filename}")

    return {
        "field_type": parts[1],
        "init_tag": parts[3],
        "fxxx": fxxx,
        "fnum": int(fxxx[1:]),
    }
