"""ECMWF product configuration and color palettes."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EcmwfProductConfig:
    """Immutable configuration for an ECMWF-derived product."""

    parameter: str       # ECMWF short name, e.g. "tp"
    vmin: float          # Minimum display value (in product units after conversion)
    vmax: float          # Maximum display value
    palette_name: str    # Logical palette identifier
    grib_prefix: str     # S3 prefix for cached GRIB files
    cog_prefix: str      # S3 prefix for COG outputs
    tiles_prefix: str    # S3 prefix for tile outputs


ECMWF_TP_CONFIG = EcmwfProductConfig(
    parameter="tp",
    vmin=0.0,
    vmax=100.0,
    palette_name="precipitation",
    grib_prefix="grib/models/ecmwf/total_precipitation",
    cog_prefix="cog/models/ecmwf/total_precipitation",
    tiles_prefix="tiles/models/ecmwf/total_precipitation",
)

# Forecast scheduling constants
MAX_LOOKBACK_HOURS: int = 48       # Hours to look back when searching for forecasts
FORECAST_HOURS: int = 144          # Total length of each forecast (6 days)
PERIOD_HOURS: int = 3              # Duration of each processing period
FORECASTS_TO_MAINTAIN: int = 2     # Number of recent forecasts to keep active


def _build_precipitation_palette() -> tuple[str, ...]:
    """
    Build a 256-color precipitation palette (light blues → dark reds).

    Returns hex color strings compatible with normalize_and_colorize().
    Index 0 corresponds to vmin (0 mm), index 255 to vmax (100 mm).
    Control points follow a standard meteorological precipitation scale.
    """
    # Control points: (normalized_position 0–1, R, G, B)
    control: list[tuple[float, int, int, int]] = [
        (0.00, 255, 255, 255),  # 0 mm   — white
        (0.01, 190, 225, 255),  # 1 mm   — very light blue
        (0.05, 120, 185, 250),  # 5 mm   — light blue
        (0.10,  50, 135, 225),  # 10 mm  — medium blue
        (0.15,  30, 100, 200),  # 15 mm  — blue
        (0.20,  30, 170, 170),  # 20 mm  — blue-cyan
        (0.25,  30, 180, 100),  # 25 mm  — cyan-green
        (0.30, 100, 200,  50),  # 30 mm  — green
        (0.40, 220, 230,  20),  # 40 mm  — yellow-green
        (0.50, 255, 200,   0),  # 50 mm  — yellow
        (0.60, 255, 130,   0),  # 60 mm  — orange
        (0.70, 240,  50,   0),  # 70 mm  — red-orange
        (0.80, 200,   0,  20),  # 80 mm  — red
        (0.90, 150,   0,  80),  # 90 mm  — dark red
        (1.00, 100,   0, 100),  # 100 mm — dark purple
    ]

    positions = [c[0] for c in control]
    colors = [(c[1], c[2], c[3]) for c in control]

    palette: list[str] = []
    for i in range(256):
        x = i / 255.0
        hi = 0
        while hi < len(positions) - 1 and positions[hi] < x:
            hi += 1
        lo = max(hi - 1, 0)

        if lo == hi:
            r, g, b = colors[lo]
        else:
            span = positions[hi] - positions[lo]
            t = (x - positions[lo]) / span if span > 0 else 0.0
            r = round(colors[lo][0] + t * (colors[hi][0] - colors[lo][0]))
            g = round(colors[lo][1] + t * (colors[hi][1] - colors[lo][1]))
            b = round(colors[lo][2] + t * (colors[hi][2] - colors[lo][2]))

        palette.append(f"#{int(r):02x}{int(g):02x}{int(b):02x}")

    return tuple(palette)


PRECIPITATION_PALETTE: tuple[str, ...] = _build_precipitation_palette()
