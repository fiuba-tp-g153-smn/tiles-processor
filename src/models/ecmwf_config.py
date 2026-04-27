"""ECMWF product configuration and color palettes."""

from dataclasses import dataclass

from models.ecmwf_palettes import PRECIPITATION_COLORS, PRECIPITATION_THRESHOLDS


@dataclass(frozen=True, slots=True)
class EcmwfProductConfig:
    """Immutable configuration for an ECMWF-derived product."""

    parameter: str  # ECMWF short name, e.g. "tp"
    vmin: float  # Minimum display value (in product units after conversion)
    vmax: float  # Maximum display value
    palette_name: str  # Logical palette identifier
    grib_prefix: str  # S3 prefix for cached GRIB files
    cog_prefix: str  # S3 prefix for COG outputs
    tiles_prefix: str  # S3 prefix for tile outputs


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
MAX_LOOKBACK_HOURS: int = 48  # Hours to look back when searching for forecasts
FORECAST_HOURS: int = 144  # Total length of each forecast (6 days)
PERIOD_HOURS: int = 3  # Cadence between centered timestamps and half-window offset
ACCUMULATION_HOURS: int = 6  # Centered accumulation window (= 2 × PERIOD_HOURS)
FORECASTS_TO_MAINTAIN: int = 3  # Number of recent forecasts to keep active
