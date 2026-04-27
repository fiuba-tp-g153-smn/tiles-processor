"""ECMWF product configuration shared by all ECMWF-derived products."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EcmwfProductConfig:
    """Immutable configuration for an ECMWF-derived product."""

    parameter: str  # ECMWF short name, e.g. "tp" or "msl"
    vmin: float  # Minimum display value (in product units after conversion)
    vmax: float  # Maximum display value
    palette_name: str  # Logical palette identifier
    grib_prefix: str  # S3 prefix for cached GRIB files
    cog_prefix: str  # S3 prefix for COG outputs
    tiles_prefix: str  # S3 prefix for tile outputs
    producer_data_source_id: str  # data_source_id of the producer-side source
    period_data_source_id: str  # data_source_id of the period-side (worker) source
    processor_id: str  # processor_id of the subprocess processor
    inline_processor_id: str  # processor_id of the inline GRIB downloader
    band_id: str  # band_id used in WorkUnit and tracker
    log_prefix: str  # log line prefix, e.g. "ECMWF-TP"
    geojson_prefix: str | None = None  # S3 prefix for GeoJSON outputs (optional)


ECMWF_TP_CONFIG = EcmwfProductConfig(
    parameter="tp",
    vmin=0.0,
    vmax=100.0,
    palette_name="precipitation",
    grib_prefix="grib/models/ecmwf/total_precipitation",
    cog_prefix="cog/models/ecmwf/total_precipitation",
    tiles_prefix="tiles/models/ecmwf/total_precipitation",
    producer_data_source_id="ecmwf_tp_producer",
    period_data_source_id="ecmwf_tp_period",
    processor_id="ecmwf_tp_processor",
    inline_processor_id="ecmwf_tp_grib_download",
    band_id="ecmwf_tp",
    log_prefix="ECMWF-TP",
)

ECMWF_MSLP_CONFIG = EcmwfProductConfig(
    parameter="msl",
    vmin=950.0,  # hPa
    vmax=1050.0,  # hPa
    palette_name="pressure",
    grib_prefix="grib/models/ecmwf/mean_sea_level_pressure",
    cog_prefix="cog/models/ecmwf/mean_sea_level_pressure",
    tiles_prefix="tiles/models/ecmwf/mean_sea_level_pressure",
    producer_data_source_id="ecmwf_mslp_producer",
    period_data_source_id="ecmwf_mslp_period",
    processor_id="ecmwf_mslp_processor",
    inline_processor_id="ecmwf_mslp_grib_download",
    band_id="ecmwf_mslp",
    log_prefix="ECMWF-MSLP",
    geojson_prefix="geojson/models/ecmwf/mean_sea_level_pressure",
)

# Forecast scheduling constants (global to ECMWF Open Data, not product-specific)
MAX_LOOKBACK_HOURS: int = 48  # Hours to look back when searching for forecasts
FORECAST_HOURS: int = 144  # Total length of each forecast (6 days)
STEP_HOURS: int = 3  # Cadence of model output steps
FORECASTS_TO_MAINTAIN: int = 3  # Number of recent forecasts to keep active
