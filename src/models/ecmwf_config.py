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
    grib_prefix="grib/ecmwf-ifs/total-precipitation",
    cog_prefix="cog/ecmwf-ifs/total-precipitation",
    tiles_prefix="tiles/ecmwf-ifs/total-precipitation",
    producer_data_source_id="ecmwf_ifs_total_precipitation_producer",
    period_data_source_id="ecmwf_ifs_total_precipitation_period",
    processor_id="ecmwf_ifs_total_precipitation_processor",
    inline_processor_id="ecmwf_ifs_total_precipitation_grib_download",
    band_id="ecmwf_ifs_total_precipitation",
    log_prefix="ECMWF-TP",
)

ECMWF_MSLP_CONFIG = EcmwfProductConfig(
    parameter="msl",
    vmin=950.0,  # hPa
    vmax=1050.0,  # hPa
    palette_name="pressure",
    grib_prefix="grib/ecmwf-ifs/mean-sea-level-pressure",
    cog_prefix="cog/ecmwf-ifs/mean-sea-level-pressure",
    tiles_prefix="tiles/ecmwf-ifs/mean-sea-level-pressure",
    producer_data_source_id="ecmwf_ifs_mean_sea_level_pressure_producer",
    period_data_source_id="ecmwf_ifs_mean_sea_level_pressure_period",
    processor_id="ecmwf_ifs_mean_sea_level_pressure_processor",
    inline_processor_id="ecmwf_ifs_mean_sea_level_pressure_grib_download",
    band_id="ecmwf_ifs_mean_sea_level_pressure",
    log_prefix="ECMWF-MSLP",
    geojson_prefix="geojson/ecmwf-ifs/mean-sea-level-pressure",
)

# Forecast scheduling constants (global to ECMWF Open Data, not product-specific)
MAX_LOOKBACK_HOURS: int = 48  # Hours to look back when searching for forecasts
FORECAST_HOURS: int = 144  # Total length of each forecast (6 days)
STEP_HOURS: int = 3  # Cadence of model output steps
WINDOW_HOURS: int = STEP_HOURS * 2  # Accumulation window length (6h)
FORECASTS_TO_MAINTAIN: int = 3  # Number of recent forecasts to keep active
