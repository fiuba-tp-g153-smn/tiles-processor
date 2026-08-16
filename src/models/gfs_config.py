"""GFS configuration: how we reach the model, and what we build from it."""

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Ingestion: one producer, one GRIB, three products
# ---------------------------------------------------------------------------

# INVARIANT: GFS registers exactly ONE producer data source. The three GFS
# products are derived from the *same* GRIB subset, so a single download per
# (cycle, step) feeds all of them. Replicating the ECMWF one-source-per-product
# shape would triple the request load against NOMADS and the cold-start burst.
GFS_PRODUCER_DATA_SOURCE_ID = "gfs_producer"

# Step-level source: downloads the cached GRIB from S3 for a single forecast
# step. Shared by all three products because the download is identical — only
# the processor differs.
GFS_STEP_DATA_SOURCE_ID = "gfs_step"

# Inline processor that uploads the GRIB and fans out one WorkUnit per product.
GFS_INLINE_PROCESSOR_ID = "gfs_grib_download"

GFS_GRIB_PREFIX = "grib/models/gfs"

# The required variable/level list. The grib_filter CGI returns the *cross
# product* of these intersected with what the model carries, i.e. 13 messages:
# HGT/TMP/UGRD/VGRD at each of 1000/500/250 mb, plus MSLET at mean sea level.
#
# Only 9 of those feed a product (TMP@1000, UGRD@1000, VGRD@1000 and TMP@250 are
# unused). There is no way to exclude single combinations — isolating the 9
# would take one request per level, tripling the request load to save ~30% of
# bytes. Requests are the scarce resource here, bytes are not, so we take all 13.
GFS_VARIABLES = ("HGT", "MSLET", "TMP", "UGRD", "VGRD")
GFS_LEVELS = ("mean_sea_level", "1000_mb", "500_mb", "250_mb")
GFS_EXPECTED_MESSAGE_COUNT = 13


@dataclass(frozen=True, slots=True)
class GfsAccessConfig:
    """Where and how to fetch GFS GRIB subsets."""

    subset_endpoint: str
    timeout_seconds: int = 120
    max_concurrent_downloads: int = 4

    def __post_init__(self) -> None:
        """Validate the invariants that hold regardless of whether GFS is used.

        The endpoint is deliberately *not* checked here. It is enforced by
        `require_endpoint()` at the point of use instead.
        """
        if self.max_concurrent_downloads < 1:
            raise ValueError(
                "gfs_max_concurrent_downloads must be >= 1, "
                f"got {self.max_concurrent_downloads}"
            )

    @property
    def is_configured(self) -> bool:
        """True when an endpoint is set and GFS ingestion can actually run."""
        return bool(self.subset_endpoint.strip())

    def require_endpoint(self) -> str:
        """Return the endpoint, failing loudly when GFS is enabled without one.

        Called when a fetcher is constructed, so enabling a GFS product without
        `GFS_SUBSET_ENDPOINT` fails at wiring time with an actionable message
        rather than on the first download attempt.
        """
        if not self.is_configured:
            raise ValueError(
                "A GFS product is enabled but no endpoint is configured. Set the "
                "GFS_SUBSET_ENDPOINT environment variable in .env "
            )
        return self.subset_endpoint


CYCLE_HOURS = (0, 6, 12, 18)  # UTC hours at which NCEP issues GFS runs

# 3-hourly out to +48h then 6-hourly.
FORECAST_HOURS = 144  # 6 days
SHORT_RANGE_LIMIT_HOURS = 48
STEP_HOURS_SHORT = 3
STEP_HOURS_LONG = 6


def forecast_steps() -> list[int]:
    """Forecast hours to produce: 0,3,...,48 then 54,60,...,144 (33 values)."""
    short = list(range(0, SHORT_RANGE_LIMIT_HOURS + 1, STEP_HOURS_SHORT))
    long = list(
        range(
            SHORT_RANGE_LIMIT_HOURS + STEP_HOURS_LONG,
            FORECAST_HOURS + 1,
            STEP_HOURS_LONG,
        )
    )
    return short + long


PA_TO_HPA = 100.0

ISOBAR_STEP_HPA = 3.0
THICKNESS_STEP_M = 60.0
THICKNESS_LEVELS_HPA = (500, 1000)
HIGHLIGHTED_THICKNESS_M = (5280.0, 5400.0, 5580.0, 5700.0)
GEOPOTENTIAL_STEP_M = 60.0
ISOTHERM_STEP_C = 5.0

POINT_QUERY_THICKNESS = "thickness"
POINT_QUERY_TEMPERATURE = "temperature"
POINT_QUERY_GEOPOTENTIAL = "geopotential"


@dataclass(frozen=True, slots=True)
class GfsProductConfig:
    """Immutable configuration for one GFS-derived product."""

    product_id: str  # "mslp" | "500" | "250"
    band_id: str  # WorkUnit band_id, e.g. "gfs_500"
    processor_id: str
    cog_prefix: str
    tiles_prefix: str
    geojson_prefix: str
    log_prefix: str  # log line prefix, e.g. "GFS-500"
    level_hpa: int | None = None  # isobaric level; None for the MSLP product


GFS_MSLP_CONFIG = GfsProductConfig(
    product_id="mslp",
    band_id="gfs_mslp",
    processor_id="gfs_mslp",
    cog_prefix="cog/models/gfs/mean_sea_level_pressure",
    tiles_prefix="tiles/models/gfs/mean_sea_level_pressure",
    geojson_prefix="geojson/models/gfs/mean_sea_level_pressure",
    log_prefix="GFS-MSLP",
)

GFS_500_CONFIG = GfsProductConfig(
    product_id="500",
    band_id="gfs_500",
    processor_id="gfs_upper_level",
    cog_prefix="cog/models/gfs/500hpa",
    tiles_prefix="tiles/models/gfs/500hpa",
    geojson_prefix="geojson/models/gfs/500hpa",
    log_prefix="GFS-500",
    level_hpa=500,
)

GFS_250_CONFIG = GfsProductConfig(
    product_id="250",
    band_id="gfs_250",
    processor_id="gfs_upper_level",
    cog_prefix="cog/models/gfs/250hpa",
    tiles_prefix="tiles/models/gfs/250hpa",
    geojson_prefix="geojson/models/gfs/250hpa",
    log_prefix="GFS-250",
    level_hpa=250,
)

GFS_PRODUCT_CONFIGS: dict[str, GfsProductConfig] = {
    GFS_MSLP_CONFIG.product_id: GFS_MSLP_CONFIG,
    GFS_500_CONFIG.product_id: GFS_500_CONFIG,
    GFS_250_CONFIG.product_id: GFS_250_CONFIG,
}

_BY_BAND_ID: dict[str, GfsProductConfig] = {
    cfg.band_id: cfg for cfg in GFS_PRODUCT_CONFIGS.values()
}


def get_gfs_product_config(product_id: str) -> GfsProductConfig:
    """Look up a GFS product config by product_id."""
    if product_id not in GFS_PRODUCT_CONFIGS:
        raise ValueError(
            f"Unknown GFS product_id '{product_id}'. "
            f"Valid: {list(GFS_PRODUCT_CONFIGS)}"
        )
    return GFS_PRODUCT_CONFIGS[product_id]


def get_gfs_product_config_by_band(band_id: str) -> GfsProductConfig:
    """Look up a GFS product config by WorkUnit band_id."""
    if band_id not in _BY_BAND_ID:
        raise ValueError(f"Unknown GFS band_id '{band_id}'. Valid: {list(_BY_BAND_ID)}")
    return _BY_BAND_ID[band_id]


def primary_cog_key(product: GfsProductConfig, cycle_ts: str, image_id: str) -> str:
    """S3 key of the product's main field, and the fan-out's dedup sentinel."""
    return f"{product.cog_prefix}/{cycle_ts}/{image_id}.tif"


def secondary_cog_key(
    product: GfsProductConfig, cycle_ts: str, variable: str, image_id: str
) -> str:
    """S3 key of a secondary point-query COG."""
    return f"{product.cog_prefix}/{cycle_ts}/{variable}/{image_id}.tif"
