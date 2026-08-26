"""
Radar product configuration for weather radar processing.

This module provides metadata about radar products (variables) and
filename parsing utilities. Color palettes are now in radar_palettes.py.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RadarStationFilter:
    """Decides which radar stations (RMA1, RMA2, …) are processed.

    Radar IDs are discovered at runtime (parsed from filenames), never fixed in
    config, so the choice is modeled as a per-station predicate rather than a
    materialized set — this lets a blacklist work without enumerating every
    station up front, and a brand-new radar is covered automatically.

    Accepted ``radar_stations`` shapes in settings.json (see ``from_settings``):
        "all"                          -> every station (also the default)
        "none"                         -> no station
        {"whitelist": ["RMA1", ...]}   -> only the listed stations
        {"blacklist": ["RMA3", ...]}   -> every station except the listed ones
    """

    mode: str
    stations: frozenset[str] = frozenset()

    def allows(self, radar_id: str) -> bool:
        """Return True if ``radar_id`` should be processed under this filter."""
        if self.mode == "all":
            return True
        if self.mode == "none":
            return False
        if self.mode == "whitelist":
            return radar_id in self.stations
        return radar_id not in self.stations  # blacklist

    @classmethod
    def from_settings(cls, raw: Any) -> "RadarStationFilter":
        """Build a filter from the raw ``radar_stations`` settings value.

        ``None`` (key absent) defaults to "all" — the historical behavior of
        processing every discovered radar. Ambiguous/malformed values fail fast
        with a message that names the offending shape.
        """
        if raw is None or raw == "all":
            return cls("all")
        if raw == "none":
            return cls("none")
        if isinstance(raw, dict):
            return cls._from_dict(raw)
        raise ValueError(
            'radar_stations must be "all", "none", or an object with exactly '
            f'one of "whitelist"/"blacklist", got {raw!r}'
        )

    @classmethod
    def _from_dict(cls, raw: dict) -> "RadarStationFilter":
        has_white = "whitelist" in raw
        has_black = "blacklist" in raw
        if has_white == has_black:
            raise ValueError(
                'radar_stations object needs exactly one of "whitelist"/'
                f'"blacklist" (got keys {sorted(raw)})'
            )
        mode = "whitelist" if has_white else "blacklist"
        stations = raw[mode]
        if not isinstance(stations, list) or not all(
            isinstance(s, str) for s in stations
        ):
            raise ValueError(
                f'radar_stations "{mode}" must be a list of station-ID strings'
            )
        return cls(mode, frozenset(stations))


@dataclass(frozen=True, slots=True)
class RadarProductConfig:
    """
    Configuration for a specific radar product (variable).

    Attributes:
        product_id: Identifier (e.g., "DBZH", "VRAD", "RHOHV"). Also the path
            segment products are published under, so it must be unique.
        field_name: PyART field name for the variable
        subvolume: Which subvolume to process ("01", "02" or "04")
        s3_tiles_prefix: S3 key prefix for storing tiles
        s3_cog_prefix: S3 key prefix for storing COG files
        unit: Display unit for the variable
        long_name: Descriptive name
        file_variable: Filename token this product reads, when it differs from
            ``product_id``. Empty (the common case) means they are the same.
            Set it when two products share one physical moment but different
            scan geometry — e.g. DBZH_450KM is the DBZH moment of the
            long-range subvolume 04, so it reads "DBZH" files but publishes
            under its own product path.
    """

    product_id: str
    field_name: str
    subvolume: str
    s3_tiles_prefix: str
    s3_cog_prefix: str
    unit: str = ""
    long_name: str = ""
    file_variable: str = ""

    @property
    def variable(self) -> str:
        """Filename variable token this product is discovered from."""
        return self.file_variable or self.product_id

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON encoding."""
        return {
            "product_id": self.product_id,
            "field_name": self.field_name,
            "subvolume": self.subvolume,
            "s3_tiles_prefix": self.s3_tiles_prefix,
            "s3_cog_prefix": self.s3_cog_prefix,
            "unit": self.unit,
            "long_name": self.long_name,
        }


# Pre-defined radar product configurations
# Color palettes are defined in radar_palettes.py

DBZH_CONFIG = RadarProductConfig(
    product_id="DBZH",
    field_name="reflectivity",
    subvolume="01",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="dBZ",
    long_name="Horizontal Reflectivity",
)

# Long-range reflectivity: the same 0.55° DBZH moment as DBZH_CONFIG, but read
# from subvolume 04, whose single sweep reaches ~445 km (1235 gates × 360 m)
# instead of the ~235 km of the 15-sweep subvolume 01. Same palette and field,
# its own product path so both coexist per radar.
DBZH_450KM_CONFIG = RadarProductConfig(
    product_id="DBZH_450KM",
    field_name="reflectivity",
    subvolume="04",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="dBZ",
    long_name="Horizontal Reflectivity (450 km)",
    file_variable="DBZH",
)

ZH_CONFIG = RadarProductConfig(
    product_id="ZH",
    field_name="reflectivity",
    subvolume="01",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="dBZ",
    long_name="Reflectivity",
)

TH_CONFIG = RadarProductConfig(
    product_id="TH",
    field_name="total_power",
    subvolume="01",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="dBZ",
    long_name="Total Power",
)

VRAD_CONFIG = RadarProductConfig(
    product_id="VRAD",
    field_name="velocity",
    subvolume="02",  # VRAD uses volume 02
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="m/s",
    long_name="Radial Velocity",
)

WRAD_CONFIG = RadarProductConfig(
    product_id="WRAD",
    field_name="spectrum_width",
    subvolume="02",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="m/s",
    long_name="Spectrum Width",
)

RHOHV_CONFIG = RadarProductConfig(
    product_id="RHOHV",
    field_name="cross_correlation_ratio",
    subvolume="01",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="",
    long_name="Cross-correlation Coefficient",
)

ZDR_CONFIG = RadarProductConfig(
    product_id="ZDR",
    field_name="differential_reflectivity",
    subvolume="01",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="dB",
    long_name="Differential Reflectivity",
)

KDP_CONFIG = RadarProductConfig(
    product_id="KDP",
    field_name="specific_differential_phase",
    subvolume="01",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="°/km",
    long_name="Specific Differential Phase",
)

PHIDP_CONFIG = RadarProductConfig(
    product_id="PHIDP",
    field_name="differential_phase",
    subvolume="01",
    s3_tiles_prefix="tiles/radar",
    s3_cog_prefix="cog/radar",
    unit="°",
    long_name="Differential Phase",
)

# Registry for looking up radar product configs by ID
RADAR_PRODUCT_CONFIGS = {
    "DBZH": DBZH_CONFIG,
    "DBZH_450KM": DBZH_450KM_CONFIG,
    "ZH": ZH_CONFIG,
    "TH": TH_CONFIG,
    "VRAD": VRAD_CONFIG,
    "WRAD": WRAD_CONFIG,
    "RHOHV": RHOHV_CONFIG,
    "ZDR": ZDR_CONFIG,
    "KDP": KDP_CONFIG,
    "PHIDP": PHIDP_CONFIG,
}


def get_radar_product_config(product_id: str) -> RadarProductConfig:
    """Get radar product configuration by ID."""
    if product_id not in RADAR_PRODUCT_CONFIGS:
        raise ValueError(
            f"Unknown product_id '{product_id}'. "
            f"Valid: {list(RADAR_PRODUCT_CONFIGS.keys())}"
        )
    return RADAR_PRODUCT_CONFIGS[product_id]


def get_radar_product_config_for_file(
    variable: str, subvolume: str
) -> RadarProductConfig:
    """Resolve the product a radar file belongs to from its filename tokens.

    The variable token alone is ambiguous: DBZH files exist in both the
    short-range subvolume 01 (product DBZH) and the long-range subvolume 04
    (product DBZH_450KM). The (variable, subvolume) pair is unique across the
    registry, so it is what identifies the product a file must be published as.
    """
    for config in RADAR_PRODUCT_CONFIGS.values():
        if config.variable == variable and config.subvolume == subvolume:
            return config
    raise ValueError(
        f"No radar product for variable '{variable}' subvolume '{subvolume}'"
    )


def parse_radar_filename(filename: str) -> dict:
    """
    Parse radar filename into components.

    Filename format: RMA1_0315_01_DBZH_20260114T170328Z.H5
                     ^    ^    ^  ^    ^
                     |    |    |  |    timestamp
                     |    |    |  variable (DBZH, VRAD, etc.)
                     |    |    subvolume (01 or 02)
                     |    volume
                     radar_id

    Returns:
        Dict with radar_id, volume, subvolume, variable, timestamp
    """
    stem = filename.replace(".H5", "").replace(".h5", "")
    parts = stem.split("_")

    if len(parts) < 5:
        raise ValueError(f"Invalid radar filename format: {filename}")

    return {
        "radar_id": parts[0],
        "volume": parts[1],
        "subvolume": parts[2],
        "variable": parts[3],
        "timestamp": parts[4],
    }
