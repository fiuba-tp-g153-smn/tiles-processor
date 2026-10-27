"""
Radar product configuration for weather radar processing.

This module provides metadata about radar products (variables) and
filename parsing utilities. Color palettes are now in radar_palettes.py.
"""

from dataclasses import dataclass, replace
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
        product_id: Identifier (e.g., "dbzh", "vrad", "rhohv"). Also the path
            segment products are published under, so it must be unique.
        field_name: PyART field name for the variable
        subvolume: Which subvolume to process ("01", "02" or "04")
        s3_tiles_prefix: S3 key prefix for storing tiles
        s3_cog_prefix: S3 key prefix for storing COG files
        unit: Display unit for the variable
        long_name: Descriptive name
        file_variable: Uppercase ODIM token this product is discovered from in
            the H5 filename (RMA1_0315_01_DBZH_...). Pinned explicitly on every
            product: ``product_id`` is lowercase for S3/URL use and would never
            match the filename, and a mismatch discovers nothing *silently*.
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
    product_id="dbzh",
    file_variable="DBZH",
    field_name="reflectivity",
    subvolume="01",
    s3_tiles_prefix="tiles/radar/sinarame",
    s3_cog_prefix="cog/radar/sinarame",
    unit="dBZ",
    long_name="Horizontal Reflectivity",
)

# Long-range reflectivity: the same 0.55° DBZH moment as DBZH_CONFIG, but read
# from subvolume 04, whose single sweep reaches ~445 km (1235 gates × 360 m)
# instead of the ~235 km of the 15-sweep subvolume 01. Same palette and field,
# its own product path so both coexist per radar.
DBZH_450KM_CONFIG = RadarProductConfig(
    product_id="dbzh-450km",
    file_variable="DBZH",
    field_name="reflectivity",
    subvolume="04",
    s3_tiles_prefix="tiles/radar/sinarame",
    s3_cog_prefix="cog/radar/sinarame",
    unit="dBZ",
    long_name="Horizontal Reflectivity (450 km)",
)


VRAD_CONFIG = RadarProductConfig(
    product_id="vrad",
    file_variable="VRAD",
    field_name="velocity",
    subvolume="02",  # VRAD uses volume 02
    s3_tiles_prefix="tiles/radar/sinarame",
    s3_cog_prefix="cog/radar/sinarame",
    unit="m/s",
    long_name="Radial Velocity",
)


RHOHV_CONFIG = RadarProductConfig(
    product_id="rhohv",
    file_variable="RHOHV",
    field_name="cross_correlation_ratio",
    subvolume="01",
    s3_tiles_prefix="tiles/radar/sinarame",
    s3_cog_prefix="cog/radar/sinarame",
    unit="",
    long_name="Cross-correlation Coefficient",
)

ZDR_CONFIG = RadarProductConfig(
    product_id="zdr",
    file_variable="ZDR",
    field_name="differential_reflectivity",
    subvolume="01",
    s3_tiles_prefix="tiles/radar/sinarame",
    s3_cog_prefix="cog/radar/sinarame",
    unit="dB",
    long_name="Differential Reflectivity",
)

KDP_CONFIG = RadarProductConfig(
    product_id="kdp",
    file_variable="KDP",
    field_name="specific_differential_phase",
    subvolume="01",
    s3_tiles_prefix="tiles/radar/sinarame",
    s3_cog_prefix="cog/radar/sinarame",
    unit="°/km",
    long_name="Specific Differential Phase",
)


# Registry for looking up radar product configs by ID
RADAR_PRODUCT_CONFIGS = {
    "dbzh": DBZH_CONFIG,
    "dbzh-450km": DBZH_450KM_CONFIG,
    "vrad": VRAD_CONFIG,
    "rhohv": RHOHV_CONFIG,
    "zdr": ZDR_CONFIG,
    "kdp": KDP_CONFIG,
}


# --- INTA (Rainbow5) -------------------------------------------------------
#
# The INTA radars carry the same physical moments as SINARAME, so they reuse the
# product configs above wholesale — same PyART field, same palette, same unit —
# and only differ in where they are published. Building them with `replace`
# keeps that inheritance literal: a palette or field fix on the SINARAME config
# lands on both networks.

# Only the 240 km volumes are published. Anguil and Pergamino interleave 120 km
# scans (VOL_120_ZVW / SMN_120) in the same folder, distinguishable only by the
# file header. Publishing both under one product would make the layer's extent
# jump between two footprints as the user scrubs through time, so the short
# scans are filtered out — the analogue of SINARAME's subvolume filter.
INTA_STOP_RANGE_KM = 240.0


@dataclass(frozen=True, slots=True)
class IntaScanSpec:
    """Which INTA file is published as which product.

    The Rainbow5 data type alone is ambiguous — ``dBZ`` comes both as a 240 km
    ``.vol`` volume and as a single-sweep long-range ``.azi`` PPI — so a file is
    identified by the (variable, extension) pair, the INTA analogue of
    SINARAME's (variable, subvolume).

    Attributes:
        rainbow_variable: Rainbow5 data type in the filename (``dBZ``).
        extension: Lowercase file extension, ``.vol`` or ``.azi``.
        product_id: The product the file is published as.
        long_range: The scan must reach past ``INTA_STOP_RANGE_KM`` instead of
            stopping exactly at it. Each site sets its own long-range extent
            (e.g. ANG 480 km, PER 400 km), so it is a threshold rather than a
            fixed range.
    """

    rainbow_variable: str
    extension: str
    product_id: str
    long_range: bool = False

    def accepts_range(self, stop_range_km: float) -> bool:
        """True when a scan of this range belongs to this product."""
        if self.long_range:
            return stop_range_km > INTA_STOP_RANGE_KM
        return stop_range_km == INTA_STOP_RANGE_KM

    @property
    def product_config(self) -> RadarProductConfig:
        """The INTA-namespaced config this file is published with."""
        return INTA_PRODUCT_CONFIGS[self.product_id]


INTA_SCAN_SPECS = (
    IntaScanSpec("dBZ", ".vol", "dbzh"),
    IntaScanSpec("ZDR", ".vol", "zdr"),
    IntaScanSpec("RhoHV", ".vol", "rhohv"),
    IntaScanSpec("KDP", ".vol", "kdp"),
    IntaScanSpec("dBZ", ".azi", "dbzh-450km", long_range=True),
)

INTA_RAINBOW_TO_PRODUCT = {
    spec.rainbow_variable: spec.product_id
    for spec in INTA_SCAN_SPECS
    if spec.extension == ".vol"
}

INTA_PRODUCT_CONFIGS = {
    spec.product_id: replace(
        RADAR_PRODUCT_CONFIGS[spec.product_id],
        s3_tiles_prefix="tiles/radar/inta",
        s3_cog_prefix="cog/radar/inta",
    )
    for spec in INTA_SCAN_SPECS
}


def get_inta_scan_spec(rainbow_variable: str, extension: str) -> IntaScanSpec:
    """Resolve the spec an INTA file is published under.

    Args:
        rainbow_variable: Rainbow5 data type in the filename (``dBZ``).
        extension: File extension, any case (``.vol``, ``.AZI``).

    Raises:
        ValueError: The pair has no mapping (an unsupported or diagnostic
            moment such as ``dBuZ``, or a variable not shipped in that format).
    """
    key = (rainbow_variable, extension.lower())
    for spec in INTA_SCAN_SPECS:
        if (spec.rainbow_variable, spec.extension) == key:
            return spec
    mapped = sorted((s.rainbow_variable, s.extension) for s in INTA_SCAN_SPECS)
    raise ValueError(
        f"Rainbow variable '{rainbow_variable}' in '{extension}' files is not "
        f"mapped to a radar product. Mapped: {mapped}"
    )


def get_inta_product_config(rainbow_variable: str) -> RadarProductConfig:
    """Resolve the product a Rainbow5 ``.vol`` data type is published as.

    Raises:
        ValueError: The data type has no mapping (an unsupported or diagnostic
            moment such as ``dBuZ``).
    """
    return get_inta_scan_spec(rainbow_variable, ".vol").product_config


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
