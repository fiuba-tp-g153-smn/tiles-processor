"""
Radar product configuration for weather radar processing.

This module provides metadata about radar products (variables) and
filename parsing utilities. Color palettes are now in radar_palettes.py.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RadarProductConfig:
    """
    Configuration for a specific radar product (variable).

    Attributes:
        product_id: Identifier (e.g., "DBZH", "VRAD", "RHOHV")
        field_name: PyART field name for the variable
        subvolume: Which subvolume to process ("01" or "02")
        s3_tiles_prefix: S3 key prefix for storing tiles
        s3_cog_prefix: S3 key prefix for storing COG files
        unit: Display unit for the variable
        long_name: Descriptive name
    """

    product_id: str
    field_name: str
    subvolume: str
    s3_tiles_prefix: str
    s3_cog_prefix: str
    unit: str = ""
    long_name: str = ""

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
        Dict with radar_id, volume, subvolume, variable, timestamp, format
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
        "format": "sinarame",
    }


# Maps INTA Rainbow5 variable names (in filename) to canonical product IDs.
INTA_VARIABLE_TO_PRODUCT: dict[str, str] = {
    "dBZ": "DBZH",
}

# Hardcoded radar_id for INTA radars until filenames/metadata carry an identifier.
# Currently only Paraná (PAR) is operational. Update when more INTA radars are added.
INTA_RADAR_ID = "PAR"


def parse_inta_radar_filename(filename: str) -> dict:
    """
    Parse INTA Rainbow5 filename into components.

    Filename format: 2026052115400400dBZ.vol
                     ^             ^^  ^
                     |             ||  variable (dBZ, ZDR, ...)
                     |             |centiseconds (2 digits, ignored)
                     YYYYMMDDHHmmss (14 digits)

    Returns:
        Dict with radar_id, variable, timestamp, format='rainbow5'.
        timestamp is normalized to RMA convention: YYYYMMDDTHHmmssZ
    """
    import re  # pylint: disable=import-outside-toplevel

    stem = filename
    for suffix in (".vol", ".VOL"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    else:
        raise ValueError(f"Expected .vol extension: {filename}")

    match = re.match(r"^(\d{14})\d*([A-Za-z]\w*)$", stem)
    if not match:
        raise ValueError(f"Invalid INTA radar filename format: {filename}")

    ts_raw = match.group(1)  # YYYYMMDDHHmmss
    variable_raw = match.group(2)  # e.g. "dBZ"

    if variable_raw not in INTA_VARIABLE_TO_PRODUCT:
        raise ValueError(
            f"Unknown INTA variable '{variable_raw}' in {filename}. "
            f"Supported: {list(INTA_VARIABLE_TO_PRODUCT)}"
        )

    # Normalize: "20260521154004" → "20260521T154004Z"
    timestamp = f"{ts_raw[:8]}T{ts_raw[8:]}Z"

    return {
        "radar_id": INTA_RADAR_ID,
        "variable": INTA_VARIABLE_TO_PRODUCT[variable_raw],
        "subvolume": "01",
        "timestamp": timestamp,
        "format": "rainbow5",
    }


def parse_radar_file(filename: str) -> dict:
    """
    Dispatch filename parser by extension.

    Returns the same dict shape as parse_radar_filename, with an added
    'format' key: 'sinarame' for .H5/.h5 or 'rainbow5' for .vol/.VOL.
    """
    lower = filename.lower()
    if lower.endswith(".vol"):
        return parse_inta_radar_filename(filename)
    if lower.endswith(".h5"):
        return parse_radar_filename(filename)
    raise ValueError(f"Unsupported radar file extension: {filename}")
