"""Product-specific configuration for image processing."""

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class ProductConfig:
    """
    Configuration for a specific product processing.

    This contains all product-specific parameters needed to process
    imagery through the pipeline (satellite bands, weather model outputs, etc.).

    Attributes:
        product_id: Identifier for the product (e.g., "band_13", "band_9", "ecmwf_total_precipitation")
        file_pattern: Pattern to match files in source (e.g., "C13_G19" for NOAA S3)
        vmin: Minimum value for normalization
        vmax: Maximum value for normalization
        palette_name: Name of the color palette to use
        s3_prefix: S3 key prefix for storing tiles
        product_name: Name for the output product metadata
    """

    product_id: str
    file_pattern: str
    vmin: float
    vmax: float
    palette_name: str
    s3_prefix: str
    product_name: str

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON encoding."""
        return {
            "product_id": self.product_id,
            "file_pattern": self.file_pattern,
            "vmin": self.vmin,
            "vmax": self.vmax,
            "palette_name": self.palette_name,
            "s3_prefix": self.s3_prefix,
            "product_name": self.product_name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ProductConfig":
        """Deserialize from dictionary."""
        return cls(
            product_id=data["product_id"],
            file_pattern=data["file_pattern"],
            vmin=data["vmin"],
            vmax=data["vmax"],
            palette_name=data["palette_name"],
            s3_prefix=data["s3_prefix"],
            product_name=data["product_name"],
        )


# Pre-defined product configurations
BAND_13_CONFIG = ProductConfig(
    product_id="band_13",
    file_pattern="C13_G19",
    vmin=183.15,  # -90°C in Kelvin
    vmax=323.15,  # +50°C in Kelvin
    palette_name="CLOUD_TOPS_PALETTE",
    s3_prefix="band_13/tiles",
    product_name="Cloud_Tops",
)

BAND_9_CONFIG = ProductConfig(
    product_id="band_9",
    file_pattern="C09_G19",
    vmin=161.0,  # -112.15°C in Kelvin
    vmax=330.0,  # +56.85°C in Kelvin
    palette_name="WATER_VAPOR_PALETTE",
    s3_prefix="band_9/tiles",
    product_name="Water_Vapor",
)

ECMWF_TOTAL_PRECIPITATION_CONFIG = ProductConfig(
    product_id="ecmwf_total_precipitation",
    file_pattern="",  # Not used for ECMWF (no file pattern filtering needed)
    vmin=0.0,  # 0 mm
    vmax=200.0,  # 200 mm (extreme precipitation)
    palette_name="PRECIPITATION_PALETTE",
    s3_prefix="models/ecmwf/total_precipitation",
    product_name="Total_Precipitation_ECMWF",
)

# Registry for looking up product configs by ID
# Keep BAND_CONFIGS name for backwards compatibility
BAND_CONFIGS = {
    "band_13": BAND_13_CONFIG,
    "band_9": BAND_9_CONFIG,
    "ecmwf_total_precipitation": ECMWF_TOTAL_PRECIPITATION_CONFIG,
}

# Alias for clarity
PRODUCT_CONFIGS = BAND_CONFIGS


def get_product_config(product_id: str) -> ProductConfig:
    """Get product configuration by ID."""
    if product_id not in PRODUCT_CONFIGS:
        raise ValueError(
            f"Unknown product_id '{product_id}'. Valid: {list(PRODUCT_CONFIGS.keys())}"
        )
    return PRODUCT_CONFIGS[product_id]


# Backwards compatibility alias
get_band_config = get_product_config
