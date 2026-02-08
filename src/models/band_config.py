"""Band-specific configuration for satellite image processing."""

from dataclasses import dataclass


@dataclass(frozen=True)
class BandConfig:
    """
    Configuration for a specific satellite band processing.

    This contains all band-specific parameters needed to process
    satellite imagery through the pipeline.

    Attributes:
        band_id: Identifier for the band (e.g., "band_13", "band_9")
        file_pattern: Pattern to match files in NOAA S3 (e.g., "C13_G19")
        vmin: Minimum temperature for normalization (Kelvin)
        vmax: Maximum temperature for normalization (Kelvin)
        palette_name: Name of the color palette to use
        s3_prefix: S3 key prefix for storing tiles
        product_name: Name for the output product metadata
    """

    band_id: str
    file_pattern: str
    vmin: float
    vmax: float
    palette_name: str
    s3_prefix: str
    product_name: str

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON encoding."""
        return {
            "band_id": self.band_id,
            "file_pattern": self.file_pattern,
            "vmin": self.vmin,
            "vmax": self.vmax,
            "palette_name": self.palette_name,
            "s3_prefix": self.s3_prefix,
            "product_name": self.product_name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BandConfig":
        """Deserialize from dictionary."""
        return cls(
            band_id=data["band_id"],
            file_pattern=data["file_pattern"],
            vmin=data["vmin"],
            vmax=data["vmax"],
            palette_name=data["palette_name"],
            s3_prefix=data["s3_prefix"],
            product_name=data["product_name"],
        )


# Pre-defined band configurations
BAND_13_CONFIG = BandConfig(
    band_id="band_13",
    file_pattern="C13_G19",
    vmin=183.15,  # -90°C in Kelvin
    vmax=323.15,  # +50°C in Kelvin
    palette_name="CLOUD_TOPS_PALETTE",
    s3_prefix="band_13/tiles",
    product_name="Cloud_Tops",
)

BAND_9_CONFIG = BandConfig(
    band_id="band_9",
    file_pattern="C09_G19",
    vmin=161.0,  # -112.15°C in Kelvin
    vmax=330.0,  # +56.85°C in Kelvin
    palette_name="WATER_VAPOR_PALETTE",
    s3_prefix="band_9/tiles",
    product_name="Water_Vapor",
)

BAND_2_CONFIG = BandConfig(
    band_id="band_2",
    file_pattern="C02_G19",
    vmin=0.0,  # Reflectance factor min
    vmax=1.0,  # Reflectance factor max
    palette_name="VISIBLE_PALETTE",
    s3_prefix="band_2/tiles",
    product_name="Visible",
)

# Registry for looking up band configs by ID
BAND_CONFIGS = {
    "band_13": BAND_13_CONFIG,
    "band_9": BAND_9_CONFIG,
    "band_2": BAND_2_CONFIG,
}


def get_band_config(band_id: str) -> BandConfig:
    """Get band configuration by ID."""
    if band_id not in BAND_CONFIGS:
        raise ValueError(
            f"Unknown band_id '{band_id}'. Valid: {list(BAND_CONFIGS.keys())}"
        )
    return BAND_CONFIGS[band_id]
