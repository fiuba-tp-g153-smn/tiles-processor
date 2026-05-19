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
        s3_tiles_prefix: S3 key prefix for storing tiles
        s3_cog_prefix: S3 key prefix for storing COG files
        product_name: Name for the output product metadata
    """

    band_id: str
    file_pattern: str
    vmin: float
    vmax: float
    palette_name: str
    s3_tiles_prefix: str
    s3_cog_prefix: str
    product_name: str

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON encoding."""
        return {
            "band_id": self.band_id,
            "file_pattern": self.file_pattern,
            "vmin": self.vmin,
            "vmax": self.vmax,
            "palette_name": self.palette_name,
            "s3_tiles_prefix": self.s3_tiles_prefix,
            "s3_cog_prefix": self.s3_cog_prefix,
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
            s3_tiles_prefix=data["s3_tiles_prefix"],
            s3_cog_prefix=data["s3_cog_prefix"],
            product_name=data["product_name"],
        )


# Pre-defined band configurations
BAND_13_CONFIG = BandConfig(
    band_id="band_13",
    file_pattern="C13_G19",
    vmin=183.15,  # -90°C in Kelvin
    vmax=323.15,  # +50°C in Kelvin
    palette_name="CLOUD_TOPS_PALETTE",
    s3_tiles_prefix="tiles/band_13",
    s3_cog_prefix="cog/band_13",
    product_name="Cloud_Tops",
)

BAND_9_CONFIG = BandConfig(
    band_id="band_9",
    file_pattern="C09_G19",
    vmin=161.0,  # -112.15°C in Kelvin
    vmax=330.0,  # +56.85°C in Kelvin
    palette_name="WATER_VAPOR_PALETTE",
    s3_tiles_prefix="tiles/band_9",
    s3_cog_prefix="cog/band_9",
    product_name="Water_Vapor",
)

BAND_2_CONFIG = BandConfig(
    band_id="band_2",
    file_pattern="C02_G19",
    vmin=0.0,  # Reflectance factor min
    vmax=1.0,  # Reflectance factor max
    palette_name="VISIBLE_PALETTE",
    s3_tiles_prefix="tiles/band_2",
    s3_cog_prefix="cog/band_2",
    product_name="Visible",
)

GLM_FED_CONFIG = BandConfig(
    band_id="glm_fed",
    file_pattern="GLM-L2-LCFA",
    vmin=0.0,
    vmax=256.0,  # Flashes per grid cell (guide range: 0–256)
    palette_name="FED_PALETTE",
    s3_tiles_prefix="tiles/glm_fed",
    s3_cog_prefix="cog/glm_fed",
    product_name="GLM_Flash_Extent_Density",
)

GLM_TOE_CONFIG = BandConfig(
    band_id="glm_toe",
    file_pattern="GLM-L2-LCFA",  # same source files as FED — no separate download
    vmin=0.0,
    vmax=1.5e-12,  # 1500 fJ in Joules (guide range: 1–1500 fJ)
    palette_name="TOE_PALETTE",
    s3_tiles_prefix="tiles/glm_toe",
    s3_cog_prefix="cog/glm_toe",
    product_name="GLM_Total_Optical_Energy",
)

GLM_MFA_CONFIG = BandConfig(
    band_id="glm_mfa",
    file_pattern="GLM-L2-LCFA",  # same source files as FED — no separate download
    vmin=0.0,
    vmax=3000.0,  # km² operational cap
    palette_name="MFA_PALETTE",
    s3_tiles_prefix="tiles/glm_mfa",
    s3_cog_prefix="cog/glm_mfa",
    product_name="GLM_Minimum_Flash_Area",
)

# Folder-based GLM pipeline (CG_GLM-L2-GLMF inputs, LogNorm rendering).
# vmin/vmax are the SMN reference LogNorm ranges in the variable's native
# units; the processor takes log10 before normalize_and_colorize.
GLM_FOLDER_FED_CONFIG = BandConfig(
    band_id="glm_folder_fed",
    file_pattern="CG_GLM-L2-GLMF",
    vmin=1.0,
    vmax=128.0,  # flashes / cell (LogNorm)
    palette_name="GLM_FOLDER_FED_PALETTE",
    s3_tiles_prefix="tiles/glm_fed",
    s3_cog_prefix="cog/glm_fed",
    product_name="GLM_Flash_Extent_Density",
)

GLM_FOLDER_TOE_CONFIG = BandConfig(
    band_id="glm_folder_toe",
    file_pattern="CG_GLM-L2-GLMF",
    vmin=0.01,
    vmax=1500.0,  # fJ / cell (LogNorm)
    palette_name="GLM_FOLDER_TOE_PALETTE",
    s3_tiles_prefix="tiles/glm_toe",
    s3_cog_prefix="cog/glm_toe",
    product_name="GLM_Total_Optical_Energy",
)

GLM_FOLDER_MFA_CONFIG = BandConfig(
    band_id="glm_folder_mfa",
    file_pattern="CG_GLM-L2-GLMF",
    vmin=64.0,
    vmax=2500.0,  # km² / cell (LogNorm)
    palette_name="GLM_FOLDER_MFA_PALETTE",
    s3_tiles_prefix="tiles/glm_mfa",
    s3_cog_prefix="cog/glm_mfa",
    product_name="GLM_Minimum_Flash_Area",
)

# Registry for looking up band configs by ID
BAND_CONFIGS = {
    "band_13": BAND_13_CONFIG,
    "band_9": BAND_9_CONFIG,
    "band_2": BAND_2_CONFIG,
    "glm_fed": GLM_FED_CONFIG,
    "glm_toe": GLM_TOE_CONFIG,
    "glm_mfa": GLM_MFA_CONFIG,
    "glm_folder_fed": GLM_FOLDER_FED_CONFIG,
    "glm_folder_toe": GLM_FOLDER_TOE_CONFIG,
    "glm_folder_mfa": GLM_FOLDER_MFA_CONFIG,
}


def get_band_config(band_id: str) -> BandConfig:
    """Get band configuration by ID."""
    if band_id not in BAND_CONFIGS:
        raise ValueError(
            f"Unknown band_id '{band_id}'. Valid: {list(BAND_CONFIGS.keys())}"
        )
    return BAND_CONFIGS[band_id]
