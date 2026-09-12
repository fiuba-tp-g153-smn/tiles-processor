"""Band-specific configuration for satellite image processing."""

from dataclasses import dataclass


@dataclass(frozen=True)
class BandConfig:
    """
    Configuration for a specific satellite band processing.

    This contains all band-specific parameters needed to process
    satellite imagery through the pipeline.

    Attributes:
        band_id: Identifier for the product (e.g., "goes19_abi_c13", "goes19_glm_fed")
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
    band_id="goes19_abi_c13",
    file_pattern="C13_G19",
    vmin=183.15,  # -90°C in Kelvin
    vmax=323.15,  # +50°C in Kelvin
    palette_name="CLOUD_TOPS_PALETTE",
    s3_tiles_prefix="tiles/goes19/abi/c13",
    s3_cog_prefix="cog/goes19/abi/c13",
    product_name="Cloud_Tops",
)

BAND_9_CONFIG = BandConfig(
    band_id="goes19_abi_c09",
    file_pattern="C09_G19",
    vmin=161.0,  # -112.15°C in Kelvin
    vmax=330.0,  # +56.85°C in Kelvin
    palette_name="WATER_VAPOR_PALETTE",
    s3_tiles_prefix="tiles/goes19/abi/c09",
    s3_cog_prefix="cog/goes19/abi/c09",
    product_name="Water_Vapor",
)

BAND_2_CONFIG = BandConfig(
    band_id="goes19_abi_c02",
    file_pattern="C02_G19",
    vmin=0.0,  # Reflectance factor min
    vmax=1.0,  # Reflectance factor max
    palette_name="VISIBLE_PALETTE",
    s3_tiles_prefix="tiles/goes19/abi/c02",
    s3_cog_prefix="cog/goes19/abi/c02",
    product_name="Visible",
)

# Folder-based GLM pipeline (CG_GLM-L2-GLMF inputs, LogNorm rendering).
# vmin/vmax are the SMN reference LogNorm ranges in the variable's native
# units; the processor takes log10 before normalize_and_colorize.
GLM_FOLDER_FED_CONFIG = BandConfig(
    band_id="goes19_glm_fed",
    file_pattern="CG_GLM-L2-GLMF",
    vmin=1.0,
    vmax=128.0,  # flashes / cell (LogNorm)
    palette_name="GLM_FOLDER_FED_PALETTE",
    s3_tiles_prefix="tiles/goes19/glm/fed",
    s3_cog_prefix="cog/goes19/glm/fed",
    product_name="GLM_Flash_Extent_Density",
)

GLM_FOLDER_TOE_CONFIG = BandConfig(
    band_id="goes19_glm_toe",
    file_pattern="CG_GLM-L2-GLMF",
    # ``total_energy`` is converted from nJ to fJ inside aggregate_glm_window
    # so this range matches the SMN reference (grafico_glmtools_viejo.py:122)
    # 1:1 and reads at the same magnitude as FED's (1, 128) and MFA's
    # (64, 2500).
    vmin=0.01,
    vmax=1500.0,
    palette_name="GLM_FOLDER_TOE_PALETTE",
    s3_tiles_prefix="tiles/goes19/glm/toe",
    s3_cog_prefix="cog/goes19/glm/toe",
    product_name="GLM_Total_Optical_Energy",
)

GLM_FOLDER_MFA_CONFIG = BandConfig(
    band_id="goes19_glm_mfa",
    file_pattern="CG_GLM-L2-GLMF",
    vmin=64.0,
    vmax=2500.0,  # km² / cell (LogNorm)
    palette_name="GLM_FOLDER_MFA_PALETTE",
    s3_tiles_prefix="tiles/goes19/glm/mfa",
    s3_cog_prefix="cog/goes19/glm/mfa",
    product_name="GLM_Minimum_Flash_Area",
)

# Registry for looking up band configs by ID
BAND_CONFIGS = {
    "goes19_abi_c13": BAND_13_CONFIG,
    "goes19_abi_c09": BAND_9_CONFIG,
    "goes19_abi_c02": BAND_2_CONFIG,
    "goes19_glm_fed": GLM_FOLDER_FED_CONFIG,
    "goes19_glm_toe": GLM_FOLDER_TOE_CONFIG,
    "goes19_glm_mfa": GLM_FOLDER_MFA_CONFIG,
}


def get_band_config(band_id: str) -> BandConfig:
    """Get band configuration by ID."""
    if band_id not in BAND_CONFIGS:
        raise ValueError(
            f"Unknown band_id '{band_id}'. Valid: {list(BAND_CONFIGS.keys())}"
        )
    return BAND_CONFIGS[band_id]
