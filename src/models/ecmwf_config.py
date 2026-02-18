"""ECMWF model product configuration for precipitation processing."""

from dataclasses import dataclass


@dataclass(frozen=True)
class EcmwfProductConfig:
    """
    Configuration for ECMWF model product processing.

    This contains all product-specific parameters needed to process
    ECMWF forecast data through the pipeline.

    Attributes:
        product_id: Identifier for the product (e.g., "precipitation")
        parameter: ECMWF parameter code (e.g., "tp" for total precipitation)
        vmin: Minimum value for normalization (mm)
        vmax: Maximum value for normalization (mm)
        palette_name: Name of the color palette to use
        s3_prefix: S3 key prefix for storing tiles
        product_name: Name for the output product metadata
    """

    product_id: str
    parameter: str
    vmin: float
    vmax: float
    palette_name: str
    s3_prefix: str
    product_name: str

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON encoding."""
        return {
            "product_id": self.product_id,
            "parameter": self.parameter,
            "vmin": self.vmin,
            "vmax": self.vmax,
            "palette_name": self.palette_name,
            "s3_prefix": self.s3_prefix,
            "product_name": self.product_name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "EcmwfProductConfig":
        """Deserialize from dictionary."""
        return cls(
            product_id=data["product_id"],
            parameter=data["parameter"],
            vmin=data["vmin"],
            vmax=data["vmax"],
            palette_name=data["palette_name"],
            s3_prefix=data["s3_prefix"],
            product_name=data["product_name"],
        )


# Pre-defined ECMWF product configurations
ECMWF_PRECIP_CONFIG = EcmwfProductConfig(
    product_id="precipitation",
    parameter="tp",  # Total precipitation
    vmin=0.0,  # Minimum precipitation in mm
    vmax=100.0,  # Maximum precipitation in mm
    palette_name="PRECIPITATION_PALETTE",
    s3_prefix="ecmwf_precipitation/tiles",
    product_name="ECMWF_Total_Precipitation",
)

# Registry for looking up ECMWF configs by ID
ECMWF_CONFIGS = {
    "precipitation": ECMWF_PRECIP_CONFIG,
}
