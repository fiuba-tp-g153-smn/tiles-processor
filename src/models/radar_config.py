"""Radar product configuration for weather radar processing."""

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True, slots=True)
class RadarProductConfig:
    """
    Configuration for a specific radar product (variable).

    Attributes:
        product_id: Identifier (e.g., "DBZH", "VRAD", "RHOHV")
        field_name: PyART field name for the variable
        subvolume: Which subvolume to process ("01" or "02")
        colors: List of (value, RGBA) tuples for colormap
        s3_prefix: S3 key prefix for storing tiles
    """

    product_id: str
    field_name: str
    subvolume: str
    colors: Tuple[Tuple[float, Tuple[int, int, int, int]], ...]
    s3_prefix: str

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON encoding."""
        return {
            "product_id": self.product_id,
            "field_name": self.field_name,
            "subvolume": self.subvolume,
            "s3_prefix": self.s3_prefix,
        }

    @property
    def vmin(self) -> float:
        """Minimum value for normalization."""
        return min(c[0] for c in self.colors)

    @property
    def vmax(self) -> float:
        """Maximum value for normalization."""
        return max(c[0] for c in self.colors)


# Pre-defined radar product configurations
# Colors are (value, (R, G, B, A)) tuples

DBZH_CONFIG = RadarProductConfig(
    product_id="DBZH",
    field_name="reflectivity",
    subvolume="01",
    colors=(
        # Zona Baja (Azules oscuros y grisáceos)
        (-15, (60, 66, 109, 255)),    # Gris azulado muy oscuro
        (-10, (61, 78, 123, 255)),   # Azul índigo oscuro
        (-5,  (61, 89, 136, 255)),   # Azul marino
        (0,   (60, 101, 150, 255)),   # Azul medio oscuro

        # Zona Transición (Azules claros a Cyan)
        (5,   (57, 113, 163, 255)),  # Azul rey/celeste oscuro
        (10,  (47, 137, 187, 255)),  # Celeste
        (15,  (38, 163, 209, 255)),   # Cyan / Turquesa

        # Zona Verde (Lluvia ligera a moderada)
        (20,  (77, 225, 51, 255)),   # Verde Lima brillante
        (25,  (58, 176, 39, 255)),   # Verde Hoja
        (30,  (36, 114, 23, 255)),     # Verde Bosque oscuro

        # Zona Convectiva (Amarillo a Rojo)
        (35,  (213, 217, 51, 255)),   # Amarillo puro
        (40,  (214, 151, 25, 255)),   # Naranja
        (45,  (193, 0, 23, 255)),     # Rojo brillante
        (50,  (194, 0, 95, 255)),     # Rojo oscuro / Granate

        # Zona Severa (Violetas a Blanco)
        (55,  (203, 0, 205, 255)),   # Púrpura oscuro
        (60,  (223, 246, 237, 255)),   # Magenta / Fucsia
        (65,  (167, 236, 207, 255)), # Blanco puro
        (70,  (135, 223, 190, 255)), # Cyan pálido / Hielo
        (75,  (135, 223, 190, 255)), # Verde menta muy pálido
    ),
    s3_prefix="radar",
)

ZDR_CONFIG = RadarProductConfig(
    product_id="ZDR",
    field_name="differential_reflectivity",
    subvolume="01",
    colors=(
        (-2, (0, 0, 150, 255)),
        (-1, (0, 100, 255, 255)),
        (0, (150, 150, 150, 255)),
        (1, (255, 255, 150, 255)),
        (2, (255, 200, 0, 255)),
        (3, (255, 150, 0, 255)),
        (4, (255, 50, 0, 255)),
        (6, (150, 0, 0, 255)),
    ),
    s3_prefix="radar",
)

RHOHV_CONFIG = RadarProductConfig(
    product_id="RHOHV",
    field_name="cross_correlation_ratio",
    subvolume="01",
    colors=(
        (0.7, (150, 0, 150, 255)),
        (0.8, (100, 100, 255, 255)),
        (0.85, (0, 200, 255, 255)),
        (0.9, (0, 255, 150, 255)),
        (0.95, (150, 255, 0, 255)),
        (0.97, (255, 255, 0, 255)),
        (1.0, (255, 150, 0, 255)),
    ),
    s3_prefix="radar",
)

KDP_CONFIG = RadarProductConfig(
    product_id="KDP",
    field_name="specific_differential_phase",
    subvolume="01",
    colors=(
        (-1, (100, 100, 100, 255)),
        (0, (0, 150, 255, 255)),
        (0.5, (0, 255, 200, 255)),
        (1, (0, 255, 0, 255)),
        (2, (255, 255, 0, 255)),
        (3, (255, 150, 0, 255)),
        (5, (255, 0, 0, 255)),
    ),
    s3_prefix="radar",
)

VRAD_CONFIG = RadarProductConfig(
    product_id="VRAD",
    field_name="velocity",
    subvolume="02",  # VRAD uses volume 02
    colors=(
        (-30, (0, 100, 255, 255)),
        (-20, (0, 180, 255, 255)),
        (-10, (100, 255, 255, 255)),
        (0, (200, 200, 200, 255)),
        (10, (255, 255, 100, 255)),
        (20, (255, 180, 0, 255)),
        (30, (255, 100, 0, 255)),
    ),
    s3_prefix="radar",
)

# Registry for looking up radar product configs by ID
RADAR_PRODUCT_CONFIGS = {
    "DBZH": DBZH_CONFIG,
    "ZDR": ZDR_CONFIG,
    "RHOHV": RHOHV_CONFIG,
    "KDP": KDP_CONFIG,
    "VRAD": VRAD_CONFIG,
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
