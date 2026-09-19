"""Identity of a single radar scan, resolved from its source file."""

from dataclasses import dataclass

from models.radar_config import RadarProductConfig


@dataclass(frozen=True, slots=True)
class ScanIdentity:
    """What a radar file is, independent of the format it arrived in.

    Resolving this is the only part of the radar pipeline that differs between
    the SINARAME (ODIM-HDF5) and INTA (Rainbow5) feeds; everything downstream —
    the science, the palette, the tiling, the S3 layout — works off these four
    values alone.

    Attributes:
        radar_id: Station id, used as the S3 path segment (``RMA1``, ``PAR``).
        timestamp: Scan time as ``YYYYMMDDTHHMMSSZ``, the tileset id.
        product_config: The product this scan is published as. Carries the S3
            prefix, so it is also what puts the scan in its network's namespace.
        variable_id: The physical moment to render. Selects the PyART field, the
            palette and the mask, and is *not* always the product id — the
            long-range dbzh-450km product renders the plain ``DBZH`` moment.
    """

    radar_id: str
    timestamp: str
    product_config: RadarProductConfig
    variable_id: str
