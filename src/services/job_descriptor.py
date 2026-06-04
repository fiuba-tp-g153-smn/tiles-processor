"""Derive human-friendly labels for a work unit's job type.

The metrics dashboard groups statistics by ``job_type`` (always the work unit's
``data_source_id`` — already unique per band/product) and shows a friendly
``product_label`` plus the scene/forecast ``image_timestamp`` for each job.

This module reuses the existing config registries (band/radar/WRF/ECMWF) for the
friendly names so labels stay accurate without duplicating domain knowledge.
Every helper is best-effort: an unrecognized id falls back to raw values and
never raises, so a labeling gap can never break metrics recording.
"""

from dataclasses import dataclass

from models.band_config import BAND_CONFIGS
from models.radar_config import RADAR_PRODUCT_CONFIGS
from models.wrf_config import WRF_PRODUCT_CONFIGS


@dataclass(frozen=True, slots=True)
class JobDescription:
    """Display metadata derived from a work unit."""

    job_type: str
    product_label: str
    image_timestamp: str


def describe_job(data_source_id: str, image_id: str, band_id: str) -> JobDescription:
    """Build (job_type, product_label, image_timestamp) for a work unit.

    Args:
        data_source_id: The work unit's data source id (the grouping key).
        image_id: The work unit's image id (encodes the scene/forecast time).
        band_id: The work unit's band id (used for GOES product names).
    """
    label, timestamp = _label_and_timestamp(data_source_id, image_id, band_id)
    return JobDescription(
        job_type=data_source_id,
        product_label=label,
        image_timestamp=timestamp,
    )


def _label_and_timestamp(
    data_source_id: str, image_id: str, band_id: str
) -> tuple[str, str]:
    """Dispatch to the per-family labeler, falling back to raw values."""
    try:
        if data_source_id.startswith("goes19_abi_"):
            return _describe_goes(band_id, image_id)
        if data_source_id.startswith("radar_"):
            return _describe_radar(data_source_id, image_id)
        if data_source_id.startswith("glm_folder"):
            return ("GLM Lightning (FED/TOE/MFA)", image_id)
        if data_source_id.startswith("wrf_"):
            return _describe_wrf(data_source_id, image_id)
        if data_source_id.startswith("ecmwf_"):
            return (f"ECMWF {data_source_id.removeprefix('ecmwf_')}", image_id)
    except (ValueError, IndexError, KeyError):
        pass
    return (data_source_id, image_id)


def _describe_goes(band_id: str, image_id: str) -> tuple[str, str]:
    """GOES ABI: friendly product name from the band config."""
    config = BAND_CONFIGS.get(band_id)
    product = config.product_name.replace("_", " ") if config else band_id
    return (f"GOES ABI {band_id} · {product}", image_id)


def _describe_radar(data_source_id: str, image_id: str) -> tuple[str, str]:
    """Radar: ``image_id`` is ``{radar_id}_{variable}_{timestamp}``."""
    product_id = data_source_id.removeprefix("radar_")
    config = RADAR_PRODUCT_CONFIGS.get(product_id)
    long_name = config.long_name if config else product_id

    radar_id, variable, timestamp = "", product_id, image_id
    parts = image_id.split("_")
    if len(parts) >= 3:
        radar_id, variable, timestamp = parts[0], parts[1], parts[2]

    station = f"{radar_id} " if radar_id else ""
    return (f"Radar {station}{variable} · {long_name}", timestamp)


def _describe_wrf(data_source_id: str, image_id: str) -> tuple[str, str]:
    """WRF: ``image_id`` is ``{product}_{init_tag}_{fxxx}``."""
    product_id = data_source_id.removeprefix("wrf_")
    config = WRF_PRODUCT_CONFIGS.get(product_id)
    long_name = config.long_name if config else product_id
    # Strip the leading product token to leave "{init_tag}_{fxxx}".
    timestamp = image_id.removeprefix(f"{product_id}_") or image_id
    return (f"WRF {product_id} · {long_name}", timestamp)
