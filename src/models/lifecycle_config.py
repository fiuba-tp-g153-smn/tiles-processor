"""S3 lifecycle retention: output-prefix wiring + settings resolution.

Objects in the tiles bucket expire via portable S3 bucket lifecycle rules — one
expiration rule per prefix, with no empty-prefix catch-all (overlap-resolution
semantics differ across AWS/MinIO/SeaweedFS). The prefix stems below are
structural: they MUST match what the uploaders write, so they live in code, not
settings. The *day counts* are policy and come from settings.json per source,
resolved here into a concrete ``{prefix: days}`` map.
"""

from typing import Any

# Output S3 key-prefix stems each source writes, grouped by output kind. Every
# uploader writes under one of these; changing them is a code change.
SOURCE_LIFECYCLE_PREFIXES: dict[str, dict[str, str]] = {
    "goes19-abi": {"tiles": "tiles/goes19/abi", "cog": "cog/goes19/abi"},
    "goes19-glm": {"tiles": "tiles/goes19/glm", "cog": "cog/goes19/glm"},
    "radar-sinarame": {"tiles": "tiles/radar/sinarame", "cog": "cog/radar/sinarame"},
    "wrf-arg4k": {
        "tiles": "tiles/wrf-arg4k",
        "cog": "cog/wrf-arg4k",
        "geojson": "geojson/wrf-arg4k",
    },
    "ecmwf-ifs": {
        "tiles": "tiles/ecmwf-ifs",
        "cog": "cog/ecmwf-ifs",
        "geojson": "geojson/ecmwf-ifs",
        "grib": "grib/ecmwf-ifs",
    },
    "gfs": {
        "tiles": "tiles/gfs",
        "cog": "cog/gfs",
        "geojson": "geojson/gfs",
        "grib": "grib/gfs",
    },
}

OUTPUT_KINDS = frozenset({"tiles", "cog", "geojson", "grib"})
DEFAULT_RETENTION_DAYS = 1


def resolve_retention_map(sources_cfg: dict[str, Any]) -> dict[str, int]:
    """Expand per-source ``retention_days`` into a concrete ``{prefix: days}`` map.

    Each source's ``retention_days`` is an int (uniform across its output
    prefixes) or an object ``{"default": N, "<kind>": M}`` overriding one kind
    (e.g. ECMWF grib). A source without the setting falls back to the default.
    """
    resolved: dict[str, int] = {}
    for source, kind_prefixes in SOURCE_LIFECYCLE_PREFIXES.items():
        raw = sources_cfg.get(source, {}).get("retention_days")
        for kind, prefix in kind_prefixes.items():
            resolved[prefix] = _days_for(source, raw, kind)
    return resolved


def _days_for(source: str, raw: Any, kind: str) -> int:
    """Resolve the retention (in days) for one (source, output-kind)."""
    if raw is None:
        return DEFAULT_RETENTION_DAYS
    if isinstance(raw, dict):
        _reject_unknown_kinds(source, raw)
        days = raw.get(kind, raw.get("default", DEFAULT_RETENTION_DAYS))
        return _validate_days(source, days)
    return _validate_days(source, raw)


def _reject_unknown_kinds(source: str, raw: dict) -> None:
    """Reject typo'd override keys so a stray key can't silently do nothing."""
    unknown = set(raw) - OUTPUT_KINDS - {"default"}
    if unknown:
        raise ValueError(
            f"sources.{source}.retention_days has unknown keys {sorted(unknown)}; "
            f"valid: 'default' + {sorted(OUTPUT_KINDS)}"
        )


def _validate_days(source: str, days: Any) -> int:
    """A retention must be a whole number of days >= 1 (bool is not an int here)."""
    if isinstance(days, bool) or not isinstance(days, int) or days < 1:
        raise ValueError(
            f"sources.{source}.retention_days must be integers >= 1, got {days!r}"
        )
    return days
