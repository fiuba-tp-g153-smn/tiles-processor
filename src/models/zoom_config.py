"""Per-source tile zoom levels parsed from settings.json.

Zoom depth is the biggest driver of tile count / S3 storage / gdal2tiles
runtime, so it is per-source policy in settings.json. Each source ships a
``"MIN-MAX"`` spec; this module validates it and exposes both the gdal2tiles
string and the derived max zoom (used for prewarp resolution and fill-missing).
"""

import re
from dataclasses import dataclass

_ZOOM_RE = re.compile(r"^(\d+)-(\d+)$")


@dataclass(frozen=True, slots=True)
class ZoomLevels:
    """An inclusive gdal2tiles zoom range, e.g. min=3, max=7."""

    min_zoom: int
    max_zoom: int

    @property
    def spec(self) -> str:
        """The ``"MIN-MAX"`` string gdal2tiles expects (``-z``)."""
        return f"{self.min_zoom}-{self.max_zoom}"


def parse_zoom_levels(raw, name: str, *, default: str) -> ZoomLevels:
    """Parse a ``"MIN-MAX"`` zoom spec, falling back to ``default`` when unset.

    Fails fast on a malformed spec or an inverted range so a typo can't produce
    an empty or nonsensical tile pyramid.
    """
    value = raw if raw is not None else default
    match = _ZOOM_RE.match(value) if isinstance(value, str) else None
    if not match:
        raise ValueError(f'{name} must be a "MIN-MAX" zoom string, got {value!r}')
    lo, hi = int(match.group(1)), int(match.group(2))
    if lo > hi:
        raise ValueError(f"{name} min zoom {lo} must not exceed max zoom {hi}")
    return ZoomLevels(lo, hi)
