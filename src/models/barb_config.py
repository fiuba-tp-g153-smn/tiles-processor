"""Per-source wind-barb stride policy parsed from settings.json.

Barb density is per-source policy because the stride is a *grid-index* step,
not a distance: the same stride over WRF-ARG4K (~4 km) and over GFS (0.25 deg,
~28 km) yields separations that differ by roughly 7x. Each source that emits
barbs ships its own zoom -> stride mapping.

Zooms are capped in settings.json rather than in code: zooms sharing a stride
would extract the *identical* barb points and only re-bucket them into more
tiny GeoJSON files, so the frontend overzooms the deepest barb tileset instead.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BarbZoomStrides:
    """Web-Mercator zoom -> grid subsample stride for barb tiles."""

    entries: tuple[tuple[int, int], ...]

    def items(self) -> tuple[tuple[int, int], ...]:
        """The (zoom, stride) pairs, ascending by zoom."""
        return self.entries

    def stride_for(self, zoom: int) -> int:
        """The stride configured for `zoom`, or KeyError if it carries no barbs."""
        for configured, stride in self.entries:
            if configured == zoom:
                return stride
        raise KeyError(f"No barb stride configured for zoom {zoom}")

    @property
    def zooms(self) -> frozenset[int]:
        """The zoom levels that get a barb tileset."""
        return frozenset(zoom for zoom, _stride in self.entries)


def parse_barb_zoom_strides(
    raw, name: str, *, default: dict[int, int]
) -> BarbZoomStrides:
    """Parse a ``{"<zoom>": <stride>}`` mapping, falling back to `default`.

    JSON object keys are strings, so zooms are cast here. Fails fast on an
    empty mapping, a non-integer zoom or a stride below 1, so a typo cannot
    silently drop a barb layer or subsample with a degenerate step.
    """
    value = default if raw is None else raw
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{name} must be a non-empty zoom -> stride object")

    entries: list[tuple[int, int]] = []
    for zoom, stride in value.items():
        entries.append((_as_zoom(zoom, name), _as_stride(stride, name, zoom)))
    return BarbZoomStrides(tuple(sorted(entries)))


def _as_zoom(zoom, name: str) -> int:
    """Coerce a mapping key to a zoom level."""
    try:
        return int(zoom)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} has a non-integer zoom key {zoom!r}") from exc


def _as_stride(stride, name: str, zoom) -> int:
    """Coerce a mapping value to a stride of at least 1."""
    if not isinstance(stride, int) or isinstance(stride, bool) or stride < 1:
        raise ValueError(
            f"{name} zoom {zoom} stride must be an int >= 1, got {stride!r}"
        )
    return stride
