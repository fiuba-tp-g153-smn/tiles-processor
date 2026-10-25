"""Date-partitioned S3 key templates: validation and expansion over a window.

A template describes the partition folders below an input prefix, e.g.
``{yyyy}/{mm}/{dd}/`` or ``*/{yyyy}{mm}{dd}/{HH}/``. Placeholders are filled
from the scan time in UTC; a ``*`` segment matches any one folder (typically
one per radar) and is resolved by the caller with a delimiter listing.
"""

from datetime import datetime, timedelta, timezone
from string import Formatter

from models.listing_window import ListingWindow

WILDCARD = "*"

_PLACEHOLDERS = {"yyyy": "%Y", "mm": "%m", "dd": "%d", "jjj": "%j", "HH": "%H"}


def validate_key_template(template: str) -> None:
    """Raise ValueError unless every placeholder is known and ``*`` is a segment."""
    fields = {name for _, name, _, _ in Formatter().parse(template) if name is not None}
    unknown = sorted(fields - _PLACEHOLDERS.keys())
    if unknown:
        raise ValueError(
            f"unknown placeholder(s) {unknown}; allowed: {sorted(_PLACEHOLDERS)}"
        )
    if any(WILDCARD in seg and seg != WILDCARD for seg in template.split("/")):
        raise ValueError(f"'{WILDCARD}' must be a whole path segment")


def expand_time_prefixes(template: str, window: ListingWindow) -> list[str]:
    """Return the template filled for every hour (or day) the window touches.

    The step is one hour when the template has ``{HH}``, otherwise one day.
    Wildcard segments are left in place for the caller to resolve.
    """
    step = timedelta(hours=1) if "{HH}" in template else timedelta(days=1)
    moment = _floor(window.start.astimezone(timezone.utc), step)
    end = window.end.astimezone(timezone.utc)
    prefixes: dict[str, None] = {}
    while moment <= end:
        fields = {name: moment.strftime(fmt) for name, fmt in _PLACEHOLDERS.items()}
        prefixes[template.format(**fields)] = None
        moment += step
    return list(prefixes)


def _floor(moment: datetime, step: timedelta) -> datetime:
    """Truncate to the start of the hour, or of the day for a one-day step."""
    hour = moment.hour if step < timedelta(days=1) else 0
    return moment.replace(hour=hour, minute=0, second=0, microsecond=0)
