"""Time window a discovery tick asks a file repository to cover."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ListingWindow:
    """Scan times from ``start`` to ``end`` (UTC) that discovery will keep.

    A repository may use it to list less than everything (only the date
    partitions of a bucket that overlap it) and to share one listing between
    every source asking for the same window in a tick. Discovery still filters
    each key by the timestamp in its name, so a repository that ignores the
    window stays correct, only slower.
    """

    start: datetime
    end: datetime
