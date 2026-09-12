"""Pure validation of a GFS GRIB2 payload, shared by every GFS input backend.

The same guarantees are wanted whether the bytes arrived from the NOMADS CGI,
a folder or a bucket: they must be a GRIB2 file, and they must carry every
message the products read. Keeping the checks here means a file dropped into an
input folder is held to exactly the standard the HTTP path enforces.
"""

import logging
from pathlib import Path
from typing import NamedTuple

from exceptions import InvalidGribResponseError, TransientDownloadError
from models.gfs_config import GFS_EXPECTED_MESSAGE_COUNT

logger = logging.getLogger(__name__)

GRIB_MAGIC = b"GRIB"
MAX_LOGGED_BODY_BYTES = 500


class GribScan(NamedTuple):
    """What walking a GRIB file's section-0 headers found."""

    message_count: int
    ends_at_eof: bool


def validate_gfs_grib(target: Path, source_label: str, where: str) -> None:
    """Reject anything that is not the GRIB subset the products need.

    Args:
        target: The file to check; removed when it fails.
        source_label: Where the bytes came from, for the error text.
        where: Which (cycle, step) this is, for the error text.

    Raises:
        TransientDownloadError: the file is truncated (retrying can succeed).
        InvalidGribResponseError: the file is complete but is not the expected
            GRIB subset (retrying gets the same thing).
    """
    assert_is_grib(target, source_label)
    assert_message_count(target, source_label, where)


def assert_is_grib(target: Path, source_label: str) -> None:
    """Check the GRIB2 magic bytes, echoing the body when they are missing."""
    with open(target, "rb") as handle:
        magic = handle.read(len(GRIB_MAGIC))
    if magic == GRIB_MAGIC:
        return

    with open(target, "rb") as handle:
        body = handle.read(MAX_LOGGED_BODY_BYTES)
    target.unlink(missing_ok=True)
    logger.error(
        "[GFS] Source %s returned a non-GRIB body (first %d bytes): %r",
        source_label,
        len(body),
        body,
    )
    raise InvalidGribResponseError(
        f"Source {source_label} did not return a GRIB2 payload "
        f"(magic bytes were {magic!r})"
    )


def assert_message_count(target: Path, source_label: str, where: str) -> None:
    """Check the subset carries every message the products need.

    Two very different faults produce a short file, and they need opposite
    handling, so the scan separates them:

    * **Truncated** — the walk runs past the end of the file, i.e. the last
      message is cut short. A network hiccup or a proxy giving up. Transient:
      retrying gets a whole file.
    * **Well-formed but short** — the walk lands exactly on EOF with fewer
      messages than expected. The source genuinely carries fewer
      levels/variables. Permanent: retrying gets the same thing.
    """
    scan = scan_grib(target)
    if scan.ends_at_eof and scan.message_count == GFS_EXPECTED_MESSAGE_COUNT:
        return

    size = target.stat().st_size
    target.unlink(missing_ok=True)

    if not scan.ends_at_eof:
        raise TransientDownloadError(
            f"Truncated GRIB from {source_label} for {where}: "
            f"{scan.message_count} complete messages in {size} bytes and the "
            "next one runs past the end of the file"
        )
    raise InvalidGribResponseError(
        f"Source {source_label} returned {scan.message_count} GRIB messages "
        f"for {where} (expected {GFS_EXPECTED_MESSAGE_COUNT}, {size} bytes). "
        "The source may carry fewer levels/variables than the products need."
    )


def scan_grib(path: Path) -> GribScan:
    """Walk GRIB2 section 0 headers, counting messages and checking the ending.

    Each message starts with ``GRIB`` followed by 4 reserved/discipline bytes,
    an edition byte and a big-endian uint64 total length, so the file can be
    traversed without decoding any data."""
    count = 0
    size = path.stat().st_size
    offset = 0
    with open(path, "rb") as handle:
        while offset < size:
            handle.seek(offset)
            header = handle.read(16)
            if len(header) < 16 or header[:4] != GRIB_MAGIC:
                break
            message_length = int.from_bytes(header[8:16], "big")
            if message_length <= 0:
                break
            if offset + message_length > size:
                offset += message_length
                break
            count += 1
            offset += message_length
    return GribScan(message_count=count, ends_at_eof=offset == size)
