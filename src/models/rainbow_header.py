"""Rainbow5 (.vol) header parsing for the INTA radars.

Rainbow5 files are an XML preamble followed by binary blobs. Everything needed
to identify and route a scan lives in that preamble, so it can be read without
pyart/wradlib and without pulling the (multi-megabyte) payload.

The station is read from the file rather than its name on purpose: the samples
the SMN ships are named ``2026052115100400dBZ.vol`` with no radar token, while
production names carry one (``PAR2026061920000300dBZ.vol``). The header is the
only source that works for both.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timezone

# <radarinfo> sits *after* the <pargroup> scan-parameter block — around byte
# 18_700 in every sample measured — and its offset moves with however many
# parameters that group carries. Read a window comfortably past it rather than
# assuming the head of the file.
HEADER_WINDOW_BYTES = 32768

_RADARINFO_RE = re.compile(rb"<radarinfo([^>]*)>")
_ATTR_RE = re.compile(rb'(\w+)="([^"]*)"')
_NAME_RE = re.compile(rb"<name>([^<]*)</name>")
_SCAN_NAME_RE = re.compile(rb'<scan name="([^"]*)"')
_STOP_RANGE_RE = re.compile(rb"<stoprange>([^<]*)</stoprange>")
_NUMELE_RE = re.compile(rb"<numele>([^<]*)</numele>")

# ``{RADAR}{YYYYMMDDHHMMSS}{NN}{VARIABLE}.vol`` — the radar token is absent in
# the sample files and present in production; NN is a sequence number that
# follows the 14-digit timestamp. Only ``.vol`` matches: the folders also hold
# ``.azi`` products, which are not volumetric and must never be ingested.
_FILENAME_RE = re.compile(
    r"^(?P<radar>[A-Za-z]*)(?P<timestamp>\d{14})(?P<sequence>\d{2})"
    r"(?P<variable>[A-Za-z]+)\.vol$"
)


class RainbowHeaderError(ValueError):
    """Raised when a .vol file carries no parsable Rainbow5 header."""


@dataclass(frozen=True, slots=True)
class RainbowHeader:
    """Identity and scan geometry read from a Rainbow5 preamble.

    Attributes:
        radar_id: Short station id (``PAR``), used as the S3 path segment.
        radar_name: Descriptive name (``INTA_Parana``).
        latitude: Antenna latitude in degrees.
        longitude: Antenna longitude in degrees.
        altitude_m: Antenna altitude in metres.
        stop_range_km: Maximum range of the scan. The discriminant between the
            240 km volumes this pipeline publishes and the 120 km ones ANG and
            PER interleave in the same folder.
        elevation_count: Number of sweeps in the volume.
        scan_name: Scan strategy (``VOL_240_ALL``, ``SMN_120``, …). Carried for
            logging: ``stop_range_km`` is what decisions are made on, since the
            strategy names differ between stations.
    """

    radar_id: str
    radar_name: str
    latitude: float
    longitude: float
    altitude_m: float
    stop_range_km: float
    elevation_count: int
    scan_name: str


def parse_rainbow_header(window: bytes) -> RainbowHeader:
    """Parse a Rainbow5 preamble out of the leading bytes of a .vol file.

    Args:
        window: Head of the file, at least ``HEADER_WINDOW_BYTES`` long.

    Raises:
        RainbowHeaderError: The window carries no ``<radarinfo>`` tag, either
            because the file is not Rainbow5 or because the window is too short
            for this file's parameter block.
    """
    match = _RADARINFO_RE.search(window)
    if match is None:
        raise RainbowHeaderError(
            f"no <radarinfo> tag in the first {len(window)} bytes (not a "
            "Rainbow5 volume, or its header is longer than the read window)"
        )

    attrs = {k.decode(): v.decode() for k, v in _ATTR_RE.findall(match.group(1))}
    radar_id = attrs.get("id", "").strip()
    if not radar_id:
        raise RainbowHeaderError("<radarinfo> carries no id attribute")

    return RainbowHeader(
        radar_id=radar_id.upper(),
        radar_name=_text(window, _NAME_RE, default=radar_id),
        latitude=_float(attrs, "lat"),
        longitude=_float(attrs, "lon"),
        altitude_m=_float(attrs, "alt", default=0.0),
        stop_range_km=float(_text(window, _STOP_RANGE_RE, default="0") or 0),
        elevation_count=int(_text(window, _NUMELE_RE, default="0") or 0),
        scan_name=_text(window, _SCAN_NAME_RE, default=""),
    )


def parse_inta_filename(filename: str) -> dict[str, str]:
    """Split an INTA .vol filename into its parts.

    The radar token is optional (empty string when absent) because only the
    production naming carries it; callers identify the station from the header.
    ``timestamp`` is normalized to the ``YYYYMMDDTHHMMSSZ`` form the rest of the
    pipeline — and the visualizer's tileset parser — already speaks.

    Raises:
        ValueError: The name does not follow the INTA convention (``.azi``
            products and anything else are rejected here).
    """
    match = _FILENAME_RE.match(filename)
    if match is None:
        raise ValueError(f"Invalid INTA radar filename format: {filename}")

    raw = match.group("timestamp")
    return {
        "radar_id": match.group("radar").upper(),
        "timestamp": f"{raw[:8]}T{raw[8:]}Z",
        "sequence": match.group("sequence"),
        "variable": match.group("variable"),
    }


def parse_inta_timestamp(timestamp: str) -> datetime:
    """Parse a normalized ``YYYYMMDDTHHMMSSZ`` timestamp into an aware datetime."""
    return datetime.strptime(timestamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def _text(window: bytes, pattern: re.Pattern[bytes], default: str) -> str:
    match = pattern.search(window)
    return match.group(1).decode().strip() if match else default


def _float(attrs: dict[str, str], key: str, default: float | None = None) -> float:
    raw = attrs.get(key)
    if raw is None:
        if default is None:
            raise RainbowHeaderError(f"<radarinfo> carries no {key} attribute")
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RainbowHeaderError(f"<radarinfo> {key}={raw!r} is not a number") from exc
