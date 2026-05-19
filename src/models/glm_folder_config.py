"""
GLM gridded-product (folder ingestion) configuration and filename parsing.

Sample filename:
    CG_GLM-L2-GLMF-M3_G19_s20260611400000_e20260611401000_c20260611402030.nc

Segments (after the `_s` / `_e` / `_c` prefixes):
    YYYY (4) + JJJ (3, day-of-year) + HH (2) + MM (2) + SS (2) + D (1, deciseconds)
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final


_TS_TOKEN_LEN: Final[int] = 14  # YYYY+JJJ+HH+MM+SS+D


@dataclass(frozen=True, slots=True)
class GlmFolderFilenameParts:
    """Parsed components of a CG_GLM-L2-GLMF-M3 filename."""

    platform: str  # e.g. "G19"
    mode: str  # e.g. "M3"
    start_dt: datetime  # tz-aware UTC
    end_dt: datetime  # tz-aware UTC


def _parse_goes_timestamp(token: str) -> datetime:
    """Parse a GOES-style timestamp `YYYYJJJHHMMSSD` into a tz-aware UTC datetime.

    Deciseconds (`D`) are preserved as microseconds (`D * 100_000` µs).
    """
    if len(token) != _TS_TOKEN_LEN or not token.isdigit():
        raise ValueError(f"Invalid GOES timestamp token: {token!r}")
    return datetime(int(token[0:4]), 1, 1, tzinfo=timezone.utc) + timedelta(
        days=int(token[4:7]) - 1,
        hours=int(token[7:9]),
        minutes=int(token[9:11]),
        seconds=int(token[11:13]),
        microseconds=int(token[13:14]) * 100_000,
    )


def parse_glm_folder_filename(filename: str) -> GlmFolderFilenameParts:
    """Parse a CG_GLM-L2-GLMF-M? filename into its components.

    Expected pattern::

        CG_GLM-L2-GLMF-{mode}_{platform}_s{YYYYJJJHHMMSSD}_e{YYYYJJJHHMMSSD}_c{YYYYJJJHHMMSSD}.nc

    Raises:
        ValueError: When the filename does not match the expected pattern.
    """
    stem = filename
    if stem.endswith(".nc"):
        stem = stem[:-3]

    parts = stem.split("_")
    if len(parts) < 6 or parts[0] != "CG" or not parts[1].startswith("GLM-L2-GLMF-"):
        raise ValueError(f"Not a CG_GLM-L2-GLMF filename: {filename!r}")

    mode = parts[1].rsplit("-", 1)[1]
    platform = parts[2]
    start_token = parts[3]
    end_token = parts[4]

    if not (start_token.startswith("s") and end_token.startswith("e")):
        raise ValueError(f"Unexpected timestamp prefixes in: {filename!r}")

    return GlmFolderFilenameParts(
        platform=platform,
        mode=mode,
        start_dt=_parse_goes_timestamp(start_token[1:]),
        end_dt=_parse_goes_timestamp(end_token[1:]),
    )
