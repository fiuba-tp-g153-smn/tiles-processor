"""Pulls a GFS GRIB2 subset for one (cycle, forecast step) from a NOMADS mirror."""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlencode

import requests

from exceptions import (
    ForecastNotAvailableError,
    TransientDownloadError,
    UnprocessableInputError,
)
from models.gfs_config import (
    GFS_EXPECTED_MESSAGE_COUNT,
    GFS_LEVELS,
    GFS_VARIABLES,
    GfsAccessConfig,
)

logger = logging.getLogger(__name__)

_GRIB_MAGIC = b"GRIB"
_MAX_LOGGED_BODY_BYTES = 500


class InvalidGribResponseError(UnprocessableInputError):
    """The endpoint answered, but not with the GRIB we asked for.

    Deterministic and not worth retrying: it means the endpoint is misconfigured
    or serving a different payload than expected. Inherits
    ``UnprocessableInputError`` so the worker acks it as SKIPPED with the reason
    attached, instead of burning retries against an endpoint that will keep
    answering the same thing. The fetcher also logs the offending body at ERROR.
    """


class GfsGribFetcher:
    """Fetches one GFS forecast step through the NOMADS grib_filter CGI."""

    def __init__(self, access_config: GfsAccessConfig, bounds: dict[str, float]):
        self._endpoint = access_config.require_endpoint()
        self._access = access_config
        self._bounds = bounds
        self._semaphore = asyncio.Semaphore(access_config.max_concurrent_downloads)

    @property
    def endpoint(self) -> str:
        """The effective endpoint this fetcher talks to (logged on every call)."""
        return self._endpoint

    async def fetch(self, cycle: datetime, step_hours: int, dest: Path) -> Path:
        """Download the GRIB subset for `cycle`/`step_hours` into `dest`.

        Raises:
            ForecastNotAvailableError: the run is not published yet (skip).
            TransientDownloadError: 5xx, timeout, connection error or a
                truncated body (requeue — retrying can succeed).
            InvalidGribResponseError: answered with a complete body that is not
                the GRIB we asked for (misconfiguration — do not retry).
        """
        target = dest.with_suffix(".grib2")
        target.parent.mkdir(parents=True, exist_ok=True)

        async with self._semaphore:
            await asyncio.to_thread(self._download, cycle, step_hours, target)

        self._validate(target, cycle, step_hours)
        logger.info(
            "[GFS] Fetched %s f%03d (%.2f MB) from %s",
            _fmt_cycle(cycle),
            step_hours,
            target.stat().st_size / 1e6,
            self.endpoint,
        )
        return target

    def build_url(self, cycle: datetime, step_hours: int) -> str:
        """Build the full grib_filter request URL for one forecast step."""

        params: list[tuple[str, str]] = [
            ("file", f"gfs.t{cycle.hour:02d}z.pgrb2.0p25.f{step_hours:03d}")
        ]
        params += [(f"lev_{level}", "on") for level in GFS_LEVELS]
        params += [(f"var_{var}", "on") for var in GFS_VARIABLES]
        params.append(("subregion", ""))
        params += self._bbox_params()
        params.append(("dir", f"/gfs.{cycle:%Y%m%d}/{cycle.hour:02d}/atmos"))
        return f"{self.endpoint}?{urlencode(params)}"

    def _download(self, cycle: datetime, step_hours: int, target: Path) -> None:
        """Stream the response body to `target`."""
        url = self.build_url(cycle, step_hours)
        where = f"{_fmt_cycle(cycle)} f{step_hours:03d}"
        logger.info("[GFS] GET %s → %s", where, self.endpoint)
        try:
            with requests.get(
                url, timeout=self._access.timeout_seconds, stream=True
            ) as response:
                self._raise_for_status(response.status_code, cycle, step_hours)
                target.unlink(missing_ok=True)
                with open(target, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=1 << 16):
                        handle.write(chunk)
        except requests.exceptions.Timeout as exc:
            raise TransientDownloadError(
                f"Timeout after {self._access.timeout_seconds}s from "
                f"{self.endpoint} for {where}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise TransientDownloadError(
                f"Transfer from {self.endpoint} failed for {where}: {exc}"
            ) from exc

    def _bbox_params(self) -> list[tuple[str, str]]:
        """Translate the project's bounds into the CGI's bbox parameters.

        The project works in -180..180 while NOMADS expects 0-360, so western
        longitudes shift by a full turn (-110 -> 250).
        """
        return [
            ("leftlon", _fmt_lon(self._bounds["minx"])),
            ("rightlon", _fmt_lon(self._bounds["maxx"])),
            ("toplat", _fmt_coord(self._bounds["maxy"])),
            ("bottomlat", _fmt_coord(self._bounds["miny"])),
        ]

    def _raise_for_status(self, status: int, cycle: datetime, step_hours: int) -> None:
        """Map the CGI's HTTP status onto the worker's retry semantics."""
        if status == 200:
            return
        where = f"{_fmt_cycle(cycle)} f{step_hours:03d}"
        if status in (403, 404):
            raise ForecastNotAvailableError(
                f"GFS run {where} not available at {self.endpoint} (HTTP {status})"
            )
        if status >= 500:
            raise TransientDownloadError(
                f"{self.endpoint} returned HTTP {status} for {where}"
            )
        raise InvalidGribResponseError(
            f"{self.endpoint} returned HTTP {status} for {where} — "
            "likely an invalid request parameter"
        )

    def _validate(self, target: Path, cycle: datetime, step_hours: int) -> None:
        """Reject anything that is not the GRIB subset we asked for."""

        self._assert_is_grib(target)
        self._assert_message_count(target, cycle, step_hours)

    def _assert_is_grib(self, target: Path) -> None:
        """Check the GRIB2 magic bytes, echoing the body when they are missing."""
        with open(target, "rb") as handle:
            magic = handle.read(len(_GRIB_MAGIC))
        if magic == _GRIB_MAGIC:
            return

        with open(target, "rb") as handle:
            body = handle.read(_MAX_LOGGED_BODY_BYTES)
        target.unlink(missing_ok=True)
        logger.error(
            "[GFS] Endpoint %s returned a non-GRIB body (first %d bytes): %r",
            self.endpoint,
            len(body),
            body,
        )
        raise InvalidGribResponseError(
            f"Endpoint {self.endpoint} did not return a GRIB2 payload "
            f"(magic bytes were {magic!r})"
        )

    def _assert_message_count(
        self, target: Path, cycle: datetime, step_hours: int
    ) -> None:
        """Check the subset carries every message the products need.

        Two very different faults produce a short file, and they need opposite
        handling, so the scan separates them:

        * **Truncated** — the walk runs past the end of the file, i.e. the last
          message is cut short. A network hiccup or a proxy giving up. Transient:
          retrying gets a whole file.
        * **Well-formed but short** — the walk lands exactly on EOF with fewer
          messages than expected. The endpoint genuinely carries fewer
          levels/variables. Permanent: retrying gets the same thing.
        """
        scan = _scan_grib(target)
        if scan.ends_at_eof and scan.message_count == GFS_EXPECTED_MESSAGE_COUNT:
            return

        size = target.stat().st_size
        where = f"{_fmt_cycle(cycle)} f{step_hours:03d}"
        target.unlink(missing_ok=True)

        if not scan.ends_at_eof:
            raise TransientDownloadError(
                f"Truncated GRIB from {self.endpoint} for {where}: "
                f"{scan.message_count} complete messages in {size} bytes and the "
                "next one runs past the end of the file"
            )
        raise InvalidGribResponseError(
            f"Endpoint {self.endpoint} returned {scan.message_count} GRIB messages "
            f"for {where} (expected {GFS_EXPECTED_MESSAGE_COUNT}, {size} bytes). "
            "The endpoint may carry fewer levels/variables than the products need."
        )


class _GribScan(NamedTuple):
    """What walking a GRIB file's section-0 headers found."""

    message_count: int
    ends_at_eof: bool


def _scan_grib(path: Path) -> _GribScan:
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
            if len(header) < 16 or header[:4] != _GRIB_MAGIC:
                break
            message_length = int.from_bytes(header[8:16], "big")
            if message_length <= 0:
                break
            if offset + message_length > size:
                offset += message_length
                break
            count += 1
            offset += message_length
    return _GribScan(message_count=count, ends_at_eof=offset == size)


def _fmt_lon(lon: float) -> str:
    """Format a longitude for the CGI, shifting -180..180 into 0-360."""
    return _fmt_coord(lon + 360.0 if lon < 0 else lon)


def _fmt_coord(value: float) -> str:
    """Render a coordinate without a trailing ``.0`` for whole degrees."""
    return f"{value:g}"


def _fmt_cycle(cycle: datetime) -> str:
    """Format a cycle as YYYYMMDDtHHz (e.g. 20260808t00z)."""
    return f"{cycle:%Y%m%d}t{cycle.hour:02d}z"
