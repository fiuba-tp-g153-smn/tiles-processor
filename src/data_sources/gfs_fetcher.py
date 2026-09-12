"""Pulls a GFS GRIB2 subset for one (cycle, forecast step) from a NOMADS mirror."""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import requests

from data_sources.gfs_repository import GfsGribRepository, describe_step
from exceptions import (
    ForecastNotAvailableError,
    InvalidGribResponseError,
    TransientDownloadError,
)
from models.gfs_config import GFS_LEVELS, GFS_VARIABLES, GfsAccessConfig
from services.gfs_grib_validation import validate_gfs_grib

logger = logging.getLogger(__name__)


class GfsGribFetcher(GfsGribRepository):
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

    @property
    def source_label(self) -> str:
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

        validate_gfs_grib(target, self.endpoint, describe_step(cycle, step_hours))
        logger.info(
            "[GFS] Fetched %s (%.2f MB) from %s",
            describe_step(cycle, step_hours),
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
        where = describe_step(cycle, step_hours)
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
        where = describe_step(cycle, step_hours)
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


def _fmt_lon(lon: float) -> str:
    """Format a longitude for the CGI, shifting -180..180 into 0-360."""
    return _fmt_coord(lon + 360.0 if lon < 0 else lon)


def _fmt_coord(value: float) -> str:
    """Render a coordinate without a trailing ``.0`` for whole degrees."""
    return f"{value:g}"
