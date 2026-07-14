"""ECMWF producer data source: discovers missing GRIBs and downloads from ECMWF API."""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from ecmwf.opendata import Client

import requests

from clients.s3_client import S3Client
from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from models.ecmwf_config import (
    ECMWF_TP_CONFIG,
    FORECASTS_TO_MAINTAIN,
    MAX_LOOKBACK_HOURS,
    STEP_HOURS,
    EcmwfProductConfig,
)

logger = logging.getLogger(__name__)


class ForecastNotAvailableError(Exception):
    """Raised when a forecast candidate is not yet published on ECMWF Open Data (HTTP 404)."""


class TransientDownloadError(Exception):
    """Raised on transient S3 errors (e.g. 503 Slow Down) to trigger requeue instead of blocking."""


_FORECAST_BASE_HOURS = (0, 12)  # UTC hours at which ECMWF issues forecasts
_STEPS = list(range(STEP_HOURS, 145, STEP_HOURS))  # [3, 6, ..., 144]

_DEFAULT_OPENDATA_SOURCES = ("ecmwf", "azure", "aws")


class EcmwfProducerDataSource(DataSource):
    """
    Data source for ECMWF GRIB discovery (used by the producer).

    Responsibilities:
    - Calculate the N most recent available forecast timestamps.
    - Check which GRIBs are missing from the S3 cache.
    - Return ImageInfo for each missing GRIB; the worker handles download and period enqueuing.
    - Download the GRIB from the ECMWF Open Data API when the worker calls download().
    """

    def __init__(
        self,
        product_config: EcmwfProductConfig = ECMWF_TP_CONFIG,
        s3_client: S3Client | None = None,
        sources: tuple[str, ...] | None = None,
    ):
        self._product_config = product_config
        self._s3_client = s3_client
        self._sources = tuple(sources) if sources else _DEFAULT_OPENDATA_SOURCES

    @property
    def source_id(self) -> str:
        return self._product_config.producer_data_source_id

    @property
    def processor_id(self) -> str:
        return self._product_config.inline_processor_id

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Return ImageInfo for each GRIB not yet cached in S3.

        existing_tilesets from DiscoveryConfig is intentionally ignored;
        GRIB existence is checked directly via S3.
        """
        prefix = f"[{self._product_config.log_prefix}]"
        candidate_times = self._get_candidate_forecast_times(config.current_time)
        logger.info(
            "%s Candidate forecast times: %s",
            prefix,
            [t.strftime("%Y%m%dT%H%MZ") for t in candidate_times],
        )

        # Availability gate: never enqueue a run ECMWF has not published yet.
        # latest() HEADs the run URLs and returns the newest fully-published run;
        # candidates after it are skipped (no doomed download → no SKIP loop). If
        # availability can't be confirmed, emit nothing this tick (fail-safe).
        latest = await asyncio.to_thread(self._latest_available_run)
        if latest is None:
            logger.info(
                "%s Latest available run unknown this tick; emitting nothing", prefix
            )
            return []
        logger.info("%s Latest available ECMWF run: %s", prefix, _fmt_ts(latest))

        new_images = []
        for forecast_time in candidate_times:
            if forecast_time > latest:
                logger.debug(
                    "%s Run not yet published (%s > latest %s); skipping",
                    prefix,
                    _fmt_ts(forecast_time),
                    _fmt_ts(latest),
                )
                continue

            forecast_ts = _fmt_ts(forecast_time)
            grib_key = f"{self._product_config.grib_prefix}/{forecast_ts}.grib"

            # Direct HEAD on the known key (≤3/tick) instead of a prefix LIST.
            # A non-404 HEAD error propagates to the producer's per-source
            # try/except → this source is skipped this tick (fail-safe).
            if await self._s3_client.head_exists(grib_key):
                logger.debug("%s GRIB already cached: %s", prefix, grib_key)
                continue

            if forecast_ts in config.in_progress_images:
                logger.debug(
                    "%s GRIB download already in progress: %s", prefix, forecast_ts
                )
                continue

            new_images.append(
                ImageInfo(
                    image_id=forecast_ts,
                    source_uri=forecast_time.isoformat(),
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._product_config.grib_prefix,
                )
            )
            logger.info("%s Will download missing GRIB: %s", prefix, forecast_ts)

        return new_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download a GRIB from the ECMWF Open Data API, falling back across mirrors.

        Tries each configured mirror in order; the first that responds wins. A
        transient (503) or not-yet-published (404) failure on one mirror falls
        through to the next instead of aborting, so a single flaky mirror does
        not block ingestion.

        Args:
            source_uri: ISO-8601 datetime string for the forecast base time.
            dest_path: Suggested destination path (extension will be set to .grib).

        Returns:
            Path to the downloaded .grib file.

        Raises:
            TransientDownloadError: every mirror failed transiently (requeue).
            ForecastNotAvailableError: no mirror has the run yet (skip).
        """
        prefix = f"[{self._product_config.log_prefix}]"
        forecast_time = datetime.fromisoformat(source_uri)
        target = dest_path.with_suffix(".grib")
        target.parent.mkdir(parents=True, exist_ok=True)
        when = forecast_time.strftime("%Y-%m-%d %H:%M UTC")

        logger.info("%s Downloading GRIB for %s to %s", prefix, when, target)

        saw_transient = False
        saw_not_available = False
        for source in self._sources:
            try:
                self._retrieve_from_mirror(source, forecast_time, target)
            except TransientDownloadError as exc:
                saw_transient = True
                logger.warning(
                    "%s Mirror '%s' unavailable, trying next: %s", prefix, source, exc
                )
                continue
            except ForecastNotAvailableError as exc:
                saw_not_available = True
                logger.warning(
                    "%s Mirror '%s' has no data yet, trying next: %s",
                    prefix,
                    source,
                    exc,
                )
                continue

            logger.info(
                "%s GRIB downloaded from '%s': %s (%.1f MB)",
                prefix,
                source,
                target,
                target.stat().st_size / 1e6,
            )
            return target

        if saw_transient:
            raise TransientDownloadError(
                f"All ECMWF mirrors {list(self._sources)} unavailable for {when}"
            )
        if saw_not_available:
            raise ForecastNotAvailableError(
                f"Forecast not yet available on any ECMWF mirror: {when}"
            )
        raise TransientDownloadError(f"No ECMWF mirrors configured to download {when}")

    def _retrieve_from_mirror(
        self, source: str, forecast_time: datetime, target: Path
    ) -> None:
        """Retrieve the GRIB from a single mirror into ``target``.

        Raises:
            TransientDownloadError: mirror returned 503 (intercepted before
                multiurl's retry loop).
            ForecastNotAvailableError: mirror returned 404 (run not published).
        """
        when = forecast_time.strftime("%Y-%m-%d %H:%M UTC")

        # Intercept 503 BEFORE multiurl's internal retry loop . Raising a
        # non-HTTPError exception bypasses multiurl's catch and lets us move to
        # the next mirror immediately.
        def _reject_slow_down(
            response, *args, **kwargs
        ):  # pylint: disable=unused-argument
            if response.status_code == 503:
                raise TransientDownloadError(
                    f"HTTP 503 from mirror '{source}' for {when}"
                )

        client = Client(source=source)
        client.session.hooks["response"].append(_reject_slow_down)
        target.unlink(missing_ok=True)

        try:
            client.retrieve(
                date=forecast_time.strftime("%Y-%m-%d"),
                time=forecast_time.hour,
                step=_STEPS,
                type="fc",
                param=[self._product_config.parameter],
                target=str(target),
            )
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise ForecastNotAvailableError(
                    f"Forecast not available on mirror '{source}': {when}"
                ) from exc
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _latest_available_run(self) -> datetime | None:
        """Newest fully-published ECMWF run for this product (UTC-aware), or None.

        Uses ``Client.latest()`` to HEAD the run URLs for the LAST forecast step
        (published last), so a hit means the run is complete. Tries each mirror
        in order and returns the first that answers, so one flaky mirror does not
        stall discovery. Synchronous (``requests``) — call via
        ``asyncio.to_thread``. Returns None if no mirror answers so discovery
        stays fail-safe and emits nothing rather than guessing availability.
        """
        for source in self._sources:
            try:
                latest = Client(source=source).latest(
                    type="fc",
                    param=[self._product_config.parameter],
                    step=_STEPS[-1],
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning(
                    "[%s] Mirror '%s' could not determine latest run: %s",
                    self._product_config.log_prefix,
                    source,
                    exc,
                )
                continue
            if latest is not None:
                # latest() returns naive UTC; candidate times are tz-aware UTC.
                return latest.replace(tzinfo=UTC) if latest.tzinfo is None else latest
        return None

    def _get_candidate_forecast_times(self, now: datetime) -> list[datetime]:
        """Return the N most recent forecast base times that should be available."""
        candidates: list[datetime] = []
        # Walk backwards in 12-hour steps over the lookback window
        for hours_back in range(0, MAX_LOOKBACK_HOURS, 12):
            t = now - timedelta(hours=hours_back)
            base_hour = (t.hour // 12) * 12
            base = t.replace(hour=base_hour, minute=0, second=0, microsecond=0)
            if base not in candidates:
                candidates.append(base)
            if len(candidates) >= FORECASTS_TO_MAINTAIN:
                break
        return candidates


def _fmt_ts(dt: datetime) -> str:
    """Format a datetime as YYYYMMDDTHHmmZ (e.g. 20260217T0000Z)."""
    return dt.strftime("%Y%m%dT%H%MZ")
