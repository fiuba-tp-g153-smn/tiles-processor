"""ECMWF producer data source: discovers missing GRIBs and downloads from ECMWF API."""

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

from clients.s3_client import S3Client
from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from models.ecmwf_config import (
    ECMWF_TP_CONFIG,
    FORECASTS_TO_MAINTAIN,
    MAX_LOOKBACK_HOURS,
    PERIOD_HOURS,
    EcmwfProductConfig,
)

logger = logging.getLogger(__name__)


class ForecastNotAvailableError(Exception):
    """Raised when a forecast candidate is not yet published on ECMWF Open Data (HTTP 404)."""


class TransientDownloadError(Exception):
    """Raised on transient S3 errors (e.g. 503 Slow Down) to trigger requeue instead of blocking."""


_FORECAST_BASE_HOURS = (0, 12)  # UTC hours at which ECMWF issues forecasts
_STEPS = list(range(PERIOD_HOURS, 145, PERIOD_HOURS))  # [3, 6, ..., 144]


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
    ):
        self._product_config = product_config
        self._s3_client = s3_client

    @property
    def source_id(self) -> str:
        return "ecmwf_tp_producer"

    @property
    def processor_id(self) -> str:
        return "ecmwf_grib_download"

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Return ImageInfo for each GRIB not yet cached in S3.

        existing_tilesets from DiscoveryConfig is intentionally ignored;
        GRIB existence is checked directly via S3.
        """
        candidate_times = self._get_candidate_forecast_times(config.current_time)
        logger.info(
            "[ECMWF] Candidate forecast times: %s",
            [t.strftime("%Y%m%dT%H%MZ") for t in candidate_times],
        )

        existing_grib_keys = await self._list_existing_grib_keys()
        logger.info("[ECMWF] Existing GRIBs in S3: %d", len(existing_grib_keys))

        new_images = []
        for forecast_time in candidate_times:
            forecast_ts = _fmt_ts(forecast_time)
            grib_key = f"{self._product_config.grib_prefix}/{forecast_ts}.grib"

            if grib_key in existing_grib_keys:
                logger.debug("[ECMWF] GRIB already cached: %s", grib_key)
                continue

            if forecast_ts in config.in_progress_images:
                logger.debug("[ECMWF] GRIB download already in progress: %s", forecast_ts)
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
            logger.info("[ECMWF] Will download missing GRIB: %s", forecast_ts)

        return new_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download a GRIB from the ECMWF Open Data API.

        Args:
            source_uri: ISO-8601 datetime string for the forecast base time.
            dest_path: Suggested destination path (extension will be set to .grib).

        Returns:
            Path to the downloaded .grib file.
        """
        # pylint: disable=import-outside-toplevel
        from ecmwf.opendata import Client

        forecast_time = datetime.fromisoformat(source_uri)
        target = dest_path.with_suffix(".grib")
        target.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            "[ECMWF] Downloading GRIB for %s to %s",
            forecast_time.strftime("%Y-%m-%d %H:%M UTC"),
            target,
        )

        client = Client(source="aws")

        # Intercept 503 Slow Down BEFORE multiurl's internal retry loop
        # (which waits 120s × 500 attempts). Raising a non-HTTPError exception
        # bypasses multiurl's catch and lets us requeue the work unit immediately.
        def _reject_slow_down(response, *args, **kwargs):  # pylint: disable=unused-argument
            if response.status_code == 503:
                raise TransientDownloadError(
                    f"S3 rate limit (503 Slow Down) downloading {forecast_time.strftime('%Y-%m-%d %H:%M UTC')}"
                )

        client.session.hooks["response"].append(_reject_slow_down)

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
                    f"Forecast not yet available on ECMWF Open Data: {forecast_time.strftime('%Y-%m-%d %H:%M UTC')}"
                ) from exc
            raise

        logger.info("[ECMWF] GRIB downloaded: %s (%.1f MB)", target, target.stat().st_size / 1e6)
        return target

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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

    async def _list_existing_grib_keys(self) -> set[str]:
        """Return the set of GRIB S3 keys currently cached."""
        if self._s3_client is None:
            return set()
        try:
            keys = await self._s3_client.list_files(
                f"{self._product_config.grib_prefix}/", ".grib"
            )
            return set(keys)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("[ECMWF] Could not list existing GRIBs: %s", exc)
            return set()


def _fmt_ts(dt: datetime) -> str:
    """Format a datetime as YYYYMMDDTHHmmZ (e.g. 20260217T0000Z)."""
    return dt.strftime("%Y%m%dT%H%MZ")
