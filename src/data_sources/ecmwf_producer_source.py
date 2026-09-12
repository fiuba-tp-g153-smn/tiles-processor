"""ECMWF producer data source: discovers missing GRIBs and fetches them."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from data_sources.ecmwf_repository import EcmwfGribRepository, format_run_timestamp
from models.ecmwf_config import (
    ECMWF_TP_CONFIG,
    FORECASTS_TO_MAINTAIN,
    MAX_LOOKBACK_HOURS,
    STEP_HOURS,
    EcmwfProductConfig,
)

logger = logging.getLogger(__name__)

STEPS = list(range(STEP_HOURS, 145, STEP_HOURS))  # [3, 6, ..., 144]

DEFAULT_OPENDATA_SOURCES = ("ecmwf", "azure", "aws")


class EcmwfProducerDataSource(DataSource):
    """
    Data source for ECMWF GRIB discovery (used by the producer).

    Responsibilities:
    - Calculate the N most recent available forecast timestamps.
    - Check which GRIBs are missing from the S3 cache.
    - Return ImageInfo for each missing GRIB; the worker handles download and
      period enqueuing.
    - Delegate the actual fetch to the injected repository, which is what
      decides between the Open Data mirrors, a folder and a bucket.
    """

    def __init__(
        self,
        product_config: EcmwfProductConfig = ECMWF_TP_CONFIG,
        s3_client: S3Client | None = None,
        repository: EcmwfGribRepository | None = None,
    ):
        self._product_config = product_config
        self._s3_client = s3_client
        self._repository = repository

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
            [format_run_timestamp(t) for t in candidate_times],
        )

        # Availability gate: never enqueue a run the backend does not have yet,
        # so no doomed download is queued and no SKIP loop follows. If
        # availability can't be confirmed, emit nothing this tick (fail-safe).
        latest = await self._latest_available_run()
        if latest is None:
            logger.info(
                "%s Latest available run unknown this tick; emitting nothing", prefix
            )
            return []
        logger.info(
            "%s Latest available ECMWF run: %s", prefix, format_run_timestamp(latest)
        )

        new_images = []
        for forecast_time in candidate_times:
            image = await self._to_image_info_if_missing(
                forecast_time, latest, config, prefix
            )
            if image is not None:
                new_images.append(image)

        return new_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Fetch a run's GRIB through the configured repository.

        Args:
            source_uri: ISO-8601 datetime string for the forecast base time.
            dest_path: Suggested destination path (extension will be set to .grib).

        Returns:
            Path to the downloaded .grib file.

        Raises:
            TransientDownloadError: the backend failed transiently (requeue).
            ForecastNotAvailableError: the run is not available yet (skip).
        """
        prefix = f"[{self._product_config.log_prefix}]"
        forecast_time = datetime.fromisoformat(source_uri)
        target = dest_path.with_suffix(".grib")
        target.parent.mkdir(parents=True, exist_ok=True)
        when = forecast_time.strftime("%Y-%m-%d %H:%M UTC")

        logger.info("%s Fetching GRIB for %s to %s", prefix, when, target)
        result = await self._require_repository().fetch(forecast_time, target)
        logger.info(
            "%s GRIB ready: %s (%.1f MB)",
            prefix,
            result,
            result.stat().st_size / 1e6,
        )
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_s3_client(self) -> S3Client:
        """The cache client, or a clear error if the source was half-built."""
        if self._s3_client is None:
            raise RuntimeError(f"{self.source_id} requires an S3 client")
        return self._s3_client

    def _require_repository(self) -> EcmwfGribRepository:
        """The injected repository, or a clear error if the source was half-built."""
        if self._repository is None:
            raise RuntimeError(
                f"{self.source_id} requires an EcmwfGribRepository to fetch GRIBs"
            )
        return self._repository

    async def _latest_available_run(self) -> datetime | None:
        """Newest run the configured backend can serve, or None when unknown."""
        return await self._require_repository().latest_available_run()

    async def _to_image_info_if_missing(
        self,
        forecast_time: datetime,
        latest: datetime,
        config: DiscoveryConfig,
        prefix: str,
    ) -> ImageInfo | None:
        """One candidate run's ImageInfo, or None when it is unavailable or cached."""
        if forecast_time > latest:
            logger.debug(
                "%s Run not yet published (%s > latest %s); skipping",
                prefix,
                format_run_timestamp(forecast_time),
                format_run_timestamp(latest),
            )
            return None

        forecast_ts = format_run_timestamp(forecast_time)
        grib_key = f"{self._product_config.grib_prefix}/{forecast_ts}.grib"

        # Direct HEAD on the known key (≤3/tick) instead of a prefix LIST.
        # A non-404 HEAD error propagates to the producer's per-source
        # try/except → this source is skipped this tick (fail-safe).
        if await self._require_s3_client().head_exists(grib_key):
            logger.debug("%s GRIB already cached: %s", prefix, grib_key)
            return None

        if forecast_ts in config.in_progress_images:
            logger.debug(
                "%s GRIB download already in progress: %s", prefix, forecast_ts
            )
            return None

        logger.info("%s Will fetch missing GRIB: %s", prefix, forecast_ts)
        return ImageInfo(
            image_id=forecast_ts,
            source_uri=forecast_time.isoformat(),
            data_source_id=self.source_id,
            processor_id=self.processor_id,
            output_prefix=self._product_config.grib_prefix,
        )

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
