"""ECMWF GRIB downloader: inline processor that uploads the GRIB and enqueues period-end jobs."""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING

from clients.message_queue_client import MessageQueueClient
from clients.s3_client import S3Client
from models.ecmwf_config import (
    ECMWF_TP_CONFIG,
    FORECAST_HOURS,
    STEP_HOURS,
    WINDOW_HOURS,
    EcmwfProductConfig,
)
from models.work_unit import WorkUnit
from worker.inline_processor import InlineProcessor

if TYPE_CHECKING:  # annotation-only import to avoid an import cycle
    from worker.job_metrics_context import JobMetricsContext

logger = logging.getLogger(__name__)


class EcmwfGribDownloader(InlineProcessor):
    """
    Inline processor that uploads the downloaded GRIB to S3 and enqueues
    one WorkUnit per missing period-end timestamp (T+6, T+9, ..., T+144).

    Runs in the main worker process (no subprocess) because it needs access
    to the RabbitMQ client and does not use heavy scientific libraries.

    Idempotent: if the GRIB is already in S3 (retry scenario), the upload is
    skipped and only missing period-end timestamps are enqueued.
    """

    def __init__(
        self,
        product_config: EcmwfProductConfig = ECMWF_TP_CONFIG,
        s3_client: S3Client | None = None,
        bounds: dict | None = None,
    ):
        self._product_config = product_config
        self._s3_client = s3_client
        self._bounds = bounds or {}

    async def process(
        self,
        file_path: str,
        work_unit: WorkUnit,
        mq_client: MessageQueueClient,
        collector: "JobMetricsContext | None" = None,
    ) -> None:
        """
        Upload GRIB to S3 and enqueue period-end WorkUnits for missing timestamps.

        Args:
            file_path: Local path to the downloaded .grib file.
            work_unit: WorkUnit for this GRIB download job.
                       work_unit.image_id == forecast timestamp (e.g. "20260217T0000Z").
                       work_unit.source_uri == forecast ISO datetime string.
            mq_client: RabbitMQ client for publishing period-end work units.
            collector: Optional metrics accumulator; receives the per-stage
                breakdown (upload / list) so the producer row shows a desglose
                instead of a bare total.
        """
        if self._s3_client is None:
            raise RuntimeError("EcmwfGribDownloader requires an S3 client")

        prefix = f"[{self._product_config.log_prefix}]"
        forecast_ts = work_unit.image_id
        forecast_time = datetime.fromisoformat(work_unit.source_uri)
        grib_s3_key = f"{self._product_config.grib_prefix}/{forecast_ts}.grib"

        # Step 1: Upload GRIB (idempotent). Splits its own time into the
        # existence LIST and the actual PUT (the SeaweedFS write we care about).
        list_s, upload_s = await self._upload_grib_if_missing(
            file_path, grib_s3_key, forecast_ts
        )

        # Step 2: Find missing period-end timestamps and enqueue
        list_start = perf_counter()
        existing_cog_keys = await self._list_existing_cog_keys(
            forecast_ts, forecast_time
        )
        list_s += perf_counter() - list_start

        enqueued = 0
        for hour_end in _end_hours():
            end_time = forecast_time + timedelta(hours=hour_end)
            end_ts = _fmt_ts(end_time)

            cog_key = f"{self._product_config.cog_prefix}/{forecast_ts}/{end_ts}.tif"
            if cog_key in existing_cog_keys:
                logger.debug("%s Period already processed: %s", prefix, end_ts)
                continue

            period_unit = WorkUnit.create(
                image_id=end_ts,
                source_uri=json.dumps(
                    {
                        "grib_path": grib_s3_key,
                        "forecast_time": forecast_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "hour_end": hour_end,
                    }
                ),
                data_source_id=self._product_config.period_data_source_id,
                processor_id=self._product_config.processor_id,
                output_prefix=f"{self._product_config.tiles_prefix}/{forecast_ts}",
                bounds=self._bounds,
                band_id=self._product_config.band_id,
            )
            mq_client.publish(period_unit)
            enqueued += 1

        logger.info(
            "%s Enqueued %d work units for forecast %s",
            prefix,
            enqueued,
            forecast_ts,
        )

        # Dashboard stages: "upload" → "Subida", "list" → "Verif. existentes".
        # upload_s is ~0 when the GRIB already existed (PUT skipped) — correct.
        # The RabbitMQ publish is intentionally not recorded: it is fire-and-
        # forget and always ~0.01s, so it would add only noise to the breakdown.
        if collector is not None:
            collector.set_stage_timings({"upload": upload_s, "list": list_s})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _upload_grib_if_missing(
        self, local_path: str, grib_s3_key: str, forecast_ts: str
    ) -> tuple[float, float]:
        """Upload GRIB to S3 unless it already exists (idempotency).

        Returns ``(list_seconds, upload_seconds)`` so the caller can attribute
        the existence LIST and the actual PUT to separate dashboard stages.
        ``upload_seconds`` is 0.0 when the GRIB already existed (PUT skipped).
        """
        assert self._s3_client is not None
        prefix = f"[{self._product_config.log_prefix}]"
        list_start = perf_counter()
        try:
            existing = await self._s3_client.head_exists(grib_s3_key)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # Existence HEAD failed (not a 404) — proceed to upload; the PUT is
            # an idempotent overwrite, so re-uploading is safe.
            logger.warning(
                "%s GRIB existence HEAD failed (%s); uploading anyway", prefix, exc
            )
            existing = False
        list_s = perf_counter() - list_start
        if existing:
            logger.info(
                "%s GRIB already in S3, skipping upload: %s", prefix, grib_s3_key
            )
            return list_s, 0.0

        logger.info("%s Uploading GRIB to S3: %s", prefix, grib_s3_key)
        upload_start = perf_counter()
        uploaded = await self._s3_client.upload_file(grib_s3_key, Path(local_path))
        upload_s = perf_counter() - upload_start
        if not uploaded:
            raise RuntimeError(f"Failed to upload GRIB to S3: {grib_s3_key}")
        logger.info("%s GRIB uploaded: %s", prefix, grib_s3_key)
        return list_s, upload_s

    async def _list_existing_cog_keys(
        self, forecast_ts: str, forecast_time: datetime
    ) -> set[str]:
        """Return which period-end COG keys already exist, via concurrent HEADs.

        Every candidate key is known (``cog_prefix/<ts>/<end_ts>.tif`` for each
        period end), so direct HEADs replace a prefix scan that would compete
        with concurrent tile uploads. On any HEAD failure, return empty (treat
        all as missing → enqueue all) — same fail-safe as the old LIST.
        """
        assert self._s3_client is not None
        candidate_keys = [
            f"{self._product_config.cog_prefix}/{forecast_ts}/"
            f"{_fmt_ts(forecast_time + timedelta(hours=hour_end))}.tif"
            for hour_end in _end_hours()
        ]
        try:
            results = await asyncio.gather(
                *(self._s3_client.head_exists(key) for key in candidate_keys)
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "[%s] Could not check existing COGs: %s",
                self._product_config.log_prefix,
                exc,
            )
            return set()
        return {key for key, exists in zip(candidate_keys, results) if exists}


def _end_hours() -> list[int]:
    """Return period-end timestamps T+6, T+9, ..., T+144 (47 values; T+3 dropped, T+0 absent)."""
    return list(range(WINDOW_HOURS, FORECAST_HOURS + 1, STEP_HOURS))


def _fmt_ts(dt: datetime) -> str:
    """Format datetime as YYYYMMDDTHHmmZ."""
    return dt.strftime("%Y%m%dT%H%MZ")
