"""ECMWF GRIB downloader: inline processor that uploads the GRIB and enqueues period-end jobs."""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

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
    ) -> None:
        """
        Upload GRIB to S3 and enqueue period-end WorkUnits for missing timestamps.

        Args:
            file_path: Local path to the downloaded .grib file.
            work_unit: WorkUnit for this GRIB download job.
                       work_unit.image_id == forecast timestamp (e.g. "20260217T0000Z").
                       work_unit.source_uri == forecast ISO datetime string.
            mq_client: RabbitMQ client for publishing period-end work units.
        """
        if self._s3_client is None:
            raise RuntimeError("EcmwfGribDownloader requires an S3 client")

        prefix = f"[{self._product_config.log_prefix}]"
        forecast_ts = work_unit.image_id
        forecast_time = datetime.fromisoformat(work_unit.source_uri)
        grib_s3_key = f"{self._product_config.grib_prefix}/{forecast_ts}.grib"

        # Step 1: Upload GRIB (idempotent)
        await self._upload_grib_if_missing(file_path, grib_s3_key, forecast_ts)

        # Step 2: Find missing period-end timestamps and enqueue
        existing_cog_keys = await self._list_existing_cog_keys(forecast_ts)
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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _upload_grib_if_missing(
        self, local_path: str, grib_s3_key: str, forecast_ts: str
    ) -> None:
        """Upload GRIB to S3 unless it already exists (idempotency)."""
        assert self._s3_client is not None
        prefix = f"[{self._product_config.log_prefix}]"
        existing = await self._s3_client.list_files(
            f"{self._product_config.grib_prefix}/", f"{forecast_ts}.grib"
        )
        if existing:
            logger.info(
                "%s GRIB already in S3, skipping upload: %s", prefix, grib_s3_key
            )
            return

        logger.info("%s Uploading GRIB to S3: %s", prefix, grib_s3_key)
        uploaded = await self._s3_client.upload_file(grib_s3_key, Path(local_path))
        if not uploaded:
            raise RuntimeError(f"Failed to upload GRIB to S3: {grib_s3_key}")
        logger.info("%s GRIB uploaded: %s", prefix, grib_s3_key)

    async def _list_existing_cog_keys(self, forecast_ts: str) -> set[str]:
        """Return the set of COG keys already generated for this forecast."""
        assert self._s3_client is not None
        try:
            keys = await self._s3_client.list_files(
                f"{self._product_config.cog_prefix}/{forecast_ts}/", ".tif"
            )
            return set(keys)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "[%s] Could not list existing COGs: %s",
                self._product_config.log_prefix,
                exc,
            )
            return set()


def _end_hours() -> list[int]:
    """Return period-end timestamps T+6, T+9, ..., T+144 (47 values; T+3 dropped, T+0 absent)."""
    return list(range(WINDOW_HOURS, FORECAST_HOURS + 1, STEP_HOURS))


def _fmt_ts(dt: datetime) -> str:
    """Format datetime as YYYYMMDDTHHmmZ."""
    return dt.strftime("%Y%m%dT%H%MZ")
