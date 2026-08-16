"""GFS inline processor: caches the GRIB and fans it out to the enabled products."""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from time import perf_counter
from clients.message_queue_client import MessageQueueClient
from clients.s3_client import S3Client
from models.gfs_config import (
    GFS_GRIB_PREFIX,
    GFS_STEP_DATA_SOURCE_ID,
    GfsProductConfig,
    primary_cog_key,
)
from models.work_unit import WorkUnit
from worker.inline_processor import InlineProcessor
from worker.job_metrics_context import JobMetricsContext

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[GFS]"


class GfsGribDownloader(InlineProcessor):
    """Uploads one forecast step's GRIB and enqueues a WorkUnit per product.

    This is where the single-download-many-products design pays off: the
    producer fetches one GRIB subset per (cycle, step), and this processor fans
    it out to `gfs_mslp`, `gfs_500` and `gfs_250` — all reading the same cached
    object. Downloads scale with steps, not with products.

    Runs in the main worker process (no subprocess) because it needs the
    RabbitMQ client and touches no heavy scientific libraries.

    Idempotent: a retried step skips the upload if the GRIB is already cached,
    and only enqueues products whose COG is still missing.
    """

    def __init__(
        self,
        products: list[GfsProductConfig],
        s3_client: S3Client,
        bounds: dict[str, float],
    ):
        self._products = products
        self._s3_client = s3_client
        self._bounds = bounds

    async def process(
        self,
        file_path: str,
        work_unit: WorkUnit,
        mq_client: MessageQueueClient,
        collector: JobMetricsContext | None = None,
    ) -> None:
        """Cache the GRIB in S3, then enqueue the per-product rendering units.

        Args:
            file_path: Local path to the freshly fetched GRIB.
            work_unit: Unit for this step. `image_id` is `<cycle>_f<step>` and
                `source_uri` carries `cycle` / `step_hours`.
            mq_client: Publishes the per-product work units.
            collector: Optional metrics accumulator; receives the upload/list
                split so the dashboard shows a breakdown, not a bare total.
        """
        meta = json.loads(work_unit.source_uri)
        cycle = datetime.fromisoformat(meta["cycle"])
        cycle_ts = _fmt_cycle(cycle)
        grib_key = f"{GFS_GRIB_PREFIX}/{cycle_ts}/{work_unit.image_id}.grib2"

        list_s, upload_s = await self._upload_grib_if_missing(Path(file_path), grib_key)

        list_start = perf_counter()
        pending = await self._pending_products(cycle_ts, work_unit.image_id)
        list_s += perf_counter() - list_start

        for product in pending:
            mq_client.publish(
                self._build_work_unit(product, work_unit, cycle, grib_key)
            )

        logger.info(
            "%s Step %s: enqueued %d/%d products",
            _LOG_PREFIX,
            work_unit.image_id,
            len(pending),
            len(self._products),
        )

        if collector is not None:
            collector.set_stage_timings({"upload": upload_s, "list": list_s})

    async def _upload_grib_if_missing(
        self, local_path: Path, grib_key: str
    ) -> tuple[float, float]:
        """Cache the GRIB unless it is already there.

        Returns ``(list_seconds, upload_seconds)`` so the existence HEAD and the
        actual PUT land in separate dashboard stages. ``upload_seconds`` is 0.0
        when the object already existed.
        """
        list_start = perf_counter()
        try:
            exists = await self._s3_client.head_exists(grib_key)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "%s GRIB existence check failed (%s); uploading anyway",
                _LOG_PREFIX,
                exc,
            )
            exists = False
        list_s = perf_counter() - list_start

        if exists:
            logger.info("%s GRIB already cached: %s", _LOG_PREFIX, grib_key)
            return list_s, 0.0

        upload_start = perf_counter()
        uploaded = await self._s3_client.upload_file(grib_key, local_path)
        upload_s = perf_counter() - upload_start
        if not uploaded:
            raise RuntimeError(f"Failed to upload GFS GRIB to S3: {grib_key}")
        logger.info("%s GRIB cached: %s", _LOG_PREFIX, grib_key)
        return list_s, upload_s

    async def _pending_products(
        self, cycle_ts: str, image_id: str
    ) -> list[GfsProductConfig]:
        """Products whose COG for this step is still missing.

        Every candidate key is known up front, so concurrent HEADs replace a
        prefix scan that would contend with in-flight tile uploads. On any HEAD
        failure treat everything as pending — re-rendering is wasteful but
        correct, whereas skipping would silently drop a product.
        """
        keys = [
            primary_cog_key(product, cycle_ts, image_id) for product in self._products
        ]
        try:
            existing = await asyncio.gather(
                *(self._s3_client.head_exists(key) for key in keys)
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("%s Could not check existing GFS COGs: %s", _LOG_PREFIX, exc)
            return list(self._products)
        return [product for product, done in zip(self._products, existing) if not done]

    def _build_work_unit(
        self,
        product: GfsProductConfig,
        step_unit: WorkUnit,
        cycle: datetime,
        grib_key: str,
    ) -> WorkUnit:
        """Build the rendering unit for one product off the cached GRIB."""
        cycle_ts = _fmt_cycle(cycle)
        return WorkUnit.create(
            image_id=step_unit.image_id,
            source_uri=json.dumps(
                {
                    "grib_path": grib_key,
                    "cycle": cycle.isoformat(),
                    "step_hours": json.loads(step_unit.source_uri)["step_hours"],
                    "product_id": product.product_id,
                }
            ),
            data_source_id=GFS_STEP_DATA_SOURCE_ID,
            processor_id=product.processor_id,
            output_prefix=f"{product.tiles_prefix}/{cycle_ts}",
            bounds=self._bounds,
            band_id=product.band_id,
        )


def _fmt_cycle(cycle: datetime) -> str:
    """Format a cycle as YYYYMMDDTHHmmZ."""
    return cycle.strftime("%Y%m%dT%H%MZ")
