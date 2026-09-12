"""ECMWF GRIB repository — abstracts where a forecast run's GRIB comes from.

Three backends answer the same two questions — "what is the newest run I can
have?" and "give me that run's GRIB" — so :class:`EcmwfProducerDataSource` is
identical whether the data arrives from ECMWF's Open Data mirrors, from a
folder, or from an S3 bucket.

The folder and bucket layouts mirror the pipeline's own GRIB cache keys, so a
cache can be synced into an input location unchanged::

    <root>/<product_dir>/<YYYYMMDDTHHmmZ>.grib

where ``<product_dir>`` is the last segment of the product's ``grib_prefix``
(``total_precipitation``, ``mean_sea_level_pressure``), i.e. ``<root>`` is the
``grib/ecmwf-ifs`` level.
"""

import asyncio
import logging
import shutil
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path

import requests
from ecmwf.opendata import Client

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import join_s3_prefix, strip_s3_scheme
from exceptions import ForecastNotAvailableError, TransientDownloadError
from models.ecmwf_config import EcmwfProductConfig

logger = logging.getLogger(__name__)

GRIB_SUFFIX = ".grib"
TIMESTAMP_FORMAT = "%Y%m%dT%H%MZ"


def format_run_timestamp(forecast_time: datetime) -> str:
    """Format a run's base time as YYYYMMDDTHHmmZ (e.g. 20260217T0000Z)."""
    return forecast_time.strftime(TIMESTAMP_FORMAT)


def parse_run_timestamp(name: str) -> datetime | None:
    """Recover a run's base time from a GRIB filename, or None if unparseable."""
    stem = Path(name).name.removesuffix(GRIB_SUFFIX)
    try:
        return datetime.strptime(stem, TIMESTAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def product_directory(product_config: EcmwfProductConfig) -> str:
    """The per-product folder name, shared by the cache and the input layouts."""
    return product_config.grib_prefix.rsplit("/", 1)[-1]


def newest_run(names: list[str]) -> datetime | None:
    """The newest run base time among GRIB filenames, or None when there are none."""
    runs = [run for run in map(parse_run_timestamp, names) if run is not None]
    return max(runs) if runs else None


class EcmwfGribRepository(ABC):
    """Interface for the backends an ECMWF forecast GRIB can be read from."""

    @abstractmethod
    async def latest_available_run(self) -> datetime | None:
        """Newest fully-published run (UTC-aware), or None when unknown.

        None is the fail-safe answer: discovery emits nothing for that tick
        rather than guessing at availability.
        """

    @abstractmethod
    async def fetch(self, forecast_time: datetime, target: Path) -> Path:
        """Place the run's GRIB at ``target``.

        Raises:
            TransientDownloadError: the backend failed in a way retrying fixes.
            ForecastNotAvailableError: the run is not published/present.
        """


class OpenDataEcmwfGribRepository(EcmwfGribRepository):
    """Reads GRIBs from the ECMWF Open Data mirrors (the production default).

    Tries each configured mirror in order; the first that responds wins. A
    transient (503) or not-yet-published (404) failure on one mirror falls
    through to the next instead of aborting, so a single flaky mirror does not
    block ingestion.
    """

    def __init__(
        self,
        product_config: EcmwfProductConfig,
        sources: tuple[str, ...],
        steps: list[int],
    ) -> None:
        self._product_config = product_config
        self._sources = sources
        self._steps = steps

    async def latest_available_run(self) -> datetime | None:
        return await asyncio.to_thread(self._latest_available_run)

    async def fetch(self, forecast_time: datetime, target: Path) -> Path:
        return await asyncio.to_thread(self._retrieve, forecast_time, target)

    def _retrieve(self, forecast_time: datetime, target: Path) -> Path:
        """Walk the mirrors until one serves the run (synchronous ``requests``)."""
        prefix = f"[{self._product_config.log_prefix}]"
        when = forecast_time.strftime("%Y-%m-%d %H:%M UTC")
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
            logger.info("%s GRIB downloaded from '%s': %s", prefix, source, target)
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

        # Intercept 503 BEFORE multiurl's internal retry loop. Raising a
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
                step=self._steps,
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

    def _latest_available_run(self) -> datetime | None:
        """Newest fully-published run for this product (UTC-aware), or None.

        Uses ``Client.latest()`` to HEAD the run URLs for the LAST forecast step
        (published last), so a hit means the run is complete. Tries each mirror
        in order and returns the first that answers, so one flaky mirror does not
        stall discovery.
        """
        for source in self._sources:
            try:
                latest = Client(source=source).latest(
                    type="fc",
                    param=[self._product_config.parameter],
                    step=self._steps[-1],
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


class LocalEcmwfGribRepository(EcmwfGribRepository):
    """Reads pre-fetched GRIBs from ``<input_dir>/<product_dir>/<run>.grib``."""

    def __init__(self, input_dir: Path, product_config: EcmwfProductConfig) -> None:
        self._product_dir = input_dir / product_directory(product_config)
        self._log_prefix = f"[{product_config.log_prefix}]"

    async def latest_available_run(self) -> datetime | None:
        return newest_run(await asyncio.to_thread(self._list_names))

    async def fetch(self, forecast_time: datetime, target: Path) -> Path:
        source_path = self._product_dir / f"{format_run_timestamp(forecast_time)}"
        source_path = source_path.with_suffix(GRIB_SUFFIX)
        if not source_path.exists():
            raise ForecastNotAvailableError(f"ECMWF GRIB not found: {source_path}")
        target.unlink(missing_ok=True)
        await asyncio.to_thread(shutil.copy2, source_path, target)
        logger.info("%s GRIB copied from %s", self._log_prefix, source_path)
        return target

    def _list_names(self) -> list[str]:
        """GRIB filenames in the product folder (empty when it is missing)."""
        if not self._product_dir.exists():
            logger.warning(
                "%s ECMWF input dir does not exist, treating as empty: %s",
                self._log_prefix,
                self._product_dir,
            )
            return []
        return [p.name for p in self._product_dir.glob(f"*{GRIB_SUFFIX}")]


class S3EcmwfGribRepository(EcmwfGribRepository):
    """Reads pre-fetched GRIBs from ``<prefix>/<product_dir>/<run>.grib`` in a bucket."""

    def __init__(
        self,
        s3_client: S3Client,
        product_config: EcmwfProductConfig,
        prefix: str = "",
    ) -> None:
        self._s3_client = s3_client
        self._prefix = join_s3_prefix(prefix, product_directory(product_config))
        self._log_prefix = f"[{product_config.log_prefix}]"

    async def latest_available_run(self) -> datetime | None:
        keys = await self._s3_client.list_files(
            f"{self._prefix}/", file_pattern=GRIB_SUFFIX
        )
        return newest_run(keys)

    async def fetch(self, forecast_time: datetime, target: Path) -> Path:
        key = f"{self._prefix}/{format_run_timestamp(forecast_time)}{GRIB_SUFFIX}"
        if not await self._s3_client.head_exists(key):
            raise ForecastNotAvailableError(f"ECMWF GRIB not in bucket: {key}")
        await self._s3_client.download_to_file(
            strip_s3_scheme(key, self._s3_client.bucket_name), target
        )
        logger.info("%s GRIB downloaded from s3 key %s", self._log_prefix, key)
        return target
