"""GFS GRIB repository — abstracts where one forecast step's GRIB comes from.

Every backend answers the same question — "give me (cycle, step) as a GRIB2
file" — so :class:`GfsProducerDataSource` is identical whether the bytes come
from the NOMADS grib_filter CGI, from a folder, or from an S3 bucket.

The folder and bucket layouts mirror the pipeline's own GRIB cache keys, so a
cache can be synced into an input location unchanged::

    <root>/<YYYYMMDDTHHmmZ>/<YYYYMMDDTHHmmZ>_f<step>.grib2

i.e. ``<root>`` is the ``grib/gfs`` level.
"""

import asyncio
import logging
import shutil
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import join_s3_prefix, strip_s3_scheme
from exceptions import ForecastNotAvailableError
from services.gfs_grib_validation import validate_gfs_grib

logger = logging.getLogger(__name__)

GRIB_SUFFIX = ".grib2"


def format_cycle(cycle: datetime) -> str:
    """Format a cycle as YYYYMMDDTHHmmZ (the id used by keys and filenames)."""
    return cycle.strftime("%Y%m%dT%H%MZ")


def step_filename(cycle: datetime, step_hours: int) -> str:
    """The GRIB filename for one (cycle, step), e.g. 20260808T0000Z_f003.grib2."""
    return f"{format_cycle(cycle)}_f{step_hours:03d}{GRIB_SUFFIX}"


def step_relative_path(cycle: datetime, step_hours: int) -> str:
    """The cycle-scoped path of one step below the layout root."""
    return f"{format_cycle(cycle)}/{step_filename(cycle, step_hours)}"


def describe_step(cycle: datetime, step_hours: int) -> str:
    """A short "<cycle> f<step>" label used in log lines and error text."""
    return f"{format_cycle(cycle)} f{step_hours:03d}"


class GfsGribRepository(ABC):
    """Interface for the backends one GFS forecast step can be read from."""

    @property
    @abstractmethod
    def source_label(self) -> str:
        """Where this backend reads from, for logs and error messages."""

    @abstractmethod
    async def fetch(self, cycle: datetime, step_hours: int, dest: Path) -> Path:
        """Place the step's GRIB2 at ``dest`` (suffix forced to .grib2).

        Raises:
            ForecastNotAvailableError: the step is not published/present (skip).
            TransientDownloadError: the backend failed transiently (requeue).
            InvalidGribResponseError: the payload is not the expected subset.
        """


class LocalGfsGribRepository(GfsGribRepository):
    """Reads pre-fetched GRIBs from ``<input_dir>/<cycle>/<cycle>_f<step>.grib2``."""

    def __init__(self, input_dir: Path) -> None:
        self._input_dir = input_dir

    @property
    def source_label(self) -> str:
        return str(self._input_dir)

    async def fetch(self, cycle: datetime, step_hours: int, dest: Path) -> Path:
        target = dest.with_suffix(GRIB_SUFFIX)
        target.parent.mkdir(parents=True, exist_ok=True)
        source_path = self._input_dir / step_relative_path(cycle, step_hours)
        if not source_path.exists():
            raise ForecastNotAvailableError(f"GFS GRIB not found: {source_path}")

        target.unlink(missing_ok=True)
        await asyncio.to_thread(shutil.copy2, source_path, target)
        validate_gfs_grib(target, self.source_label, describe_step(cycle, step_hours))
        logger.info(
            "[GFS] Copied %s (%.2f MB) from %s",
            describe_step(cycle, step_hours),
            target.stat().st_size / 1e6,
            source_path,
        )
        return target


class S3GfsGribRepository(GfsGribRepository):
    """Reads pre-fetched GRIBs from ``<prefix>/<cycle>/<cycle>_f<step>.grib2``."""

    def __init__(self, s3_client: S3Client, prefix: str = "") -> None:
        self._s3_client = s3_client
        self._prefix = prefix

    @property
    def source_label(self) -> str:
        return f"s3://{self._s3_client.bucket_name}/{self._prefix}"

    async def fetch(self, cycle: datetime, step_hours: int, dest: Path) -> Path:
        target = dest.with_suffix(GRIB_SUFFIX)
        target.parent.mkdir(parents=True, exist_ok=True)
        key = join_s3_prefix(self._prefix, step_relative_path(cycle, step_hours))
        if not await self._s3_client.head_exists(key):
            raise ForecastNotAvailableError(f"GFS GRIB not in bucket: {key}")

        target.unlink(missing_ok=True)
        await self._s3_client.download_to_file(
            strip_s3_scheme(key, self._s3_client.bucket_name), target
        )
        validate_gfs_grib(target, self.source_label, describe_step(cycle, step_hours))
        logger.info(
            "[GFS] Downloaded %s (%.2f MB) from %s",
            describe_step(cycle, step_hours),
            target.stat().st_size / 1e6,
            key,
        )
        return target
