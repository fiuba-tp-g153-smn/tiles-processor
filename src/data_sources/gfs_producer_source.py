"""GFS producer data source: discovers missing GRIB steps and fetches them."""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from clients.s3_client import S3Client
from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from data_sources.gfs_fetcher import GfsGribFetcher
from models.gfs_config import (
    CYCLE_HOURS,
    GFS_GRIB_PREFIX,
    GFS_INLINE_PROCESSOR_ID,
    GFS_PRODUCER_DATA_SOURCE_ID,
    forecast_steps,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[GFS]"


class GfsProducerDataSource(DataSource):
    """Discovers GRIB steps that are missing from the S3 cache and fetches them.

    Three flow controls keep the request rate against NOMADS bounded:

    1. **Eligibility window** — a cycle is only considered once it is old enough
       to have been published, and is abandoned if it never shows up.
    2. **Dedup** — one LIST per cycle prefix tells us which steps already exist.
    3. **Per-tick cap** — at most `max_steps_per_tick` units are emitted, so a
       cold start fills gradually instead of bursting a whole cycle's 33 steps.

    They are applied in that order.
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        fetcher: GfsGribFetcher,
        s3_client: S3Client,
        cycles_to_maintain: int,
        max_steps_per_tick: int,
        probe_from_hours: int,
        probe_to_hours: int,
    ):
        self._fetcher = fetcher
        self._s3_client = s3_client
        self._cycles_to_maintain = cycles_to_maintain
        self._max_steps_per_tick = max_steps_per_tick
        self._probe_from_hours = probe_from_hours
        self._probe_to_hours = probe_to_hours

    @property
    def source_id(self) -> str:
        return GFS_PRODUCER_DATA_SOURCE_ID

    @property
    def processor_id(self) -> str:
        return GFS_INLINE_PROCESSOR_ID

    @property
    def uses_existing_tilesets(self) -> bool:
        """GRIB presence in S3 is authoritative, so the tileset LIST is dead weight."""
        return False

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """Emit one ImageInfo per missing (cycle, step), newest cycle first.

        `existing_tilesets` is ignored on purpose: GRIB presence is authoritative
        and is read straight from S3.
        """
        candidates = self._candidate_cycles(config.current_time)
        logger.info(
            "%s Candidate cycles: %s",
            _LOG_PREFIX,
            [_fmt_cycle(c) for c in candidates],
        )

        emitted: list[ImageInfo] = []
        for cycle in candidates:
            budget = self._max_steps_per_tick - len(emitted)
            if budget <= 0:
                logger.info(
                    "%s Per-tick cap of %d reached; remaining cycles wait for the "
                    "next tick",
                    _LOG_PREFIX,
                    self._max_steps_per_tick,
                )
                break
            emitted += await self._discover_cycle(
                cycle, config, budget, config.current_time
            )
        return emitted

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Fetch one GRIB subset.

        Args:
            source_uri: JSON with `cycle` (ISO-8601) and `step_hours`.
            dest_path: Suggested destination; the extension becomes `.grib2`.
        """
        meta = json.loads(source_uri)
        cycle = datetime.fromisoformat(meta["cycle"])
        return await self._fetcher.fetch(cycle, int(meta["step_hours"]), dest_path)

    async def _discover_cycle(
        self,
        cycle: datetime,
        config: DiscoveryConfig,
        budget: int,
        now: datetime,
    ) -> list[ImageInfo]:
        """Missing steps for one cycle, capped at `budget`."""
        age_hours = (now - cycle).total_seconds() / 3600.0
        if age_hours < self._probe_from_hours:
            logger.debug(
                "%s Cycle %s is only %.1fh old; not published yet",
                _LOG_PREFIX,
                _fmt_cycle(cycle),
                age_hours,
            )
            return []

        cached = await self._cached_steps(cycle)

        if age_hours > self._probe_to_hours and not cached:
            logger.debug(
                "%s Cycle %s is %.1fh old with nothing cached; abandoning",
                _LOG_PREFIX,
                _fmt_cycle(cycle),
                age_hours,
            )
            return []

        missing = [
            step
            for step in forecast_steps()
            if step not in cached
            and _image_id(cycle, step) not in config.in_progress_images
        ]
        if not missing:
            logger.debug("%s Cycle %s is complete", _LOG_PREFIX, _fmt_cycle(cycle))
            return []

        selected = missing[:budget]
        if len(selected) < len(missing):
            logger.info(
                "%s Cycle %s: %d steps missing, emitting %d this tick (cap %d)",
                _LOG_PREFIX,
                _fmt_cycle(cycle),
                len(missing),
                len(selected),
                self._max_steps_per_tick,
            )
        else:
            logger.info(
                "%s Cycle %s: emitting %d missing steps",
                _LOG_PREFIX,
                _fmt_cycle(cycle),
                len(selected),
            )
        return [self._to_image_info(cycle, step) for step in selected]

    async def _cached_steps(self, cycle: datetime) -> set[int]:
        """Forecast hours already cached for `cycle`, via one LIST."""
        prefix = f"{GFS_GRIB_PREFIX}/{_fmt_cycle(cycle)}/"
        try:
            keys = await self._s3_client.list_files(prefix, ".grib2")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "%s Could not list cached GRIBs for %s: %s",
                _LOG_PREFIX,
                _fmt_cycle(cycle),
                exc,
            )
            return set()

        steps: set[int] = set()
        for key in keys:
            step = _step_from_key(key)
            if step is not None:
                steps.add(step)
        return steps

    def _to_image_info(self, cycle: datetime, step: int) -> ImageInfo:
        return ImageInfo(
            image_id=_image_id(cycle, step),
            source_uri=json.dumps({"cycle": cycle.isoformat(), "step_hours": step}),
            data_source_id=self.source_id,
            processor_id=self.processor_id,
            output_prefix=f"{GFS_GRIB_PREFIX}/{_fmt_cycle(cycle)}",
        )

    def _candidate_cycles(self, now: datetime) -> list[datetime]:
        """The N most recent cycle base times, newest first."""
        cycles: list[datetime] = []
        probe = now.replace(minute=0, second=0, microsecond=0)
        for _ in range(24 * 2):
            if probe.hour in CYCLE_HOURS:
                if probe not in cycles:
                    cycles.append(probe)
                if len(cycles) >= self._cycles_to_maintain:
                    break
            probe -= timedelta(hours=1)
        return cycles


def _image_id(cycle: datetime, step: int) -> str:
    """Stable per-(cycle, step) id, e.g. 20260808T0000Z_f003."""
    return f"{_fmt_cycle(cycle)}_f{step:03d}"


def _fmt_cycle(cycle: datetime) -> str:
    """Format a cycle as YYYYMMDDTHHmmZ."""
    return cycle.strftime("%Y%m%dT%H%MZ")


def _step_from_key(key: str) -> int | None:
    """Recover the forecast hour from a cached GRIB key, or None if unparseable."""
    stem = key.rsplit("/", 1)[-1].removesuffix(".grib2")
    marker = stem.rfind("_f")
    if marker == -1:
        return None
    try:
        return int(stem[marker + 2 :])
    except ValueError:
        return None
