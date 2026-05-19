"""GLM folder data source — groups pre-gridded 1-minute GLM netCDFs into windows.

Equivalent to :class:`RadarDataSource` for folder-based discovery, but emits one
:class:`ImageInfo` per N-minute aggregation window (mirroring the multi-file
manifest pattern used by :class:`Goes19GlmDataSource`).
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from logging import getLogger
from pathlib import Path

from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from data_sources.glm_folder_repository import GlmFolderFileRepository
from models.band_config import BandConfig
from models.glm_folder_config import parse_glm_folder_filename

logger = getLogger(__name__)


class GlmFolderDataSource(DataSource):
    """Discovers GLM aggregation windows from a local folder of 1-minute files.

    Each window is anchored to a clock-aligned boundary spaced every
    ``produce_every_minutes`` and covers ``accum_minutes``. With the
    recommended 10/10 cadence the windows are non-overlapping; with a 5/2
    cadence they overlap by 3 minutes.

    A window is only emitted when:

      * It is complete — exactly ``accum_minutes`` distinct 1-minute files
        whose start timestamps fall inside ``[anchor, anchor + accum_minutes)``.
      * Its end is in the past with at least ``safety_lag_seconds`` of margin,
        so we don't race upstream producers still writing the last minute.
      * It has not been processed yet (``existing_tilesets``) and is not
        already in flight (``in_progress_images``).
    """

    DEFAULT_TARGET_WINDOWS = 24

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        band_config: BandConfig,
        repository: GlmFolderFileRepository,
        *,
        accum_minutes: int,
        produce_every_minutes: int,
        safety_lag_seconds: int = 30,
        target_windows: int = DEFAULT_TARGET_WINDOWS,
    ) -> None:
        if accum_minutes <= 0:
            raise ValueError("accum_minutes must be positive")
        if produce_every_minutes <= 0:
            raise ValueError("produce_every_minutes must be positive")
        self._band_config = band_config
        self._repository = repository
        self._accum_minutes = accum_minutes
        self._produce_every_minutes = produce_every_minutes
        self._safety_lag = timedelta(seconds=safety_lag_seconds)
        self._target_windows = target_windows

    @property
    def source_id(self) -> str:
        return "glm_folder"

    @property
    def processor_id(self) -> str:
        return "glm_fed"

    @property
    def band_config(self) -> BandConfig:
        """Expose band config so the producer can derive S3 dedup prefixes."""
        return self._band_config

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        source_uris = await self._repository.list_files()
        if not source_uris:
            logger.warning("[%s] No GLM files found by repository", self.source_id)
            return []

        windows = self._group_into_windows(source_uris)

        complete = self._select_complete_ready_windows(
            windows, current_time=config.current_time
        )
        complete.sort(key=lambda item: item[0], reverse=True)
        complete = complete[: self._target_windows]

        new_images: list[ImageInfo] = []
        for window_start, files in complete:
            image_id = _window_image_id(window_start)
            if image_id in config.existing_tilesets:
                continue
            if image_id in config.in_progress_images:
                continue

            source_uri = json.dumps(
                {"window_start": window_start.isoformat(), "files": files}
            )
            new_images.append(
                ImageInfo(
                    image_id=image_id,
                    source_uri=source_uri,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._band_config.s3_tiles_prefix,
                )
            )

        logger.info(
            "[%s] Discovered %d new windows (%d complete, %d total candidates)",
            self.source_id,
            len(new_images),
            len(complete),
            len(windows),
        )
        return new_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        manifest = json.loads(source_uri)
        files = manifest["files"]
        window_start = manifest["window_start"]

        logger.info(
            "[%s] Copying %d files for window %s -> %s",
            self.source_id,
            len(files),
            window_start,
            dest_path,
        )
        return await self._repository.download_to_dir(files, dest_path)

    def _group_into_windows(self, source_uris: list[str]) -> dict[datetime, list[str]]:
        """Bucket files by the anchor of every overlapping window they belong to."""
        windows: dict[datetime, list[str]] = defaultdict(list)
        accum = timedelta(minutes=self._accum_minutes)

        for uri in source_uris:
            try:
                parts = parse_glm_folder_filename(Path(uri).name)
            except ValueError as exc:
                logger.debug("Skipping non-GLM file %s (%s)", uri, exc)
                continue

            file_start = parts.start_dt
            for anchor in self._anchors_covering(file_start, accum):
                windows[anchor].append(uri)

        return windows

    def _anchors_covering(
        self, file_start: datetime, accum: timedelta
    ) -> list[datetime]:
        """Return every window anchor whose [anchor, anchor+accum) covers ``file_start``.

        Anchors are aligned to the wall clock at multiples of
        ``produce_every_minutes`` past the hour.
        """
        step = timedelta(minutes=self._produce_every_minutes)
        anchored_to_hour = file_start.replace(minute=0, second=0, microsecond=0)
        delta = file_start - anchored_to_hour
        latest_anchor = anchored_to_hour + step * (delta // step)

        anchors: list[datetime] = []
        anchor = latest_anchor
        while anchor <= file_start < anchor + accum:
            anchors.append(anchor)
            anchor -= step
        return anchors

    def _select_complete_ready_windows(
        self,
        windows: dict[datetime, list[str]],
        current_time: datetime,
    ) -> list[tuple[datetime, list[str]]]:
        accum = timedelta(minutes=self._accum_minutes)
        cutoff = current_time - self._safety_lag
        ready: list[tuple[datetime, list[str]]] = []

        for anchor, files in windows.items():
            window_end = anchor + accum
            if window_end > cutoff:
                continue
            if len(set(files)) != self._accum_minutes:
                logger.debug(
                    "[%s] Skipping incomplete window %s: %d/%d files",
                    self.source_id,
                    anchor.isoformat(),
                    len(set(files)),
                    self._accum_minutes,
                )
                continue
            ready.append((anchor, sorted(set(files))))
        return ready


def _window_image_id(window_start: datetime) -> str:
    """Build a 14-char NOAA-style image id from the window start.

    Format: ``YYYYJJJHHMMSSD`` (year, day-of-year, hour, minute, second,
    decisecond — always 0 for clock-aligned windows). Matches the dedup
    contract used by the previous AWS-based GLM source so existing S3
    tilesets remain comparable after the cutover.
    """
    window_start = window_start.astimezone(timezone.utc).replace(tzinfo=None)
    doy = window_start.timetuple().tm_yday
    return (
        f"{window_start.year:04d}"
        f"{doy:03d}"
        f"{window_start.hour:02d}"
        f"{window_start.minute:02d}"
        f"{window_start.second:02d}"
        f"0"
    )
