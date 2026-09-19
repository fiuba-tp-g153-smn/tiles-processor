"""INTA radar data source - discovers Rainbow5 .vol files via a repository."""

import asyncio
from collections import defaultdict
from logging import getLogger
from pathlib import Path

from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from data_sources.inta_radar_repository import IntaRadarFileRepository
from models.radar_config import (
    INTA_STOP_RANGE_KM,
    RadarProductConfig,
    RadarStationFilter,
)
from models.rainbow_header import (
    RainbowHeaderError,
    parse_inta_filename,
    parse_rainbow_header,
)

logger = getLogger(__name__)


class IntaRadarDataSource(DataSource):
    """
    Data source for the INTA radars (Anguil, Paraná, Pergamino).

    These deliver Rainbow5 volumes rather than the SINARAME ODIM-HDF5, so they
    need their own reader and their own discovery. What they do *not* need is
    their own products: the moments are the same physical variables, so each
    source is configured with a ``RadarProductConfig`` derived from the
    SINARAME one — same PyART field, same palette — differing only in the S3
    prefix that puts it in the INTA namespace.

    Two things can only be learned from inside the file, so discovery reads a
    bounded header window per candidate:

    1. **The station.** Files are named ``{RADAR}{YYYYMMDDHHMMSS}{NN}{VAR}.vol``
       but the radar token is only present in production — the SMN's sample
       files omit it. The header carries it either way.
    2. **The scan range.** Anguil and Pergamino interleave 120 km volumes with
       the 240 km ones in the same folder, distinguishable by nothing else.
       Only the 240 km scans are published (see ``INTA_STOP_RANGE_KM``).
    """

    # Scans arrive every 10 minutes, so 12 covers ~2 hours — enough for the
    # visualizer's longest playback window with margin.
    TARGET_IMAGES = 12

    # Header reads are network-bound in S3 mode; cap the fan-out the way the
    # rest of the pipeline bounds concurrent I/O.
    HEADER_CONCURRENCY = 16

    def __init__(
        self,
        product_config: RadarProductConfig,
        rainbow_variable: str,
        repository: IntaRadarFileRepository,
        station_filter: RadarStationFilter | None = None,
        target_images: int | None = None,
    ):
        """
        Initialize an INTA data source for a specific product.

        Args:
            product_config: The product config this variable is published as
            rainbow_variable: Rainbow5 data type in the filename (e.g. "dBZ")
            repository: Repository for listing, header-reading and downloading
            station_filter: Decides which stations to discover; ``None`` means
                every station
            target_images: Max images published per station per tick; ``None``
                uses TARGET_IMAGES
        """
        self._product_config = product_config
        self._rainbow_variable = rainbow_variable
        self._repository = repository
        self._station_filter = station_filter
        self._target_images = (
            target_images if target_images is not None else self.TARGET_IMAGES
        )

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return f"radar_inta_{self._product_config.product_id}"

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return "radar_inta"

    @property
    def product_config(self) -> RadarProductConfig:
        """Get the radar product configuration."""
        return self._product_config

    @property
    def rainbow_variable(self) -> str:
        """The Rainbow5 data type this source discovers."""
        return self._rainbow_variable

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new .vol files for this source's variable.

        Filters by filename variable first (cheap, no I/O), then reads the
        header of the survivors to resolve station and scan range, then drops
        anything already processed or in progress.
        """
        source_uris = await self._repository.list_files()
        if not source_uris:
            logger.warning("[%s] No .vol files found by repository", self.source_id)
            return []

        candidates = [uri for uri in source_uris if self._matches_variable(uri)]
        if not candidates:
            return []

        new_images: list[ImageInfo] = []
        skipped_disabled_radar = 0
        skipped_short_range = 0
        unreadable = 0

        for source_uri, header in await self._read_headers(candidates):
            if header is None:
                unreadable += 1
                continue

            # Anguil/Pergamino mix 120 km volumes into the same folder; they
            # would render under the same product with a different footprint.
            if header.stop_range_km != INTA_STOP_RANGE_KM:
                skipped_short_range += 1
                continue

            if self._station_filter is not None and not self._station_filter.allows(
                header.radar_id
            ):
                skipped_disabled_radar += 1
                continue

            timestamp = parse_inta_filename(Path(source_uri).name)["timestamp"]
            # Same shape as the SINARAME image_id, so the producer's tileset
            # dedup matches these against S3 without any special-casing.
            image_id = (
                f"{header.radar_id}_{self._product_config.product_id}_{timestamp}"
            )

            if image_id in config.existing_tilesets:
                logger.debug("Skipping %s (already processed)", image_id)
                continue
            if image_id in config.in_progress_images:
                logger.debug("Skipping %s (in progress)", image_id)
                continue

            new_images.append(
                ImageInfo(
                    image_id=image_id,
                    source_uri=source_uri,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._product_config.s3_tiles_prefix,
                )
            )

        target_images = self._cap_per_station(new_images)

        if skipped_short_range:
            logger.info(
                "[%s] Skipped %d files from scans shorter than %.0f km",
                self.source_id,
                skipped_short_range,
                INTA_STOP_RANGE_KM,
            )
        if skipped_disabled_radar:
            logger.info(
                "[%s] Skipped %d files from radars disabled in config",
                self.source_id,
                skipped_disabled_radar,
            )
        if unreadable:
            logger.warning(
                "[%s] Skipped %d files with an unreadable Rainbow header",
                self.source_id,
                unreadable,
            )

        logger.info(
            "[%s] Found %d new files, publishing %d (limit %d, from %d total "
            ".vol files)",
            self.source_id,
            len(new_images),
            len(target_images),
            self._target_images,
            len(source_uris),
        )
        return target_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Copy a .vol file to the worker's temp directory for processing."""
        dest_with_ext = await self._repository.download(source_uri, dest_path)
        logger.info(
            "[%s] Downloaded %s to %s",
            self.source_id,
            Path(source_uri).name,
            dest_with_ext,
        )
        return dest_with_ext

    def _matches_variable(self, source_uri: str) -> bool:
        """True when the filename parses and names this source's variable."""
        try:
            parsed = parse_inta_filename(Path(source_uri).name)
        except ValueError as exc:
            logger.debug("Skipping file with invalid name: %s (%s)", source_uri, exc)
            return False
        return parsed["variable"] == self._rainbow_variable

    async def _read_headers(self, source_uris: list[str]) -> list[tuple[str, object]]:
        """Read each file's Rainbow header, bounded by a semaphore.

        A file whose header cannot be parsed yields ``None`` rather than
        raising: one malformed volume must not sink the whole discovery tick.
        """
        semaphore = asyncio.Semaphore(self.HEADER_CONCURRENCY)

        async def read(uri: str) -> tuple[str, object]:
            async with semaphore:
                try:
                    window = await self._repository.read_header(uri)
                    return uri, parse_rainbow_header(window)
                except (RainbowHeaderError, OSError) as exc:
                    logger.debug("Unreadable Rainbow header in %s: %s", uri, exc)
                    return uri, None

        return list(await asyncio.gather(*(read(uri) for uri in source_uris)))

    def _cap_per_station(self, images: list[ImageInfo]) -> list[ImageInfo]:
        """Keep only the newest ``target_images`` per station."""
        by_radar: dict[str, list[ImageInfo]] = defaultdict(list)
        for img in images:
            by_radar[img.image_id.split("_")[0]].append(img)

        capped: list[ImageInfo] = []
        for station_images in by_radar.values():
            station_images.sort(key=lambda img: img.image_id, reverse=True)
            capped.extend(station_images[: self._target_images])
        return capped
