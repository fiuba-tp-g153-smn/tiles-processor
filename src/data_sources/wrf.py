"""WRF-ARG4K data source — discovers FIELD2D NetCDF files via a repository."""

from datetime import datetime, timedelta, timezone
from logging import getLogger
from pathlib import Path

from data_sources.base import DataSource, DiscoveryConfig, ImageInfo
from data_sources.wrf_repository import WrfFileRepository
from models.wrf_config import WrfProductConfig, parse_wrf_filename

logger = getLogger(__name__)


class WrfDataSource(DataSource):
    """
    Data source for WRF-ARG4K model output (SMN Argentina).

    Discovers FIELD2D .nc files via the injected repository (local folder or
    S3 bucket with the same layout). One instance per
    product: each creates image_ids unique to (product_id, init_tag, fxxx).
    The processor derives the FIELD3D path from the FIELD2D path when needed.

    File naming convention:
        WRF_ARG4K.FCST_L0_FIELD2D.01H.<INIT_TAG>.<FXXX>.M000.nc
    F000 (initialization hour) is processed for every product except those
    with ``skip_f000=True`` (1h-accumulation products lacking pp01H at init).
    """

    # Máximo de corridas (init_tags) a publicar: las más recientes, contadas
    # antes del dedup para que una corrida ya procesada no ceda su lugar a una
    # más vieja. data-service muestra las últimas 3 (inits_to_keep).
    TARGET_RUNS = 3  # default; overridable via settings.json

    # Corridas cada 6 h, más un par de horas hasta que se escriben: la tercera
    # más nueva empieza ~20 h atrás. Lo anterior no se muestra nunca.
    MAX_AGE_HOURS = 24  # default; overridable via settings.json

    def __init__(
        self,
        product_config: WrfProductConfig,
        repository: WrfFileRepository,
        target_runs: int | None = None,
        max_age_hours: int | None = None,
    ):
        self._product_config = product_config
        self._repository = repository
        self._target_runs = target_runs if target_runs is not None else self.TARGET_RUNS
        self._max_age = timedelta(
            hours=max_age_hours if max_age_hours is not None else self.MAX_AGE_HOURS
        )

    @property
    def source_id(self) -> str:
        return f"wrf_arg4k_{self._product_config.product_id}"

    @property
    def processor_id(self) -> str:
        return "wrf_arg4k"

    @property
    def product_config(self) -> WrfProductConfig:
        """The WRF product configuration for this source."""
        return self._product_config

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """Discover unprocessed WRF forecast steps for this product.

        Only steps of the newest ``target_runs`` runs that started within the
        max age window are considered; the cap is taken before dedup, so a run
        already processed never lets an older one through.
        """
        source_uris = await self._repository.list_files()
        if not source_uris:
            logger.warning(
                "[%s] No WRF FIELD2D files found in input dir", self.source_id
            )
            return []

        steps = self._recent_steps(source_uris, config.current_time - self._max_age)
        runs_newest_first = sorted({init for init, _, _ in steps}, reverse=True)
        allowed = set(runs_newest_first[: self._target_runs])

        target = [
            self._to_image_info(uri, image_id)
            for init, uri, image_id in steps
            if init in allowed
            and image_id not in config.existing_tilesets
            and image_id not in config.in_progress_images
        ]

        logger.info(
            "[%s] %d step(s) across %d recent run(s); publishing %d new from "
            "newest %d run(s)",
            self.source_id,
            len(steps),
            len(runs_newest_first),
            len(target),
            self._target_runs,
        )
        return target

    def _recent_steps(
        self, source_uris: list[str], cutoff: datetime
    ) -> list[tuple[str, str, str]]:
        """(init_tag, source_uri, image_id) of every step whose run started at or
        after ``cutoff``. Malformed names and skipped F000 steps are dropped."""
        steps: list[tuple[str, str, str]] = []
        too_old = 0
        for source_uri in source_uris:
            filename = Path(source_uri).name
            try:
                parsed = parse_wrf_filename(filename)
                init_time = _parse_init_time(parsed["init_tag"])
            except ValueError as e:
                logger.debug("Skipping file with invalid name: %s (%s)", filename, e)
                continue

            # F000 se descarta solo en productos de acumulado 1h (pp01H), donde
            # la variable no existe en la hora de inicialización. El resto sí.
            if parsed["fnum"] == 0 and self._product_config.skip_f000:
                continue
            if init_time < cutoff:
                too_old += 1
                continue

            image_id = (
                f"{self._product_config.product_id}_"
                f"{parsed['init_tag']}_{parsed['fxxx']}"
            )
            steps.append((parsed["init_tag"], source_uri, image_id))

        if too_old:
            logger.info(
                "[%s] Skipped %d files from runs older than %d h",
                self.source_id,
                too_old,
                self._max_age // timedelta(hours=1),
            )
        return steps

    def _to_image_info(self, source_uri: str, image_id: str) -> ImageInfo:
        return ImageInfo(
            image_id=image_id,
            source_uri=source_uri,
            data_source_id=self.source_id,
            processor_id=self.processor_id,
            output_prefix=self._product_config.s3_tiles_prefix,
        )

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """Copy WRF FIELD2D NetCDF file to the worker's work directory."""
        dest = await self._repository.download(source_uri, dest_path)
        logger.info(
            "[%s] Copied %s → %s",
            self.source_id,
            Path(source_uri).name,
            dest,
        )
        return dest


def _parse_init_time(init_tag: str) -> datetime:
    """Parse a WRF init_tag (``20260601_000000``, UTC)."""
    return datetime.strptime(init_tag, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
