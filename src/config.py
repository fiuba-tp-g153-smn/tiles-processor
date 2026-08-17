"""Application configuration loaded from environment variables and settings.json."""

import json
import logging
import os
import socket
from pathlib import Path
from typing import Any, Dict

from models.gfs_config import GfsAccessConfig
from models.input_source_config import (
    INPUT_MODE_LOCAL,
    INPUT_MODE_S3,
    InputSourceConfig,
)
from models.lifecycle_config import resolve_retention_map
from models.radar_config import RadarStationFilter


class Config:  # pylint: disable=too-many-instance-attributes,invalid-name
    """Application configuration from environment variables and settings.json.

    Attributes use UPPER_CASE to match their environment variable names,
    following the convention used by Django, Flask, and other Python frameworks.
    """

    def __init__(  # pylint: disable=too-many-statements,too-many-locals
        self, settings_path: Path | None = None
    ):
        if settings_path is None:
            settings_path = Path(__file__).parent.parent / "settings.json"

        settings = self._load_settings(settings_path)

        # Environment variables
        self.LOG_LEVEL: str = self._get_required_env("LOG_LEVEL").upper()
        self.DATA_DIR: str = self._get_required_env("DATA_DIR")
        self.TMP_DIR: str = str(Path(self.DATA_DIR) / "tmp")

        # S3 Configuration
        self.S3_TILES_DATA_ENDPOINT: str = self._get_required_env(
            "S3_TILES_DATA_ENDPOINT"
        )
        self.S3_TILES_DATA_RW_ACCESS_KEY: str = self._get_required_env(
            "S3_TILES_DATA_TILES_PROCESSOR_USER"
        )
        self.S3_TILES_DATA_RW_SECRET_KEY: str = self._get_required_env(
            "S3_TILES_DATA_TILES_PROCESSOR_PASSWORD"
        )
        self.S3_TILES_DATA_BUCKET_NAME: str = self._get_required_env(
            "S3_TILES_DATA_BUCKET_NAME"
        )
        self.S3_TILES_DATA_SECURE: bool = (
            os.getenv("S3_TILES_DATA_SECURE", "false").lower() == "true"
        )
        # Max concurrent tile/COG/GRIB uploads per S3 client (separate from
        # downloads); also sizes the aioboto3 connection pool. Total concurrent
        # PUTs against the gateway ≈ workers × WORKER_CONCURRENCY × this. `or`
        # (not getenv default) so a compose-supplied empty string also defaults.
        self.S3_UPLOAD_CONCURRENCY: int = int(
            os.getenv("S3_UPLOAD_CONCURRENCY") or "32"
        )

        # RabbitMQ Configuration
        self.RABBITMQ_HOST: str = self._get_required_env("RABBITMQ_HOST")
        self.RABBITMQ_PORT: int = int(self._get_required_env("RABBITMQ_PORT"))
        self.RABBITMQ_USER: str = self._get_required_env("RABBITMQ_USER")
        self.RABBITMQ_PASSWORD: str = self._get_required_env("RABBITMQ_PASSWORD")
        self.RABBITMQ_QUEUE: str = self._get_required_env("RABBITMQ_QUEUE")
        self.RABBITMQ_DLQ: str = self._get_required_env("RABBITMQ_DLQ")
        self.RABBITMQ_DLX: str = self._get_required_env("RABBITMQ_DLX")
        # Two work queues for lightweight units, split so radar and WRF never
        # head-of-line block each other (a 700+ WRF run vs the steady radar
        # stream). Workers round-robin the two; the producer routes per unit.
        # Defaulted (not required) so existing .env files keep working; `or`
        # (not getenv default) so a compose-supplied empty string also falls
        # back to the default.
        self.RABBITMQ_RADAR_LIGHT_QUEUE: str = (
            os.getenv("RABBITMQ_RADAR_LIGHT_QUEUE") or "tiles_radar_light_queue"
        )
        self.RABBITMQ_WRF_LIGHT_QUEUE: str = (
            os.getenv("RABBITMQ_WRF_LIGHT_QUEUE") or "tiles_wrf_light_queue"
        )

        # "normal" (default) drains RABBITMQ_QUEUE strict-first, then
        # round-robins the two light queues; "light" only round-robins the
        # light queues. Set per pool in compose.
        self.WORKER_TYPE: str = (os.getenv("WORKER_TYPE") or "normal").strip().lower()
        if self.WORKER_TYPE not in ("normal", "light"):
            raise ValueError(
                f"WORKER_TYPE must be 'normal' or 'light', got '{self.WORKER_TYPE}'"
            )

        # How many work units a worker processes concurrently as asyncio tasks.
        # Overlaps one unit's I/O-bound upload tail with the next unit's
        # CPU-bound compute. `or` so a compose-supplied empty string defaults.
        self.WORKER_CONCURRENCY: int = int(os.getenv("WORKER_CONCURRENCY") or "2")
        if self.WORKER_CONCURRENCY < 1:
            raise ValueError(
                f"WORKER_CONCURRENCY must be >= 1, got {self.WORKER_CONCURRENCY}"
            )

        # Stable identifier for this worker, recorded as `worker_host` on every
        # job so the dashboard can group the timeline by container (worker1,
        # worker-light1, ...). Compose sets it per service; unset falls back to
        # the host name, preserving the prior behavior for old deploys/dev runs.
        self.WORKER_ID: str = os.getenv("WORKER_ID") or socket.gethostname()

        # ---- Settings from settings.json (grouped by concern) ----
        self.TIMEZONE: str = settings["timezone"]

        # Metrics + metrics API (the /status backend service)
        _metrics = settings.get("metrics", {})
        self.ENABLE_METRICS: bool = _metrics.get("enabled", True)
        self.METRICS_DB_PATH: str = _metrics.get(
            "db_path", str(Path(self.TMP_DIR) / "metrics.db")
        )
        # Hard cap on job_metrics rows (producer prunes to the newest N). ~0.6 KB/row,
        # so 1,000,000 ≈ ~600 MB. Bounds metrics.db growth.
        self.METRICS_MAX_ROWS: int = int(_metrics.get("max_rows", 1_000_000))
        self.METRICS_API_PORT: int = int(os.getenv("METRICS_API_PORT", "6020"))
        # API key required by the metrics API's write endpoints (e.g. /api/import).
        # Empty disables writes (they fail closed with 503). Reads stay open.
        self.METRICS_API_KEY: str = os.getenv("METRICS_API_KEY", "")

        # Each data source's full config lives under "sources.<name>": its input
        # repository, product toggles, and any per-source tuning, co-located.
        _sources: Dict[str, Any] = settings.get("sources", {})
        _goes19 = _sources.get("goes19", {})
        _glm = _sources.get("glm", {})
        _radar = _sources.get("radar", {})
        _wrf = _sources.get("wrf", {})
        _ecmwf = _sources.get("ecmwf", {})
        _gfs = _sources.get("gfs", {})

        # --- GOES-19 ABI ---
        # Discovery knobs are None when unset -> the data source keeps its own
        # class-constant default (see factories.py). Same pattern for glm/radar/wrf.
        _goes19_products = _goes19.get("products", {})
        self.ENABLE_BAND_13: bool = _goes19_products.get("band_13", True)
        self.ENABLE_BAND_9: bool = _goes19_products.get("band_9", True)
        self.ENABLE_BAND_2: bool = _goes19_products.get("band_2", False)
        self.GOES_TARGET_IMAGES: int | None = self._opt_int(
            _goes19.get("target_images"), "sources.goes19.target_images"
        )
        self.GOES_MAX_HOURS_BACK: int | None = self._opt_int(
            _goes19.get("max_hours_back"), "sources.goes19.max_hours_back", minimum=0
        )

        # --- GLM (pre-gridded CG_GLM-L2-GLMF folder) ---
        _glm_products = _glm.get("products", {})
        self.ENABLE_GLM_FED: bool = _glm_products.get("fed", False)
        self.ENABLE_GLM_TOE: bool = _glm_products.get("toe", False)
        self.ENABLE_GLM_MFA: bool = _glm_products.get("mfa", False)
        self.GLM_ACCUM_MINUTES: int = int(_glm.get("accum_minutes", 10))
        self.GLM_PRODUCE_EVERY_MINUTES: int = int(_glm.get("produce_every_minutes", 10))
        self.GLM_SAFETY_LAG_SECONDS: int | None = self._opt_int(
            _glm.get("safety_lag_seconds"), "sources.glm.safety_lag_seconds", minimum=0
        )
        self.GLM_TARGET_WINDOWS: int | None = self._opt_int(
            _glm.get("target_windows"), "sources.glm.target_windows"
        )

        # --- Radar (SINARAME) ---
        _radar_product_ids = ["DBZH", "ZDR", "RHOHV", "KDP", "VRAD"]
        _radar_products = _radar.get("products", {})
        self.ENABLED_RADAR_PRODUCTS: dict[str, bool] = {
            pid: _radar_products.get(pid, False) for pid in _radar_product_ids
        }
        # Per-radar-station enablement, AND-combined with the product flags above:
        # a (radar, product) pair is processed iff the product is enabled AND the
        # station filter allows the radar. Accepts "all" (default), "none",
        # {"whitelist": [...]}, or {"blacklist": [...]}; see RadarStationFilter.
        self.RADAR_STATION_FILTER: RadarStationFilter = (
            RadarStationFilter.from_settings(_radar.get("stations"))
        )
        self.RADAR_TARGET_IMAGES: int | None = self._opt_int(
            _radar.get("target_images"), "sources.radar.target_images"
        )

        # --- WRF (WRF-ARG4K FIELD2D) ---
        _wrf_products = _wrf.get("products", {})
        self.ENABLED_WRF_PRODUCTS: dict[str, bool] = {
            pid: _wrf_products.get(pid, False)
            for pid in [
                "Colmax",
                "Rafagas",
                "Campo900hPa",
                "Precipitacion1h",
                "MUCAPE",
                "AguaPrecipitable",
                "JetCapasBajas",
                "CortanteNivelesBajos",
                "CAPE_BRN",
                "Granizo",
            ]
        }
        self.WRF_TARGET_RUNS: int | None = self._opt_int(
            _wrf.get("target_runs"), "sources.wrf.target_runs"
        )

        # --- ECMWF ---
        _ecmwf_products = _ecmwf.get("products", {})
        self.ENABLE_ECMWF_PRECIPITATION: bool = _ecmwf_products.get(
            "precipitation", False
        )
        self.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE: bool = _ecmwf_products.get(
            "mean_sea_level_pressure", False
        )
        _ecmwf_mslp = _ecmwf.get("mslp", {})
        self.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE: float = float(
            _ecmwf_mslp.get("isobar_simplify_tolerance", 0.1)
        )
        self.ECMWF_MSLP_SMOOTHING_SIGMA: float = float(
            _ecmwf_mslp.get("smoothing_sigma", 1.5)
        )
        self.ECMWF_TP_SMOOTHING_RESOLUTION_DEG: float = float(
            os.getenv("ECMWF_TP_SMOOTHING_RESOLUTION_DEG") or "0.01"
        )
        self.ECMWF_OPENDATA_SOURCES: tuple[str, ...] = tuple(
            s.strip()
            for s in (os.getenv("ECMWF_OPENDATA_SOURCES") or "ecmwf,azure,aws").split(
                ","
            )
            if s.strip()
        )

        # --- GFS ---
        _gfs_products = _gfs.get("products", {})
        self.ENABLE_GFS_MSLP: bool = _gfs_products.get("mslp", False)
        self.ENABLE_GFS_500: bool = _gfs_products.get("500", False)
        self.ENABLE_GFS_250: bool = _gfs_products.get("250", False)
        self.GFS_ACCESS: GfsAccessConfig = self._parse_gfs_access(_gfs)
        self.GFS_CYCLES_TO_MAINTAIN: int = int(_gfs.get("cycles_to_maintain", 3))
        self.GFS_MAX_STEPS_PER_TICK: int = int(_gfs.get("max_steps_per_tick", 12))
        _gfs_probe = _gfs.get("availability_probe_hours", {})
        self.GFS_AVAILABILITY_PROBE_FROM_HOURS: int = int(_gfs_probe.get("from", 3))
        self.GFS_AVAILABILITY_PROBE_TO_HOURS: int = int(_gfs_probe.get("to", 8))
        self.GFS_SMOOTHING_SIGMA: float = float(_gfs.get("smoothing_sigma", 1.5))
        self.GFS_ISOLINE_SIMPLIFY_TOLERANCE: float = float(
            _gfs.get("isoline_simplify_tolerance", 0.05)
        )
        self.GFS_TILE_SMOOTHING_RESOLUTION_DEG: float = float(
            os.getenv("GFS_TILE_SMOOTHING_RESOLUTION_DEG") or "0.01"
        )

        # --- Per-source input repositories (local folder or S3, same layout).
        # Mode/dir/bucket/endpoint/prefix come from sources.<name>.input;
        # credentials from {ENV_PREFIX}_S3_ACCESS_KEY/_SECRET_KEY env vars. ---
        self.RADAR_INPUT: InputSourceConfig = self._parse_input_source(
            _radar, "radar", default_dir=str(Path(self.DATA_DIR) / "radar_h5")
        )
        self.GLM_FOLDER_INPUT: InputSourceConfig = self._parse_input_source(
            _glm,
            "glm",
            env_prefix="GLM_FOLDER",
            default_dir=str(Path(self.DATA_DIR) / "glm_h5"),
        )
        self.WRF_INPUT: InputSourceConfig = self._parse_input_source(
            _wrf, "wrf", default_dir=str(Path(self.DATA_DIR) / "wrf_nc")
        )
        self.GOES19_INPUT: InputSourceConfig = self._parse_input_source(
            _goes19,
            "goes19",
            default_dir=str(Path(self.DATA_DIR) / "goes19"),
            default_mode=INPUT_MODE_S3,
            default_bucket="noaa-goes19",
        )
        # Legacy *_INPUT_DIR aliases retained for callers that read them directly.
        self.RADAR_INPUT_DIR: str = self.RADAR_INPUT.input_dir
        self.GLM_FOLDER_INPUT_DIR: str = self.GLM_FOLDER_INPUT.input_dir
        self.WRF_INPUT_DIR: str = self.WRF_INPUT.input_dir

        # Light-queue routing: matching units go to the light queue so a larger
        # pool of cheap workers drains them in parallel with the heavy
        # GOES/GLM/ECMWF queue. Radar routes all-or-nothing ("all"/"none"); WRF
        # accepts "all"/"none"/an explicit product list.
        self.LIGHT_QUEUE_ALL_RADAR: bool = self._parse_radar_light_queue(
            _radar.get("light_queue")
        )
        self.LIGHT_QUEUE_WRF_PRODUCTS: frozenset[str] = self._parse_light_queue(
            _wrf.get("light_queue"), self.ENABLED_WRF_PRODUCTS.keys()
        )

        # Tile-bucket S3 lifecycle: {prefix: days}, resolved from each source's
        # retention_days. One non-overlapping rule per prefix is applied at
        # worker boot (see S3Client.configure_lifecycle_policy).
        self.TILE_LIFECYCLE_RETENTION: dict[str, int] = resolve_retention_map(_sources)

        # Job Configuration
        self.JOB_TTL_MINUTES: int = int(self._get_required_env("JOB_TTL_MINUTES"))

        # Health Check
        self.HEALTH_PORT: int = int(os.getenv("HEALTH_PORT", "8080"))

        # Bounding box (from JSON)
        # Coordinates are in EPSG:4326 (longitude/latitude)
        self.BOUNDS_MINX: float = float(settings["bounds"]["minx"])  # West longitude
        self.BOUNDS_MINY: float = float(settings["bounds"]["miny"])  # South latitude
        self.BOUNDS_MAXX: float = float(settings["bounds"]["maxx"])  # East longitude
        self.BOUNDS_MAXY: float = float(settings["bounds"]["maxy"])  # North latitude
        self._validate_bounds()

    @staticmethod
    def _parse_gfs_access(gfs_cfg: Dict[str, Any]) -> GfsAccessConfig:
        """Parse GFS endpoint access from sources.gfs + env overrides.

        Intentionally separate from `_parse_input_source`: that helper models a
        file repository with a local/S3 folder layout, an invariant the other
        sources rely on. GFS reads through an HTTP CGI, which shares none of it.
        The endpoint is env-only (per-environment, never baked into the image);
        only the tuning knobs come from settings.json.
        """
        return GfsAccessConfig(
            subset_endpoint=os.getenv("GFS_SUBSET_ENDPOINT") or "",
            timeout_seconds=int(gfs_cfg.get("http_timeout_seconds", 120)),
            max_concurrent_downloads=int(gfs_cfg.get("max_concurrent_downloads", 2)),
        )

    @staticmethod
    def _parse_input_source(  # pylint: disable=too-many-arguments
        source_cfg: Dict[str, Any],
        json_key: str,
        *,
        default_dir: str,
        env_prefix: str | None = None,
        default_mode: str = INPUT_MODE_LOCAL,
        default_bucket: str | None = None,
    ) -> InputSourceConfig:
        """Parse one source's input config from ``sources.<json_key>.input``.

        ``json_key`` names the source in error messages (its JSON path);
        ``env_prefix`` (defaulting to its upper-case) names the credential env
        vars, so a source whose JSON key differs from its historical env prefix
        keeps both stable.
        """
        env_prefix = env_prefix or json_key.upper()
        inp = source_cfg.get("input", {})
        mode = inp.get("mode", default_mode)
        if mode not in (INPUT_MODE_LOCAL, INPUT_MODE_S3):
            raise ValueError(
                f"sources.{json_key}.input.mode must be 'local' or 's3', got '{mode}'"
            )
        bucket = inp.get("s3_bucket", default_bucket)
        if mode == INPUT_MODE_S3 and not bucket:
            raise ValueError(
                f"sources.{json_key}.input has mode 's3' but no s3_bucket set"
            )
        # `or None` normalizes compose-supplied empty strings to unset.
        access_key = os.getenv(f"{env_prefix}_S3_ACCESS_KEY") or None
        secret_key = os.getenv(f"{env_prefix}_S3_SECRET_KEY") or None
        if bool(access_key) != bool(secret_key):
            # S3Client silently falls back to anonymous on a half-set pair.
            raise ValueError(
                f"{env_prefix}_S3_ACCESS_KEY and {env_prefix}_S3_SECRET_KEY "
                "must be set together (or neither, for anonymous access)"
            )
        return InputSourceConfig(
            mode=mode,
            input_dir=inp.get("dir", default_dir),
            s3_bucket=bucket,
            s3_endpoint=inp.get("s3_endpoint") or None,
            s3_prefix=inp.get("s3_prefix", ""),
            s3_secure=bool(inp.get("s3_secure", False)),
            s3_access_key=access_key,
            s3_secret_key=secret_key,
        )

    @staticmethod
    def _opt_int(raw: Any, name: str, *, minimum: int = 1) -> int | None:
        """Validate an optional integer setting; ``None`` (unset) passes through.

        Returns None when absent so the consuming data source keeps its own
        class-constant default; otherwise fails fast on a non-int or out-of-range
        value (bool is rejected — it is an int subclass but never a valid count).
        """
        if raw is None:
            return None
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}, got {raw!r}")
        return raw

    @staticmethod
    def _parse_light_queue(raw: Any, all_ids) -> frozenset[str]:
        """Resolve a light_queue selector into the product IDs it routes.

        Accepts "all" (every product), "none"/absent (empty), or an explicit
        list of product IDs.
        """
        if raw is None or raw == "none":
            return frozenset()
        if raw == "all":
            return frozenset(all_ids)
        if isinstance(raw, list) and all(isinstance(s, str) for s in raw):
            return frozenset(raw)
        raise ValueError(
            'light_queue must be "all", "none", or a list of product IDs, '
            f"got {raw!r}"
        )

    @staticmethod
    def _parse_radar_light_queue(raw: Any) -> bool:
        """Radar routes all-or-nothing (worker pools split by queue, not by radar
        product), so its light_queue accepts only "all"/"none"/absent."""
        if raw is None or raw == "none":
            return False
        if raw == "all":
            return True
        raise ValueError(
            'sources.radar.light_queue must be "all" or "none" (radar routes '
            f"all-or-nothing), got {raw!r}"
        )

    @staticmethod
    def _get_required_env(key: str) -> str:
        """Get a required environment variable, raising if not set."""
        value = os.getenv(key)
        if not value or not value.strip():
            raise ValueError(
                f"Environment variable '{key}' is required but not set or empty."
            )
        return value

    @staticmethod
    def _load_settings(settings_path: Path) -> Dict[str, Any]:
        """Load settings from JSON file."""
        if not settings_path.exists():
            raise FileNotFoundError(
                f"Settings file not found at '{settings_path}'. "
                "Please create a settings.json file in the project root."
            )
        with open(settings_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _validate_bounds(self) -> None:
        """Fail fast on a swapped/typo'd bounding box before it reaches every clip.

        An inverted or out-of-range box silently yields empty/garbage clips for
        every product (noticed only downstream as blank tiles), so reject it at
        construction, naming the offending field — mirroring the WORKER_TYPE /
        WORKER_CONCURRENCY checks above.
        """
        for name, lng in (
            ("BOUNDS_MINX", self.BOUNDS_MINX),
            ("BOUNDS_MAXX", self.BOUNDS_MAXX),
        ):
            if not -180.0 <= lng <= 180.0:
                raise ValueError(
                    f"{name} must be a longitude in [-180, 180], got {lng}"
                )
        for name, lat in (
            ("BOUNDS_MINY", self.BOUNDS_MINY),
            ("BOUNDS_MAXY", self.BOUNDS_MAXY),
        ):
            if not -90.0 <= lat <= 90.0:
                raise ValueError(f"{name} must be a latitude in [-90, 90], got {lat}")
        if self.BOUNDS_MINX >= self.BOUNDS_MAXX:
            raise ValueError(
                f"BOUNDS_MINX ({self.BOUNDS_MINX}) must be < "
                f"BOUNDS_MAXX ({self.BOUNDS_MAXX})"
            )
        if self.BOUNDS_MINY >= self.BOUNDS_MAXY:
            raise ValueError(
                f"BOUNDS_MINY ({self.BOUNDS_MINY}) must be < "
                f"BOUNDS_MAXY ({self.BOUNDS_MAXY})"
            )

    def get_bounds(self) -> Dict[str, float]:
        """Get the bounding box configuration for clipping."""
        return {
            "minx": self.BOUNDS_MINX,
            "miny": self.BOUNDS_MINY,
            "maxx": self.BOUNDS_MAXX,
            "maxy": self.BOUNDS_MAXY,
        }

    def log_config(self) -> None:  # pylint: disable=too-many-statements
        """Log the current configuration values."""
        logger = logging.getLogger(__name__)
        logger.info("=== Configuration ===")
        logger.info("LOG_LEVEL: %s", self.LOG_LEVEL)
        logger.info("TIMEZONE: %s", self.TIMEZONE)
        logger.info("ENABLE_BAND_13: %s", self.ENABLE_BAND_13)
        logger.info("ENABLE_BAND_9: %s", self.ENABLE_BAND_9)
        logger.info("ENABLE_BAND_2: %s", self.ENABLE_BAND_2)
        logger.info("ENABLE_GLM_FED: %s", self.ENABLE_GLM_FED)
        logger.info("ENABLE_GLM_TOE: %s", self.ENABLE_GLM_TOE)
        logger.info("ENABLE_GLM_MFA: %s", self.ENABLE_GLM_MFA)
        logger.info("ENABLE_ECMWF_PRECIPITATION: %s", self.ENABLE_ECMWF_PRECIPITATION)
        logger.info(
            "ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE: %s",
            self.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE,
        )
        logger.info(
            "ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE: %s",
            self.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE,
        )
        logger.info("ECMWF_MSLP_SMOOTHING_SIGMA: %s", self.ECMWF_MSLP_SMOOTHING_SIGMA)
        logger.info("ENABLE_GFS_MSLP: %s", self.ENABLE_GFS_MSLP)
        logger.info("ENABLE_GFS_500: %s", self.ENABLE_GFS_500)
        logger.info("ENABLE_GFS_250: %s", self.ENABLE_GFS_250)
        logger.info("GFS_SUBSET_ENDPOINT: %s", self.GFS_ACCESS.subset_endpoint)
        logger.info(
            "GFS_MAX_CONCURRENT_DOWNLOADS: %s", self.GFS_ACCESS.max_concurrent_downloads
        )
        logger.info("GFS_MAX_STEPS_PER_TICK: %s", self.GFS_MAX_STEPS_PER_TICK)
        for pid, enabled in self.ENABLED_RADAR_PRODUCTS.items():
            logger.info("ENABLE_RADAR_%s: %s", pid, enabled)
        _radar_filter = self.RADAR_STATION_FILTER
        logger.info(
            "RADAR_STATION_FILTER: mode=%s stations=%s",
            _radar_filter.mode,
            ", ".join(sorted(_radar_filter.stations)) or "-",
        )
        logger.info("RADAR_INPUT_DIR: %s", self.RADAR_INPUT_DIR)
        logger.info("GLM_FOLDER_INPUT_DIR: %s", self.GLM_FOLDER_INPUT_DIR)
        for name, src in (
            ("RADAR", self.RADAR_INPUT),
            ("GLM_FOLDER", self.GLM_FOLDER_INPUT),
            ("WRF", self.WRF_INPUT),
            ("GOES19", self.GOES19_INPUT),
        ):
            logger.info(
                "%s_INPUT: mode=%s dir=%s bucket=%s endpoint=%s prefix=%s "
                "credentials=%s",
                name,
                src.mode,
                src.input_dir,
                src.s3_bucket,
                src.s3_endpoint,
                src.s3_prefix,
                "set" if src.s3_access_key else "anonymous",
            )
        logger.info("GLM_ACCUM_MINUTES: %s", self.GLM_ACCUM_MINUTES)
        logger.info("GLM_PRODUCE_EVERY_MINUTES: %s", self.GLM_PRODUCE_EVERY_MINUTES)
        for pid, enabled in self.ENABLED_WRF_PRODUCTS.items():
            logger.info("ENABLE_WRF_%s: %s", pid, enabled)
        logger.info("WRF_INPUT_DIR: %s", self.WRF_INPUT_DIR)
        logger.info("DATA_DIR: %s", self.DATA_DIR)
        logger.info("TMP_DIR: %s", self.TMP_DIR)
        logger.info("BOUNDS_MINX: %s", self.BOUNDS_MINX)
        logger.info("BOUNDS_MINY: %s", self.BOUNDS_MINY)
        logger.info("BOUNDS_MAXX: %s", self.BOUNDS_MAXX)
        logger.info("BOUNDS_MAXY: %s", self.BOUNDS_MAXY)
        logger.info("S3_TILES_DATA_ENDPOINT: %s", self.S3_TILES_DATA_ENDPOINT)
        logger.info("S3_TILES_DATA_BUCKET_NAME: %s", self.S3_TILES_DATA_BUCKET_NAME)
        logger.info("S3_TILES_DATA_SECURE: %s", self.S3_TILES_DATA_SECURE)
        logger.info("S3_UPLOAD_CONCURRENCY: %s", self.S3_UPLOAD_CONCURRENCY)
        logger.info("RABBITMQ_HOST: %s", self.RABBITMQ_HOST)
        logger.info("RABBITMQ_PORT: %s", self.RABBITMQ_PORT)
        logger.info("RABBITMQ_QUEUE: %s", self.RABBITMQ_QUEUE)
        logger.info("RABBITMQ_RADAR_LIGHT_QUEUE: %s", self.RABBITMQ_RADAR_LIGHT_QUEUE)
        logger.info("RABBITMQ_WRF_LIGHT_QUEUE: %s", self.RABBITMQ_WRF_LIGHT_QUEUE)
        logger.info("WORKER_TYPE: %s", self.WORKER_TYPE)
        logger.info("WORKER_CONCURRENCY: %s", self.WORKER_CONCURRENCY)
        logger.info("RABBITMQ_DLQ: %s", self.RABBITMQ_DLQ)
        logger.info("RABBITMQ_DLX: %s", self.RABBITMQ_DLX)
        logger.info("WORKER_ID: %s", self.WORKER_ID)
        logger.info("LIGHT_QUEUE_ALL_RADAR: %s", self.LIGHT_QUEUE_ALL_RADAR)
        logger.info(
            "LIGHT_QUEUE_WRF_PRODUCTS: %s",
            ", ".join(sorted(self.LIGHT_QUEUE_WRF_PRODUCTS)) or "(none)",
        )
        logger.info("JOB_TTL_MINUTES: %s", self.JOB_TTL_MINUTES)
        logger.info("HEALTH_PORT: %s", self.HEALTH_PORT)
        logger.info("ENABLE_METRICS: %s", self.ENABLE_METRICS)
        logger.info("METRICS_DB_PATH: %s", self.METRICS_DB_PATH)
        logger.info("METRICS_MAX_ROWS: %s", self.METRICS_MAX_ROWS)
        logger.info("METRICS_API_PORT: %s", self.METRICS_API_PORT)
        logger.info("METRICS_API_KEY: %s", "set" if self.METRICS_API_KEY else "unset")
        logger.info(
            "TILE_LIFECYCLE_RETENTION: %s",
            ", ".join(
                f"{prefix}={days}d"
                for prefix, days in sorted(self.TILE_LIFECYCLE_RETENTION.items())
            ),
        )
        logger.info("=====================")
