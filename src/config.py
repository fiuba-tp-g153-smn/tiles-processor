"""Application configuration loaded from environment variables and settings.json."""

import json
import logging
import os
import socket
from pathlib import Path
from typing import Any, Dict

from models.input_source_config import (
    INPUT_MODE_LOCAL,
    INPUT_MODE_S3,
    InputSourceConfig,
)


class Config:  # pylint: disable=too-many-instance-attributes,invalid-name
    """Application configuration from environment variables and settings.json.

    Attributes use UPPER_CASE to match their environment variable names,
    following the convention used by Django, Flask, and other Python frameworks.
    """

    def __init__(  # pylint: disable=too-many-statements
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

        # RabbitMQ Configuration
        self.RABBITMQ_HOST: str = self._get_required_env("RABBITMQ_HOST")
        self.RABBITMQ_PORT: int = int(self._get_required_env("RABBITMQ_PORT"))
        self.RABBITMQ_USER: str = self._get_required_env("RABBITMQ_USER")
        self.RABBITMQ_PASSWORD: str = self._get_required_env("RABBITMQ_PASSWORD")
        self.RABBITMQ_QUEUE: str = self._get_required_env("RABBITMQ_QUEUE")
        self.RABBITMQ_DLQ: str = self._get_required_env("RABBITMQ_DLQ")
        self.RABBITMQ_DLX: str = self._get_required_env("RABBITMQ_DLX")
        # Second work queue for lightweight units (radar/WRF). Defaulted (not
        # required) so existing .env files keep working; light workers set
        # RABBITMQ_QUEUE to this value to consume it. `or` (not getenv default)
        # so a compose-supplied empty string also falls back to the default.
        self.RABBITMQ_LIGHT_QUEUE: str = (
            os.getenv("RABBITMQ_LIGHT_QUEUE") or "tiles_light_queue"
        )

        # Stable identifier for this worker, recorded as `worker_host` on every
        # job so the dashboard can group the timeline by container (worker1,
        # worker-light1, ...). Compose sets it per service; unset falls back to
        # the host name, preserving the prior behavior for old deploys/dev runs.
        self.WORKER_ID: str = os.getenv("WORKER_ID") or socket.gethostname()

        # Settings from JSON
        self.TIMEZONE: str = settings["timezone"]
        self.TILE_RETENTION_DAYS: int = settings.get("tile_retention_days", 1)

        # Feature Toggles (from JSON)
        self.ENABLE_BAND_13: bool = settings["features"].get("enable_band_13", True)
        self.ENABLE_BAND_9: bool = settings["features"].get("enable_band_9", True)
        self.ENABLE_BAND_2: bool = settings["features"].get("enable_band_2", False)
        self.ENABLE_GLM_FED: bool = settings["features"].get("enable_glm_fed", False)
        self.ENABLE_GLM_TOE: bool = settings["features"].get("enable_glm_toe", False)
        self.ENABLE_GLM_MFA: bool = settings["features"].get("enable_glm_mfa", False)
        self.ENABLE_ECMWF_PRECIPITATION: bool = settings["features"].get(
            "enable_ecmwf_precipitation", False
        )
        self.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE: bool = settings["features"].get(
            "enable_ecmwf_mean_sea_level_pressure", False
        )
        self.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE: float = float(
            settings.get("ecmwf_mslp_isobar_simplify_tolerance", 0.1)
        )
        self.ECMWF_MSLP_SMOOTHING_SIGMA: float = float(
            settings.get("ecmwf_mslp_smoothing_sigma", 1.5)
        )
        _radar_product_ids = ["DBZH", "ZDR", "RHOHV", "KDP", "VRAD"]
        self.ENABLED_RADAR_PRODUCTS: dict[str, bool] = {
            pid: settings["features"].get(f"enable_radar_{pid}", False)
            for pid in _radar_product_ids
        }

        # Metrics + metrics API (the /status backend service)
        self.ENABLE_METRICS: bool = settings["features"].get("enable_metrics", True)
        self.METRICS_DB_PATH: str = settings.get(
            "metrics_db_path", str(Path(self.TMP_DIR) / "metrics.db")
        )
        # Hard cap on job_metrics rows (producer prunes to the newest N). ~0.6 KB/row,
        # so 1,000,000 ≈ ~600 MB. Bounds metrics.db growth.
        self.METRICS_MAX_ROWS: int = int(settings.get("metrics_max_rows", 1_000_000))
        self.METRICS_API_PORT: int = int(os.getenv("METRICS_API_PORT", "6020"))
        # API key required by the metrics API's write endpoints (e.g. /api/import).
        # Empty disables writes (they fail closed with 503). Reads stay open.
        self.METRICS_API_KEY: str = os.getenv("METRICS_API_KEY", "")

        # Per-source input configuration (local folder or S3 bucket with the
        # same layout). Mode/bucket/endpoint/prefix come from settings.json;
        # credentials from {NAME}_S3_ACCESS_KEY/_SECRET_KEY env vars.
        self.RADAR_INPUT: InputSourceConfig = self._parse_input_source(
            settings, "radar", default_dir=str(Path(self.DATA_DIR) / "radar_h5")
        )
        self.GLM_FOLDER_INPUT: InputSourceConfig = self._parse_input_source(
            settings, "glm_folder", default_dir=str(Path(self.DATA_DIR) / "glm_h5")
        )
        self.WRF_INPUT: InputSourceConfig = self._parse_input_source(
            settings, "wrf", default_dir=str(Path(self.DATA_DIR) / "wrf_nc")
        )
        self.GOES19_INPUT: InputSourceConfig = self._parse_input_source(
            settings,
            "goes19",
            default_dir=str(Path(self.DATA_DIR) / "goes19"),
            default_mode=INPUT_MODE_S3,
            default_bucket="noaa-goes19",
        )

        # Radar Configuration
        # Path to directory containing .H5 radar files
        self.RADAR_INPUT_DIR: str = self.RADAR_INPUT.input_dir

        # GLM Folder Configuration (pre-gridded CG_GLM-L2-GLMF netCDFs)
        self.GLM_FOLDER_INPUT_DIR: str = self.GLM_FOLDER_INPUT.input_dir
        self.GLM_ACCUM_MINUTES: int = int(settings.get("glm_accum_minutes", 10))
        self.GLM_PRODUCE_EVERY_MINUTES: int = int(
            settings.get("glm_produce_every_minutes", 10)
        )
        self.GLM_RESOLUTION_DEG: float = float(settings.get("glm_resolution_deg", 0.02))

        # WRF Configuration
        self.ENABLED_WRF_PRODUCTS: dict[str, bool] = {
            pid: settings["features"].get(f"enable_wrf_{pid}", False)
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
        self.WRF_INPUT_DIR: str = self.WRF_INPUT.input_dir

        # Light-queue routing (settings.json). Units matching these go to the
        # light queue so a larger pool of cheap workers can drain them in
        # parallel with the heavy GOES/GLM/ECMWF queue.
        _light_queue = settings.get("light_queue", {})
        self.LIGHT_QUEUE_ALL_RADAR: bool = bool(_light_queue.get("radar", False))
        self.LIGHT_QUEUE_WRF_PRODUCTS: frozenset[str] = frozenset(
            _light_queue.get("wrf", [])
        )

        # Job Configuration
        self.JOB_TTL_MINUTES: int = int(self._get_required_env("JOB_TTL_MINUTES"))

        # SeaweedFS Filer (optional — only needed when using SeaweedFS)
        self.SEAWEEDFS_FILER_ENDPOINT: str | None = os.getenv(
            "SEAWEEDFS_FILER_ENDPOINT"
        )
        self.SEAWEEDFS_TILE_TTL: str | None = os.getenv("SEAWEEDFS_TILE_TTL", "1m")
        self.SEAWEEDFS_RADAR_TILE_TTL: str | None = os.getenv(
            "SEAWEEDFS_RADAR_TILE_TTL"
        )
        self.SEAWEEDFS_ECMWF_TTL: str | None = os.getenv("SEAWEEDFS_ECMWF_TTL")
        self.SEAWEEDFS_ECMWF_GRIB_TTL: str | None = os.getenv(
            "SEAWEEDFS_ECMWF_GRIB_TTL"
        )
        self.SEAWEEDFS_WRF_TTL: str | None = os.getenv("SEAWEEDFS_WRF_TTL")

        # Health Check
        self.HEALTH_PORT: int = int(os.getenv("HEALTH_PORT", "8080"))

        # Bounding box (from JSON)
        # Coordinates are in EPSG:4326 (longitude/latitude)
        self.BOUNDS_MINX: float = settings["bounds"]["minx"]  # West longitude
        self.BOUNDS_MINY: float = settings["bounds"]["miny"]  # South latitude
        self.BOUNDS_MAXX: float = settings["bounds"]["maxx"]  # East longitude
        self.BOUNDS_MAXY: float = settings["bounds"]["maxy"]  # North latitude

    @staticmethod
    def _parse_input_source(
        settings: Dict[str, Any],
        name: str,
        *,
        default_dir: str,
        default_mode: str = INPUT_MODE_LOCAL,
        default_bucket: str | None = None,
    ) -> InputSourceConfig:
        """Parse one source's input config from settings.json + env credentials."""
        mode = settings.get(f"{name}_input_mode", default_mode)
        if mode not in (INPUT_MODE_LOCAL, INPUT_MODE_S3):
            raise ValueError(f"{name}_input_mode must be 'local' or 's3', got '{mode}'")
        bucket = settings.get(f"{name}_s3_bucket", default_bucket)
        if mode == INPUT_MODE_S3 and not bucket:
            raise ValueError(
                f"{name}_input_mode is 's3' but {name}_s3_bucket is not set"
            )
        # `or None` normalizes compose-supplied empty strings to unset.
        access_key = os.getenv(f"{name.upper()}_S3_ACCESS_KEY") or None
        secret_key = os.getenv(f"{name.upper()}_S3_SECRET_KEY") or None
        if bool(access_key) != bool(secret_key):
            # S3Client silently falls back to anonymous on a half-set pair.
            raise ValueError(
                f"{name.upper()}_S3_ACCESS_KEY and {name.upper()}_S3_SECRET_KEY "
                "must be set together (or neither, for anonymous access)"
            )
        return InputSourceConfig(
            mode=mode,
            input_dir=settings.get(f"{name}_input_dir", default_dir),
            s3_bucket=bucket,
            s3_endpoint=settings.get(f"{name}_s3_endpoint") or None,
            s3_prefix=settings.get(f"{name}_s3_prefix", ""),
            s3_secure=bool(settings.get(f"{name}_s3_secure", False)),
            s3_access_key=access_key,
            s3_secret_key=secret_key,
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
        for pid, enabled in self.ENABLED_RADAR_PRODUCTS.items():
            logger.info("ENABLE_RADAR_%s: %s", pid, enabled)
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
        logger.info("GLM_RESOLUTION_DEG: %s", self.GLM_RESOLUTION_DEG)
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
        logger.info("SEAWEEDFS_FILER_ENDPOINT: %s", self.SEAWEEDFS_FILER_ENDPOINT)
        logger.info("SEAWEEDFS_TILE_TTL: %s", self.SEAWEEDFS_TILE_TTL)
        logger.info("SEAWEEDFS_RADAR_TILE_TTL: %s", self.SEAWEEDFS_RADAR_TILE_TTL)
        logger.info("SEAWEEDFS_ECMWF_TTL: %s", self.SEAWEEDFS_ECMWF_TTL)
        logger.info("SEAWEEDFS_ECMWF_GRIB_TTL: %s", self.SEAWEEDFS_ECMWF_GRIB_TTL)
        logger.info("SEAWEEDFS_WRF_TTL: %s", self.SEAWEEDFS_WRF_TTL)
        logger.info("RABBITMQ_HOST: %s", self.RABBITMQ_HOST)
        logger.info("RABBITMQ_PORT: %s", self.RABBITMQ_PORT)
        logger.info("RABBITMQ_QUEUE: %s", self.RABBITMQ_QUEUE)
        logger.info("RABBITMQ_LIGHT_QUEUE: %s", self.RABBITMQ_LIGHT_QUEUE)
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
        logger.info("=====================")
