import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

from apscheduler.triggers.cron import CronTrigger


class Config:
    def __init__(self, settings_path: Path | None = None):
        if settings_path is None:
            settings_path = Path(__file__).parent.parent / "settings.json"

        settings = self._load_settings(settings_path)

        # Environment variables
        self.LOG_LEVEL: str = self._get_required_env("LOG_LEVEL").upper()
        data_dir = Path(self._get_required_env("DATA_DIR"))
        self.TMP_DIR: str = str(data_dir / "tmp")
        self.SCHEDULER_DB_PATH: str = str(data_dir / "scheduler" / "jobs.db")

        # S3 Configuration
        self.S3_TILES_DATA_ENDPOINT: str = self._get_required_env(
            "S3_TILES_DATA_ENDPOINT"
        )
        self.S3_TILES_DATA_ACCESS_KEY: str = self._get_required_env(
            "S3_TILES_DATA_ACCESS_KEY"
        )
        self.S3_TILES_DATA_SECRET_KEY: str = self._get_required_env(
            "S3_TILES_DATA_SECRET_KEY"
        )
        self.S3_TILES_DATA_BUCKET_NAME: str = os.getenv(
            "S3_TILES_DATA_BUCKET_NAME", "tiles-data"
        )
        self.S3_TILES_DATA_SECURE: bool = (
            os.getenv("S3_TILES_DATA_SECURE", "false").lower() == "true"
        )

        # Settings from JSON
        self.TIMEZONE: str = settings["timezone"]

        # Scheduler (from JSON, validated)
        self.BAND_13_SCHEDULE_CRON: str = self._validate_cron_expression(
            settings["scheduler"]["band_13_cron"], "scheduler.band_13_cron"
        )
        self.BAND_9_SCHEDULE_CRON: str = self._validate_cron_expression(
            settings["scheduler"]["band_9_cron"], "scheduler.band_9_cron"
        )
        # Default heartbeat schedule: every minute
        self.HEARTBEAT_SCHEDULE_CRON: str = "* * * * *"

        # Feature Toggles (from JSON)
        self.ENABLE_BAND_13: bool = settings["features"]["enable_band_13"]
        self.ENABLE_BAND_9: bool = settings["features"]["enable_band_9"]

        # Bounding box (from JSON)
        # Coordinates are in EPSG:4326 (longitude/latitude)
        self.BOUNDS_MINX: float = settings["bounds"]["minx"]  # West longitude
        self.BOUNDS_MINY: float = settings["bounds"]["miny"]  # South latitude
        self.BOUNDS_MAXX: float = settings["bounds"]["maxx"]  # East longitude
        self.BOUNDS_MAXY: float = settings["bounds"]["maxy"]  # North latitude

        # Constants
        self.MAX_TMP_DIR_SIZE_BYTES: int = 10 * 1024 * 1024 * 1024  # 10 GB

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

    @staticmethod
    def _validate_cron_expression(expr: str, name: str) -> str:
        """
        Validate a CRON expression at startup using APScheduler's CronTrigger.

        Args:
            expr: The CRON expression to validate (5-field format)
            name: The name of the config variable (for error messages)

        Returns:
            The validated expression if valid

        Raises:
            ValueError: If the expression is invalid
        """
        try:
            CronTrigger.from_crontab(expr)
            return expr
        except (ValueError, KeyError) as e:
            raise ValueError(
                f"Invalid CRON expression for {name}: '{expr}'. "
                f"Expected 5-field format (minute hour day month weekday). Error: {e}"
            )

    def get_bounds(self) -> Dict[str, float]:
        """Get the bounding box configuration for clipping."""
        return {
            "minx": self.BOUNDS_MINX,
            "miny": self.BOUNDS_MINY,
            "maxx": self.BOUNDS_MAXX,
            "maxy": self.BOUNDS_MAXY,
        }

    def get_job_schedules(self) -> Dict[str, str]:
        return {
            "process_band_13": self.BAND_13_SCHEDULE_CRON,
            "process_band_9": self.BAND_9_SCHEDULE_CRON,
            "heartbeat": self.HEARTBEAT_SCHEDULE_CRON,
        }

    def log_config(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info("=== Configuration ===")
        logger.info(f"LOG_LEVEL: {self.LOG_LEVEL}")
        logger.info(f"TIMEZONE: {self.TIMEZONE}")
        logger.info(f"BAND_13_SCHEDULE_CRON: {self.BAND_13_SCHEDULE_CRON}")
        logger.info(f"BAND_9_SCHEDULE_CRON: {self.BAND_9_SCHEDULE_CRON}")
        logger.info(f"ENABLE_BAND_13: {self.ENABLE_BAND_13}")
        logger.info(f"ENABLE_BAND_9: {self.ENABLE_BAND_9}")
        logger.info(f"TMP_DIR: {self.TMP_DIR}")
        logger.info(f"MAX_TMP_DIR_SIZE_BYTES: {self.MAX_TMP_DIR_SIZE_BYTES}")
        logger.info(f"SCHEDULER_DB_PATH: {self.SCHEDULER_DB_PATH}")
        logger.info(f"BOUNDS_MINX: {self.BOUNDS_MINX}")
        logger.info(f"BOUNDS_MINY: {self.BOUNDS_MINY}")
        logger.info(f"BOUNDS_MAXX: {self.BOUNDS_MAXX}")
        logger.info(f"BOUNDS_MAXY: {self.BOUNDS_MAXY}")
        logger.info(f"S3_TILES_DATA_ENDPOINT: {self.S3_TILES_DATA_ENDPOINT}")
        logger.info(f"S3_TILES_DATA_BUCKET_NAME: {self.S3_TILES_DATA_BUCKET_NAME}")
        logger.info(f"S3_TILES_DATA_SECURE: {self.S3_TILES_DATA_SECURE}")
        logger.info("=====================")
