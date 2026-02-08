"""Application configuration loaded from environment variables and settings.json."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict


class Config:  # pylint: disable=too-many-instance-attributes,invalid-name
    """Application configuration from environment variables and settings.json.

    Attributes use UPPER_CASE to match their environment variable names,
    following the convention used by Django, Flask, and other Python frameworks.
    """

    def __init__(self, settings_path: Path | None = None):
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

        # Settings from JSON
        self.TIMEZONE: str = settings["timezone"]

        # Feature Toggles (from JSON)
        self.ENABLE_BAND_13: bool = settings["features"].get("enable_band_13", True)
        self.ENABLE_BAND_9: bool = settings["features"].get("enable_band_9", True)
        self.ENABLE_BAND_2: bool = settings["features"].get("enable_band_2", False)
        self.ENABLE_RADAR: bool = settings["features"].get("enable_radar", True)

        # Job Configuration
        self.JOB_TTL_MINUTES: int = int(self._get_required_env("JOB_TTL_MINUTES"))

        # Health Check
        self.HEALTH_PORT: int = int(os.getenv("HEALTH_PORT", "8080"))

        # Bounding box (from JSON)
        # Coordinates are in EPSG:4326 (longitude/latitude)
        self.BOUNDS_MINX: float = settings["bounds"]["minx"]  # West longitude
        self.BOUNDS_MINY: float = settings["bounds"]["miny"]  # South latitude
        self.BOUNDS_MAXX: float = settings["bounds"]["maxx"]  # East longitude
        self.BOUNDS_MAXY: float = settings["bounds"]["maxy"]  # North latitude

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

    def log_config(self) -> None:
        """Log the current configuration values."""
        logger = logging.getLogger(__name__)
        logger.info("=== Configuration ===")
        logger.info("LOG_LEVEL: %s", self.LOG_LEVEL)
        logger.info("TIMEZONE: %s", self.TIMEZONE)
        logger.info("ENABLE_BAND_13: %s", self.ENABLE_BAND_13)
        logger.info("ENABLE_BAND_9: %s", self.ENABLE_BAND_9)
        logger.info("ENABLE_BAND_2: %s", self.ENABLE_BAND_2)
        logger.info("ENABLE_RADAR: %s", self.ENABLE_RADAR)
        logger.info("DATA_DIR: %s", self.DATA_DIR)
        logger.info("TMP_DIR: %s", self.TMP_DIR)
        logger.info("BOUNDS_MINX: %s", self.BOUNDS_MINX)
        logger.info("BOUNDS_MINY: %s", self.BOUNDS_MINY)
        logger.info("BOUNDS_MAXX: %s", self.BOUNDS_MAXX)
        logger.info("BOUNDS_MAXY: %s", self.BOUNDS_MAXY)
        logger.info("S3_TILES_DATA_ENDPOINT: %s", self.S3_TILES_DATA_ENDPOINT)
        logger.info("S3_TILES_DATA_BUCKET_NAME: %s", self.S3_TILES_DATA_BUCKET_NAME)
        logger.info("S3_TILES_DATA_SECURE: %s", self.S3_TILES_DATA_SECURE)
        logger.info("RABBITMQ_HOST: %s", self.RABBITMQ_HOST)
        logger.info("RABBITMQ_PORT: %s", self.RABBITMQ_PORT)
        logger.info("RABBITMQ_QUEUE: %s", self.RABBITMQ_QUEUE)
        logger.info("RABBITMQ_DLQ: %s", self.RABBITMQ_DLQ)
        logger.info("RABBITMQ_DLX: %s", self.RABBITMQ_DLX)
        logger.info("JOB_TTL_MINUTES: %s", self.JOB_TTL_MINUTES)
        logger.info("HEALTH_PORT: %s", self.HEALTH_PORT)
        logger.info("=====================")
