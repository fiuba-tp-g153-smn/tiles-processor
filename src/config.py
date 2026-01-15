import os
from typing import Dict

from apscheduler.triggers.cron import CronTrigger


def get_required_env(key: str) -> str:
    """Get a required environment variable, raising if not set."""
    value = os.getenv(key)
    if not value or not value.strip():
        raise ValueError(
            f"Environment variable '{key}' is required but not set or empty."
        )
    return value


def validate_cron_expression(expr: str, name: str) -> str:
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


class Config:
    # General
    LOG_LEVEL: str = get_required_env("LOG_LEVEL").upper()

    # Timezone
    # Examples: "UTC", "America/New_York", "Europe/London", "Asia/Tokyo", "America/Argentina/Buenos_Aires"
    TIMEZONE: str = get_required_env("TZ")

    # Scheduler
    # Format: Full cron expression (e.g. "*/10 * * * *")
    # Examples:
    #   "*/10 * * * *"  -> Every 10 minutes
    #   "0 9 * * *"     -> Every day at 09:00 UTC
    #   "0 0 * * 1"     -> Every Monday at 00:00 UTC
    #   "30 18 * * 5"   -> Every Friday at 18:30 UTC
    #   "0 0 1,15 * *"  -> On the 1st and 15th of every month at 00:00 UTC
    BAND_13_SCHEDULE_CRON: str = validate_cron_expression(
        get_required_env("BAND_13_SCHEDULE_CRON"), "BAND_13_SCHEDULE_CRON"
    )
    BAND_9_SCHEDULE_CRON: str = validate_cron_expression(
        get_required_env("BAND_9_SCHEDULE_CRON"), "BAND_9_SCHEDULE_CRON"
    )

    # Feature Toggles
    ENABLE_BAND_13: bool = get_required_env("ENABLE_BAND_13").lower() in ("true", "1")
    ENABLE_BAND_9: bool = get_required_env("ENABLE_BAND_9").lower() in ("true", "1")

    # Paths
    TMP_DIR: str = get_required_env("TMP_DIR_CONTAINER")
    MAX_TMP_DIR_SIZE_BYTES: int = 10 * 1024 * 1024 * 1024  # 10 GB

    # Scheduler persistence
    # Path to SQLite database for APScheduler job persistence
    # Jobs survive container restarts when stored in a mounted volume
    SCHEDULER_DB_PATH: str = get_required_env("SCHEDULER_DB_PATH")

    # Bounding box for clipping satellite imagery
    # Coordinates are in EPSG:4326 (longitude/latitude)
    BOUNDS_MINX: float = float(get_required_env("BOUNDS_MINX"))  # West longitude
    BOUNDS_MINY: float = float(get_required_env("BOUNDS_MINY"))  # South latitude
    BOUNDS_MAXX: float = float(get_required_env("BOUNDS_MAXX"))  # East longitude
    BOUNDS_MAXY: float = float(get_required_env("BOUNDS_MAXY"))  # North latitude

    @classmethod
    def get_bounds(cls) -> Dict[str, float]:
        """Get the bounding box configuration for clipping."""
        return {
            "minx": cls.BOUNDS_MINX,
            "miny": cls.BOUNDS_MINY,
            "maxx": cls.BOUNDS_MAXX,
            "maxy": cls.BOUNDS_MAXY,
        }

    @classmethod
    def get_job_schedules(cls) -> Dict[str, str]:
        return {
            "process_band_13": cls.BAND_13_SCHEDULE_CRON,
            "process_band_9": cls.BAND_9_SCHEDULE_CRON,
        }

    @classmethod
    def log_config(cls):
        import logging

        logger = logging.getLogger(__name__)
        logger.info("=== Configuration ===")
        logger.info(f"LOG_LEVEL: {cls.LOG_LEVEL}")
        logger.info(f"TIMEZONE: {cls.TIMEZONE}")
        logger.info(f"BAND_13_SCHEDULE_CRON: {cls.BAND_13_SCHEDULE_CRON}")
        logger.info(f"BAND_9_SCHEDULE_CRON: {cls.BAND_9_SCHEDULE_CRON}")
        logger.info(f"ENABLE_BAND_13: {cls.ENABLE_BAND_13}")
        logger.info(f"ENABLE_BAND_9: {cls.ENABLE_BAND_9}")
        logger.info(f"TMP_DIR: {cls.TMP_DIR}")
        logger.info(f"MAX_TMP_DIR_SIZE_BYTES: {cls.MAX_TMP_DIR_SIZE_BYTES}")
        logger.info(f"SCHEDULER_DB_PATH: {cls.SCHEDULER_DB_PATH}")
        logger.info(f"BOUNDS_MINX: {cls.BOUNDS_MINX}")
        logger.info(f"BOUNDS_MINY: {cls.BOUNDS_MINY}")
        logger.info(f"BOUNDS_MAXX: {cls.BOUNDS_MAXX}")
        logger.info(f"BOUNDS_MAXY: {cls.BOUNDS_MAXY}")
        logger.info("=====================")


config = Config()
