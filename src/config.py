import os
from typing import Dict

def get_required_env(key: str) -> str:
    value = os.getenv(key)
    if not value or not value.strip():
        raise ValueError(f"Environment variable '{key}' is required but not set or empty.")
    return value

class Config:
    # General
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Timezone (defaults to UTC)
    # Examples: "UTC", "America/New_York", "Europe/London", "Asia/Tokyo", "America/Argentina/Buenos_Aires"
    TIMEZONE: str = os.getenv("TZ", "UTC")
    
    # Scheduler
    # Format: Full cron expression (e.g. "*/10 * * * *")
    # Examples:
    #   "*/10 * * * *"  -> Every 10 minutes
    #   "0 9 * * *"     -> Every day at 09:00 UTC
    #   "0 0 * * 1"     -> Every Monday at 00:00 UTC
    #   "30 18 * * 5"   -> Every Friday at 18:30 UTC
    #   "0 0 1,15 * *"  -> On the 1st and 15th of every month at 00:00 UTC
    BAND_13_SCHEDULE_CRON: str = get_required_env("BAND_13_SCHEDULE_CRON")
    BAND_9_SCHEDULE_CRON: str = get_required_env("BAND_9_SCHEDULE_CRON")
    
    # Feature Toggles
    # Default to True to maintain backward compatibility if env vars are missing
    ENABLE_BAND_13: bool = os.getenv("ENABLE_BAND_13", "true").lower() in ("true", "1")
    ENABLE_BAND_9: bool = os.getenv("ENABLE_BAND_9", "true").lower() in ("true", "1")
    
    # Paths
    TMP_DIR: str = os.getenv("TMP_DIR_CONTAINER", ".tmp")
    MAX_TMP_DIR_SIZE_BYTES: int = 10 * 1024 * 1024 * 1024  # 10 GB

    # Bounding box for clipping satellite imagery (defaults to Argentina region)
    # Coordinates are in EPSG:4326 (longitude/latitude)
    BOUNDS_MINX: float = float(os.getenv("BOUNDS_MINX", "-90.0"))   # West longitude
    BOUNDS_MINY: float = float(os.getenv("BOUNDS_MINY", "-60.0"))   # South latitude
    BOUNDS_MAXX: float = float(os.getenv("BOUNDS_MAXX", "-30.0"))   # East longitude
    BOUNDS_MAXY: float = float(os.getenv("BOUNDS_MAXY", "-15.0"))   # North latitude

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
        logger.info(f"ENABLE_BAND_13: {cls.ENABLE_BAND_13}")
        logger.info(f"BAND_9_SCHEDULE_CRON: {cls.BAND_9_SCHEDULE_CRON}")
        logger.info(f"ENABLE_BAND_9: {cls.ENABLE_BAND_9}")
        logger.info(f"TMP_DIR: {cls.TMP_DIR}")
        logger.info("=====================")

config = Config()
