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
    
    # Paths
    TMP_DIR: str = os.getenv("TMP_DIR_CONTAINER", ".tmp")

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
        logger.info(f"TMP_DIR: {cls.TMP_DIR}")
        logger.info("=====================")

config = Config()
