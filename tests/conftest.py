import os
import pytest

# Set default environment variables for tests
# These are set before any other imports to ensure config.py picks them up
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("TZ", "UTC")
os.environ.setdefault("BAND_13_SCHEDULE_CRON", "*/10 * * * *")
os.environ.setdefault("BAND_9_SCHEDULE_CRON", "*/10 * * * *")
os.environ.setdefault("ENABLE_BAND_13", "true")
os.environ.setdefault("ENABLE_BAND_9", "true")
os.environ.setdefault("TMP_DIR_HOST", "./.tmp")
os.environ.setdefault("TMP_DIR_CONTAINER", "/app/.tmp")
os.environ.setdefault("BOUNDS_MINX", "-90.0")
os.environ.setdefault("BOUNDS_MINY", "-60.0")
os.environ.setdefault("BOUNDS_MAXX", "-30.0")
os.environ.setdefault("BOUNDS_MAXY", "-15.0")
os.environ.setdefault("SCHEDULER_DB_PATH", "/tmp/test_scheduler.db")
# Also set TMP_DIR for local tests (simulating container behavior or just providing a path)
os.environ.setdefault("TMP_DIR", ".tmp")

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Ensure these vars are present for all tests, 
    though the global setdefault above handles import-time config.
    """
    pass
