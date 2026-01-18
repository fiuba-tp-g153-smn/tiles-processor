import os
import pytest

# Set default environment variables for tests
# These are set before any other imports to ensure config.py picks them up
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("DATA_DIR", "/app/data")
os.environ.setdefault("S3_TILES_DATA_ENDPOINT", "minio:9000")
os.environ.setdefault("S3_TILES_DATA_TILES_PROCESSOR_USER", "minioadmin")
os.environ.setdefault("S3_TILES_DATA_TILES_PROCESSOR_PASSWORD", "minioadmin")
os.environ.setdefault("BAND_13_SCHEDULE_CRON", "*/30 * * * *")
os.environ.setdefault("BAND_9_SCHEDULE_CRON", "*/30 * * * *")


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Ensure these vars are present for all tests,
    though the global setdefault above handles import-time config.
    """
    pass
