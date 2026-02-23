import os
import pytest

# Set default environment variables for tests
# These are set before any other imports to ensure config.py picks them up
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("DATA_DIR", "/app/data")
os.environ.setdefault("S3_TILES_DATA_ENDPOINT", "s3-service:9000")
os.environ.setdefault("S3_TILES_DATA_TILES_PROCESSOR_USER", "s3sadmin")
os.environ.setdefault("S3_TILES_DATA_TILES_PROCESSOR_PASSWORD", "s3admin")
os.environ.setdefault("S3_TILES_DATA_BUCKET_NAME", "tiles-data")
os.environ.setdefault("RABBITMQ_HOST", "rabbitmq")
os.environ.setdefault("RABBITMQ_PORT", "5672")
os.environ.setdefault("RABBITMQ_USER", "guest")
os.environ.setdefault("RABBITMQ_PASSWORD", "guest")
os.environ.setdefault("RABBITMQ_QUEUE", "tiles_queue")
os.environ.setdefault("RABBITMQ_DLQ", "tiles_dlq")
os.environ.setdefault("RABBITMQ_DLX", "tiles_dlx")
os.environ.setdefault("JOB_TTL_MINUTES", "20")


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Ensure these vars are present for all tests,
    though the global setdefault above handles import-time config.
    """
    pass
