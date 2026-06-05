import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

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


@pytest.fixture
def migrated_dbs(tmp_path):
    """Apply migrations to temp metrics + progress DBs and return their paths.

    Schema is owned by Alembic now (the repositories no longer self-create
    tables), so DB tests migrate first via the same ``run_migrations`` helper the
    ``migrate`` entrypoint uses.
    """
    # Imported here so the heavy Alembic/SQLAlchemy import is only paid by the
    # tests that actually touch a database.
    from db.migrate import run_migrations  # pylint: disable=import-outside-toplevel

    metrics = tmp_path / "metrics.db"
    progress = tmp_path / "progress_tracker.db"
    run_migrations(metrics, progress)
    return SimpleNamespace(metrics=metrics, progress=progress)
