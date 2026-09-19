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


@pytest.fixture
def rainbow_header():
    """Factory for a synthetic Rainbow5 (.vol) preamble.

    ``<radarinfo>`` is pushed past the 16 KB mark on purpose: in the real INTA
    files it sits ~18.7 KB in, behind the <pargroup> scan-parameter block, so a
    fixture that put it up front would not exercise the read window at all.
    """

    def build(
        radar_id: str = "PAR",
        name: str = "INTA_Parana",
        lat: str = "-31.848438",
        lon: str = "-60.537289",
        alt: str = "100.000000",
        stop_range: str = "240",
        numele: str = "12",
        scan_name: str = "VOL_240_ALL.vol",
        padding: int = 18_000,
    ) -> bytes:
        prologue = f'<volume version="5.22.7"><scan name="{scan_name}"><pargroup>'
        pargroup = "<pad>x</pad>" * (padding // 12)
        tail = (
            f"</pargroup><stoprange>{stop_range}</stoprange>"
            f"<numele>{numele}</numele>"
            f'<radarinfo alt="{alt}" lon="{lon}" lat="{lat}" id="{radar_id}" >'
            f"<name>{name}</name><wavelen>0.0532</wavelen></radarinfo>"
            "</scan></volume>"
        )
        return (prologue + pargroup + tail).encode()

    return build
