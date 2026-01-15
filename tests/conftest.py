import os
import pytest

# Set default environment variables for tests
# These are set before any other imports to ensure config.py picks them up
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("DATA_DIR_CONTAINER", "/app/data")


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Ensure these vars are present for all tests,
    though the global setdefault above handles import-time config.
    """
    pass
