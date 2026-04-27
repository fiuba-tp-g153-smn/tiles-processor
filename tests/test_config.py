import json
import os
import pytest
from pathlib import Path
from unittest import mock
import sys

# Ensure src is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from config import Config


class TestConfig:
    """Tests for Config class initialization."""

    @pytest.fixture
    def temp_settings_file(self, tmp_path):
        """Create a temporary settings.json file."""
        settings = {
            "timezone": "UTC",
            "scheduler": {
                "band_13_cron": "*/10 * * * *",
                "band_9_cron": "0 * * * *",
            },
            "features": {
                "enable_band_13": True,
                "enable_band_9": False,
            },
            "bounds": {
                "minx": -90.0,
                "miny": -60.0,
                "maxx": -30.0,
                "maxy": -15.0,
            },
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))
        return settings_path

    @pytest.fixture
    def env_vars(self):
        """Required environment variables for Config."""
        return {
            "LOG_LEVEL": "DEBUG",
            "DATA_DIR": "/tmp/test",
            "S3_TILES_DATA_ENDPOINT": "s3-service:9000",
            "S3_TILES_DATA_TILES_PROCESSOR_USER": "s3admin",
            "S3_TILES_DATA_TILES_PROCESSOR_PASSWORD": "s3admin",
            "S3_TILES_DATA_BUCKET_NAME": "tiles-data",
            "RABBITMQ_HOST": "rabbitmq",
            "RABBITMQ_PORT": "5672",
            "RABBITMQ_USER": "guest",
            "RABBITMQ_PASSWORD": "guest",
            "RABBITMQ_QUEUE": "tiles_queue",
            "RABBITMQ_DLQ": "tiles_dlq",
            "RABBITMQ_DLX": "tiles_dlx",
            "JOB_TTL_MINUTES": "20",
        }

    def test_config_loads_from_settings_file(self, temp_settings_file, env_vars):
        """Test that Config loads settings from JSON file."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            assert config.TIMEZONE == "UTC"
            assert config.TIMEZONE == "UTC"
            assert config.ENABLE_BAND_13 is True
            assert config.ENABLE_BAND_9 is False
            assert config.BOUNDS_MINX == -90.0
            assert config.BOUNDS_MINY == -60.0
            assert config.BOUNDS_MAXX == -30.0
            assert config.BOUNDS_MAXY == -15.0

    def test_config_loads_env_vars(self, temp_settings_file, env_vars):
        """Test that Config loads environment variables."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            assert config.LOG_LEVEL == "DEBUG"
            assert config.TMP_DIR == "/tmp/test/tmp"
            assert config.TMP_DIR == "/tmp/test/tmp"

    def test_config_raises_on_missing_env_var(self, temp_settings_file):
        """Test that Config raises ValueError when required env var is missing."""
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="LOG_LEVEL.*is required"):
                Config(settings_path=temp_settings_file)

    def test_config_raises_on_empty_env_var(self, temp_settings_file):
        """Test that Config raises ValueError when env var is empty."""
        env_vars = {
            "LOG_LEVEL": "",
            "DATA_DIR": "/tmp/test",
        }
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="LOG_LEVEL.*is required"):
                Config(settings_path=temp_settings_file)

    def test_config_raises_on_missing_settings_file(self, tmp_path, env_vars):
        """Test that Config raises FileNotFoundError when settings file is missing."""
        missing_path = tmp_path / "nonexistent.json"
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(FileNotFoundError, match="Settings file not found"):
                Config(settings_path=missing_path)

    def test_config_seaweedfs_radar_tile_ttl_from_env(
        self, temp_settings_file, env_vars
    ):
        """SEAWEEDFS_RADAR_TILE_TTL is read from the environment variable."""
        with mock.patch.dict(
            os.environ, {**env_vars, "SEAWEEDFS_RADAR_TILE_TTL": "168h"}, clear=True
        ):
            config = Config(settings_path=temp_settings_file)
            assert config.SEAWEEDFS_RADAR_TILE_TTL == "168h"

    def test_config_seaweedfs_radar_tile_ttl_defaults_to_none(
        self, temp_settings_file, env_vars
    ):
        """SEAWEEDFS_RADAR_TILE_TTL is None when the env var is not set."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            assert config.SEAWEEDFS_RADAR_TILE_TTL is None

    def test_get_bounds_returns_dict(self, temp_settings_file, env_vars):
        """Test that get_bounds returns correct dictionary."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            bounds = config.get_bounds()

            assert bounds == {
                "minx": -90.0,
                "miny": -60.0,
                "maxx": -30.0,
                "maxy": -15.0,
            }

    def test_ecmwf_mslp_settings_default_when_absent(
        self, temp_settings_file, env_vars
    ):
        """MSLP toggles use safe defaults when not present in settings.json."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            assert config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE is False
            assert config.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE == 0.1
            assert config.ECMWF_MSLP_SMOOTHING_SIGMA == 1.5

    def test_ecmwf_mslp_settings_loaded_from_file(self, tmp_path, env_vars):
        """All three MSLP settings are read from settings.json when present."""
        settings = {
            "timezone": "UTC",
            "ecmwf_mslp_isobar_simplify_tolerance": 0.5,
            "ecmwf_mslp_smoothing_sigma": 2.5,
            "features": {
                "enable_ecmwf_mean_sea_level_pressure": True,
            },
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=settings_path)
            assert config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE is True
            assert config.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE == 0.5
            assert config.ECMWF_MSLP_SMOOTHING_SIGMA == 2.5
