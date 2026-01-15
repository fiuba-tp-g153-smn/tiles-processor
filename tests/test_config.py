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
            "TMP_DIR_CONTAINER": "/tmp/test",
            "SCHEDULER_DB_PATH": "/tmp/test/scheduler.db",
        }

    def test_config_loads_from_settings_file(self, temp_settings_file, env_vars):
        """Test that Config loads settings from JSON file."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            assert config.TIMEZONE == "UTC"
            assert config.BAND_13_SCHEDULE_CRON == "*/10 * * * *"
            assert config.BAND_9_SCHEDULE_CRON == "0 * * * *"
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
            assert config.TMP_DIR == "/tmp/test"
            assert config.SCHEDULER_DB_PATH == "/tmp/test/scheduler.db"

    def test_config_raises_on_missing_env_var(self, temp_settings_file):
        """Test that Config raises ValueError when required env var is missing."""
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="LOG_LEVEL.*is required"):
                Config(settings_path=temp_settings_file)

    def test_config_raises_on_empty_env_var(self, temp_settings_file):
        """Test that Config raises ValueError when env var is empty."""
        env_vars = {
            "LOG_LEVEL": "",
            "TMP_DIR_CONTAINER": "/tmp/test",
            "SCHEDULER_DB_PATH": "/tmp/test/scheduler.db",
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

    def test_get_job_schedules_returns_dict(self, temp_settings_file, env_vars):
        """Test that get_job_schedules returns correct dictionary."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            schedules = config.get_job_schedules()

            assert schedules == {
                "process_band_13": "*/10 * * * *",
                "process_band_9": "0 * * * *",
            }


class TestValidateCronExpression:
    """Tests for CRON expression validation."""

    @pytest.fixture
    def temp_settings_file(self, tmp_path):
        """Create a temporary settings.json with valid cron."""
        settings = {
            "timezone": "UTC",
            "scheduler": {"band_13_cron": "*/10 * * * *", "band_9_cron": "0 * * * *"},
            "features": {"enable_band_13": True, "enable_band_9": True},
            "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))
        return settings_path

    @pytest.fixture
    def env_vars(self):
        return {
            "LOG_LEVEL": "INFO",
            "TMP_DIR_CONTAINER": "/tmp/test",
            "SCHEDULER_DB_PATH": "/tmp/test/scheduler.db",
        }

    def test_valid_every_10_minutes(self, temp_settings_file, env_vars):
        """Test valid expression: every 10 minutes."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            assert config.BAND_13_SCHEDULE_CRON == "*/10 * * * *"

    def test_valid_daily_at_9am(self, tmp_path, env_vars):
        """Test valid expression: daily at 9:00."""
        settings = {
            "timezone": "UTC",
            "scheduler": {"band_13_cron": "0 9 * * *", "band_9_cron": "0 9 * * *"},
            "features": {"enable_band_13": True, "enable_band_9": True},
            "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=settings_path)
            assert config.BAND_13_SCHEDULE_CRON == "0 9 * * *"

    def test_valid_weekly_monday(self, tmp_path, env_vars):
        """Test valid expression: every Monday at midnight."""
        settings = {
            "timezone": "UTC",
            "scheduler": {"band_13_cron": "0 0 * * 1", "band_9_cron": "0 0 * * 1"},
            "features": {"enable_band_13": True, "enable_band_9": True},
            "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=settings_path)
            assert config.BAND_13_SCHEDULE_CRON == "0 0 * * 1"

    def test_invalid_cron_raises_error(self, tmp_path, env_vars):
        """Test that invalid CRON expression raises ValueError."""
        settings = {
            "timezone": "UTC",
            "scheduler": {"band_13_cron": "invalid", "band_9_cron": "0 * * * *"},
            "features": {"enable_band_13": True, "enable_band_9": True},
            "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="Invalid CRON expression"):
                Config(settings_path=settings_path)

    def test_invalid_too_few_fields(self, tmp_path, env_vars):
        """Test that expression with too few fields raises ValueError."""
        settings = {
            "timezone": "UTC",
            "scheduler": {"band_13_cron": "* * *", "band_9_cron": "0 * * * *"},
            "features": {"enable_band_13": True, "enable_band_9": True},
            "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="Invalid CRON expression"):
                Config(settings_path=settings_path)

    def test_invalid_out_of_range_minute(self, tmp_path, env_vars):
        """Test that invalid minute value raises ValueError."""
        settings = {
            "timezone": "UTC",
            "scheduler": {"band_13_cron": "60 * * * *", "band_9_cron": "0 * * * *"},
            "features": {"enable_band_13": True, "enable_band_9": True},
            "bounds": {"minx": -90.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="Invalid CRON expression"):
                Config(settings_path=settings_path)
