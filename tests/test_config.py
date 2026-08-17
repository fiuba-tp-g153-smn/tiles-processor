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
            "sources": {
                "goes19": {
                    "products": {"band_13": True, "band_9": False},
                },
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

    def test_concurrency_knobs_default(self, temp_settings_file, env_vars):
        """WORKER_CONCURRENCY/S3_UPLOAD_CONCURRENCY fall back to their defaults."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            assert config.WORKER_CONCURRENCY == 2
            assert config.S3_UPLOAD_CONCURRENCY == 32

    def test_concurrency_empty_strings_use_defaults(self, temp_settings_file, env_vars):
        """Compose-supplied empty strings normalize to the defaults (not int('')) ."""
        env = {**env_vars, "WORKER_CONCURRENCY": "", "S3_UPLOAD_CONCURRENCY": ""}
        with mock.patch.dict(os.environ, env, clear=True):
            config = Config(settings_path=temp_settings_file)
            assert config.WORKER_CONCURRENCY == 2
            assert config.S3_UPLOAD_CONCURRENCY == 32

    def test_worker_concurrency_rejects_below_one(self, temp_settings_file, env_vars):
        """WORKER_CONCURRENCY < 1 fails fast."""
        env = {**env_vars, "WORKER_CONCURRENCY": "0"}
        with mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="WORKER_CONCURRENCY"):
                Config(settings_path=temp_settings_file)

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

    def test_input_sources_default_to_local(self, temp_settings_file, env_vars):
        """Radar/GLM/WRF default to local mode; GOES-19 defaults to NOAA S3."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            assert config.RADAR_INPUT.mode == "local"
            assert config.RADAR_INPUT.input_dir == "/tmp/test/radar_h5"
            assert config.GLM_FOLDER_INPUT.mode == "local"
            assert config.WRF_INPUT.mode == "local"
            assert config.GOES19_INPUT.mode == "s3"
            assert config.GOES19_INPUT.s3_bucket == "noaa-goes19"
            assert config.GOES19_INPUT.s3_endpoint is None
            assert config.GOES19_INPUT.s3_access_key is None

    def test_input_source_dir_aliases_match(self, temp_settings_file, env_vars):
        """Legacy *_INPUT_DIR attributes alias the InputSourceConfig values."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)

            assert config.RADAR_INPUT_DIR == config.RADAR_INPUT.input_dir
            assert config.GLM_FOLDER_INPUT_DIR == config.GLM_FOLDER_INPUT.input_dir
            assert config.WRF_INPUT_DIR == config.WRF_INPUT.input_dir

    def test_input_source_s3_mode_from_settings(self, tmp_path, env_vars):
        """S3 mode settings are parsed per source from sources.<name>.input."""
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
            "sources": {
                "radar": {
                    "input": {
                        "mode": "s3",
                        "s3_bucket": "radar-input",
                        "s3_endpoint": "seaweedfs:8333",
                        "s3_prefix": "radar_h5/",
                        "s3_secure": True,
                    }
                }
            },
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=settings_path)

            assert config.RADAR_INPUT.is_s3
            assert config.RADAR_INPUT.s3_bucket == "radar-input"
            assert config.RADAR_INPUT.s3_endpoint == "seaweedfs:8333"
            assert config.RADAR_INPUT.s3_prefix == "radar_h5/"
            assert config.RADAR_INPUT.s3_secure is True
            assert config.GLM_FOLDER_INPUT.mode == "local"

    def test_input_source_credentials_from_env(self, tmp_path, env_vars):
        """Per-source S3 credentials come from {ENV_PREFIX}_S3_* env vars."""
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
            "sources": {
                "wrf": {"input": {"mode": "s3", "s3_bucket": "wrf-input"}},
            },
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))
        env = {**env_vars, "WRF_S3_ACCESS_KEY": "ak", "WRF_S3_SECRET_KEY": "sk"}

        with mock.patch.dict(os.environ, env, clear=True):
            config = Config(settings_path=settings_path)

            assert config.WRF_INPUT.s3_access_key == "ak"
            assert config.WRF_INPUT.s3_secret_key == "sk"

    def test_input_source_rejects_invalid_mode(self, tmp_path, env_vars):
        """An unknown input mode fails fast, naming the JSON path."""
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
            "sources": {"radar": {"input": {"mode": "ftp"}}},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="sources.radar.input.mode"):
                Config(settings_path=settings_path)

    def test_input_source_s3_mode_requires_bucket(self, tmp_path, env_vars):
        """Mode s3 without a bucket fails fast."""
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
            "sources": {"glm": {"input": {"mode": "s3"}}},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="s3_bucket"):
                Config(settings_path=settings_path)

    @staticmethod
    def _settings_with_bounds(tmp_path, bounds):
        """Write a minimal settings.json with the given bounds and return its path."""
        settings = {"timezone": "UTC", "bounds": bounds}
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))
        return settings_path

    def test_rejects_inverted_longitude_bounds(self, tmp_path, env_vars):
        """minx >= maxx fails fast, naming the field."""
        path = self._settings_with_bounds(
            tmp_path, {"minx": -30, "miny": -60, "maxx": -90, "maxy": -15}
        )
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="BOUNDS_MINX"):
                Config(settings_path=path)

    def test_rejects_inverted_latitude_bounds(self, tmp_path, env_vars):
        """miny >= maxy fails fast, naming the field."""
        path = self._settings_with_bounds(
            tmp_path, {"minx": -90, "miny": -15, "maxx": -30, "maxy": -60}
        )
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="BOUNDS_MINY"):
                Config(settings_path=path)

    def test_rejects_out_of_range_latitude(self, tmp_path, env_vars):
        """A latitude outside [-90, 90] fails fast."""
        path = self._settings_with_bounds(
            tmp_path, {"minx": -90, "miny": -60, "maxx": -30, "maxy": 200}
        )
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="BOUNDS_MAXY"):
                Config(settings_path=path)

    def test_rejects_out_of_range_longitude(self, tmp_path, env_vars):
        """A longitude outside [-180, 180] fails fast."""
        path = self._settings_with_bounds(
            tmp_path, {"minx": -500, "miny": -60, "maxx": -30, "maxy": -15}
        )
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="BOUNDS_MINX"):
                Config(settings_path=path)

    def test_accepts_valid_bounds(self, tmp_path, env_vars):
        """A well-formed box constructs cleanly."""
        path = self._settings_with_bounds(
            tmp_path, {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15}
        )
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=path)
            assert config.BOUNDS_MINX == -90.0
            assert config.BOUNDS_MAXY == -15.0

    def test_input_source_rejects_half_set_credentials(
        self, temp_settings_file, env_vars
    ):
        """Only one of access/secret key set fails fast instead of going anonymous."""
        env = {**env_vars, "RADAR_S3_ACCESS_KEY": "ak"}
        with mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="RADAR_S3_ACCESS_KEY"):
                Config(settings_path=temp_settings_file)

    def test_input_source_empty_env_credentials_are_anonymous(
        self, temp_settings_file, env_vars
    ):
        """Compose-supplied empty credential strings normalize to anonymous."""
        env = {**env_vars, "GOES19_S3_ACCESS_KEY": "", "GOES19_S3_SECRET_KEY": ""}
        with mock.patch.dict(os.environ, env, clear=True):
            config = Config(settings_path=temp_settings_file)
            assert config.GOES19_INPUT.s3_access_key is None
            assert config.GOES19_INPUT.s3_secret_key is None

    def test_ecmwf_mslp_settings_loaded_from_file(self, tmp_path, env_vars):
        """All three MSLP settings are read from settings.json when present."""
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
            "sources": {
                "ecmwf": {
                    "products": {"mean_sea_level_pressure": True},
                    "mslp": {
                        "isobar_simplify_tolerance": 0.5,
                        "smoothing_sigma": 2.5,
                    },
                }
            },
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=settings_path)
            assert config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE is True
            assert config.ECMWF_MSLP_ISOBAR_SIMPLIFY_TOLERANCE == 0.5
            assert config.ECMWF_MSLP_SMOOTHING_SIGMA == 2.5

    @staticmethod
    def _settings_with_radar_stations(tmp_path, radar_stations):
        """Write a minimal settings.json carrying sources.radar.stations."""
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        }
        if radar_stations is not None:
            settings["sources"] = {"radar": {"stations": radar_stations}}
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))
        return settings_path

    def test_radar_stations_absent_defaults_to_all(self, temp_settings_file, env_vars):
        """No radar_stations key -> an "all" filter (every station enabled)."""
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=temp_settings_file)
            assert config.RADAR_STATION_FILTER.mode == "all"
            assert config.RADAR_STATION_FILTER.allows("RMA7") is True

    def test_radar_stations_string_none_disables_all(self, tmp_path, env_vars):
        """The "none" shorthand disables every station."""
        path = self._settings_with_radar_stations(tmp_path, "none")
        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = Config(settings_path=path)
            assert config.RADAR_STATION_FILTER.mode == "none"
            assert config.RADAR_STATION_FILTER.allows("RMA1") is False

    def test_radar_stations_whitelist(self, tmp_path, env_vars):
        """A whitelist enables only the listed stations."""
        path = self._settings_with_radar_stations(
            tmp_path, {"whitelist": ["RMA1", "RMA3"]}
        )
        with mock.patch.dict(os.environ, env_vars, clear=True):
            rf = Config(settings_path=path).RADAR_STATION_FILTER
            assert rf.mode == "whitelist"
            assert rf.stations == frozenset({"RMA1", "RMA3"})
            assert rf.allows("RMA1") and not rf.allows("RMA2")

    def test_radar_stations_blacklist(self, tmp_path, env_vars):
        """A blacklist enables every station except the listed ones."""
        path = self._settings_with_radar_stations(tmp_path, {"blacklist": ["RMA3"]})
        with mock.patch.dict(os.environ, env_vars, clear=True):
            rf = Config(settings_path=path).RADAR_STATION_FILTER
            assert rf.mode == "blacklist"
            assert not rf.allows("RMA3") and rf.allows("RMA1")

    def test_tile_lifecycle_retention_resolves_from_sources(self, tmp_path, env_vars):
        """Per-source retention_days resolves into a concrete {prefix: days} map."""
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
            "sources": {
                "radar": {"retention_days": 3},
                "ecmwf": {"retention_days": {"default": 2, "grib": 1}},
            },
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))
        with mock.patch.dict(os.environ, env_vars, clear=True):
            retention = Config(settings_path=settings_path).TILE_LIFECYCLE_RETENTION
            assert retention["tiles/radar"] == 3
            assert retention["cog/radar"] == 3
            assert retention["grib/models/ecmwf"] == 1
            assert retention["tiles/models/ecmwf"] == 2
            # A source with no retention_days falls back to the default (1 day).
            assert retention["tiles/models/gfs"] == 1

    def test_radar_stations_invalid_shape_fails_fast(self, tmp_path, env_vars):
        """An object with both whitelist and blacklist is rejected at startup."""
        path = self._settings_with_radar_stations(
            tmp_path, {"whitelist": ["RMA1"], "blacklist": ["RMA2"]}
        )
        with mock.patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError, match="radar_stations"):
                Config(settings_path=path)
