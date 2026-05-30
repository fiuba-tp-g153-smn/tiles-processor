import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from config import Config
from factories import create_data_source_registry, create_s3_client
from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG


@pytest.fixture
def config_with_filer(monkeypatch):
    monkeypatch.setenv("SEAWEEDFS_FILER_ENDPOINT", "http://filer:8888")
    monkeypatch.setenv("SEAWEEDFS_TILE_TTL", "72h")
    return Config()


class TestCreateS3Client:
    def test_with_ttl_true_passes_configured_ttl(self, config_with_filer):
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer, with_ttl=True)
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] == config_with_filer.SEAWEEDFS_TILE_TTL

    def test_with_ttl_false_passes_none(self, config_with_filer):
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer, with_ttl=False)
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] is None

    def test_default_with_ttl_is_true(self, config_with_filer):
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer)
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] == config_with_filer.SEAWEEDFS_TILE_TTL

    def test_explicit_ttl_string_passes_through(self, config_with_filer):
        """An explicit TTL string (e.g. from SEAWEEDFS_RADAR_TILE_TTL) is forwarded as-is."""
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer, with_ttl="168h")
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] == "168h"

    def test_with_ttl_none_passes_none(self, config_with_filer):
        """None disables TTL — used when SEAWEEDFS_RADAR_TILE_TTL is unset."""
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer, with_ttl=None)
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] is None


class TestCreateDataSourceRegistry:
    def _build_config(self, *, tp: bool, mslp: bool) -> MagicMock:
        config = MagicMock(spec=Config)
        config.ENABLE_BAND_13 = False
        config.ENABLE_BAND_9 = False
        config.ENABLE_BAND_2 = False
        config.ENABLE_GLM_FED = False
        config.ENABLE_GLM_TOE = False
        config.ENABLE_GLM_MFA = False
        config.ENABLED_RADAR_PRODUCTS = {}
        config.ENABLE_ECMWF_PRECIPITATION = tp
        config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE = mslp
        config.RADAR_INPUT_DIR = "/tmp/radar"
        config.GLM_FOLDER_INPUT_DIR = "/tmp/glm"
        config.GLM_ACCUM_MINUTES = 10
        config.GLM_PRODUCE_EVERY_MINUTES = 10
        config.WRF_INPUT_DIR = "/tmp/wrf"
        config.ENABLED_WRF_PRODUCTS = {}
        return config

    def test_mslp_data_sources_registered_when_enabled(self):
        config = self._build_config(tp=False, mslp=True)
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        ids = {ds.source_id for ds in registry.get_all()}
        assert ECMWF_MSLP_CONFIG.producer_data_source_id in ids
        assert ECMWF_MSLP_CONFIG.period_data_source_id in ids

    def test_mslp_data_sources_skipped_when_disabled(self):
        config = self._build_config(tp=False, mslp=False)
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        ids = {ds.source_id for ds in registry.get_all()}
        assert ECMWF_MSLP_CONFIG.producer_data_source_id not in ids
        assert ECMWF_MSLP_CONFIG.period_data_source_id not in ids

    def test_tp_and_mslp_coexist_with_distinct_ids(self):
        config = self._build_config(tp=True, mslp=True)
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        ids = {ds.source_id for ds in registry.get_all()}
        assert {
            ECMWF_TP_CONFIG.producer_data_source_id,
            ECMWF_TP_CONFIG.period_data_source_id,
            ECMWF_MSLP_CONFIG.producer_data_source_id,
            ECMWF_MSLP_CONFIG.period_data_source_id,
        }.issubset(ids)
