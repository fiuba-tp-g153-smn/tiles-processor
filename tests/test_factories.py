import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from config import Config
from data_sources.glm_folder_repository import S3GlmFolderFileRepository
from data_sources.goes19_repository import (
    LocalGoes19FileRepository,
    S3Goes19FileRepository,
)
from data_sources.radar_repository import (
    LocalRadarFileRepository,
    S3RadarFileRepository,
)
from factories import create_data_source_registry
from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG
from models.input_source_config import InputSourceConfig


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
        config.ECMWF_OPENDATA_SOURCES = ("ecmwf", "azure", "aws")
        config.RADAR_INPUT_DIR = "/tmp/radar"
        config.GLM_FOLDER_INPUT_DIR = "/tmp/glm"
        config.GLM_ACCUM_MINUTES = 10
        config.GLM_PRODUCE_EVERY_MINUTES = 10
        config.WRF_INPUT_DIR = "/tmp/wrf"
        config.ENABLED_WRF_PRODUCTS = {}
        config.RADAR_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/radar")
        config.GLM_FOLDER_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/glm")
        config.WRF_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/wrf")
        config.GOES19_INPUT = InputSourceConfig(
            mode="s3", input_dir="/tmp/goes19", s3_bucket="noaa-goes19"
        )
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

    def test_radar_local_mode_builds_local_repository(self):
        config = self._build_config(tp=False, mslp=False)
        registry = create_data_source_registry(config)

        radar_source = registry.get("radar_DBZH")
        assert isinstance(radar_source._repository, LocalRadarFileRepository)

    def test_radar_s3_mode_builds_s3_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.RADAR_INPUT = InputSourceConfig(
            mode="s3",
            input_dir="/tmp/radar",
            s3_bucket="radar-input",
            s3_endpoint="rustfs:9000",
            s3_prefix="radar_h5/",
        )
        with patch("factories.S3Client") as mock_s3_cls:
            registry = create_data_source_registry(config)

        radar_source = registry.get("radar_DBZH")
        assert isinstance(radar_source._repository, S3RadarFileRepository)
        _, kwargs = mock_s3_cls.call_args
        assert kwargs["bucket_name"] == "radar-input"
        assert kwargs["endpoint_url"] == "http://rustfs:9000"

    def test_glm_s3_mode_builds_s3_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.GLM_FOLDER_INPUT = InputSourceConfig(
            mode="s3", input_dir="/tmp/glm", s3_bucket="glm-input"
        )
        with patch("factories.S3Client"):
            registry = create_data_source_registry(config)

        glm_source = registry.get("glm_folder")
        assert isinstance(glm_source._repository, S3GlmFolderFileRepository)

    def test_goes19_local_mode_builds_local_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.GOES19_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/goes19")
        registry = create_data_source_registry(config)

        abi_source = registry.get("goes19_abi_band_13")
        assert isinstance(abi_source._repository, LocalGoes19FileRepository)

    def test_goes19_defaults_to_noaa_s3_without_config(self):
        registry = create_data_source_registry(config=None)

        abi_source = registry.get("goes19_abi_band_13")
        assert isinstance(abi_source._repository, S3Goes19FileRepository)
