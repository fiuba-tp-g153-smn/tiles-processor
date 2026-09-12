import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from config import Config
from data_sources.ecmwf_repository import (
    LocalEcmwfGribRepository,
    OpenDataEcmwfGribRepository,
    S3EcmwfGribRepository,
)
from data_sources.gfs_fetcher import GfsGribFetcher
from data_sources.gfs_repository import LocalGfsGribRepository, S3GfsGribRepository
from data_sources.glm_folder_repository import (
    LocalGlmFolderFileRepository,
    S3GlmFolderFileRepository,
)
from data_sources.goes19_repository import (
    LocalGoes19FileRepository,
    S3Goes19FileRepository,
)
from data_sources.radar_repository import (
    LocalRadarFileRepository,
    S3RadarFileRepository,
)
from data_sources.wrf_repository import LocalWrfFileRepository, S3WrfFileRepository
from factories import create_data_source_registry
from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG
from models.gfs_config import GfsAccessConfig
from models.input_source_config import InputSourceConfig
from models.radar_config import RadarStationFilter


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
        config.RADAR_STATION_FILTER = RadarStationFilter("all")
        # Discovery knobs: None -> each data source keeps its class-constant default.
        config.GOES_TARGET_IMAGES = None
        config.GOES_MAX_HOURS_BACK = None
        config.RADAR_TARGET_IMAGES = None
        config.WRF_TARGET_RUNS = None
        config.GLM_SAFETY_LAG_SECONDS = None
        config.GLM_TARGET_WINDOWS = None
        config.ENABLE_ECMWF_PRECIPITATION = tp
        config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE = mslp
        config.ECMWF_OPENDATA_SOURCES = ("ecmwf", "azure", "aws")
        config.ENABLE_GFS_MSLP = False
        config.ENABLE_GFS_500 = False
        config.ENABLE_GFS_250 = False
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
        config.ECMWF_INPUT = InputSourceConfig(mode="opendata", input_dir="/tmp/ecmwf")
        config.GFS_INPUT = InputSourceConfig(mode="nomads", input_dir="/tmp/gfs")
        return config

    @staticmethod
    def _enable_gfs(config) -> None:
        """Switch on one GFS product and the knobs its producer reads."""
        config.ENABLE_GFS_MSLP = True
        config.GFS_CYCLES_TO_MAINTAIN = 3
        config.GFS_MAX_STEPS_PER_TICK = 12
        config.GFS_AVAILABILITY_PROBE_FROM_HOURS = 3
        config.GFS_AVAILABILITY_PROBE_TO_HOURS = 8

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

        radar_source = registry.get("radar_sinarame_dbzh")
        assert isinstance(radar_source._repository, LocalRadarFileRepository)

    def test_radar_s3_mode_builds_s3_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.RADAR_INPUT = InputSourceConfig(
            mode="s3",
            input_dir="/tmp/radar",
            s3_bucket="radar-input",
            s3_endpoint="seaweedfs:8333",
            s3_prefix="radar_h5/",
        )
        with patch("factories.S3Client") as mock_s3_cls:
            registry = create_data_source_registry(config)

        radar_source = registry.get("radar_sinarame_dbzh")
        assert isinstance(radar_source._repository, S3RadarFileRepository)
        _, kwargs = mock_s3_cls.call_args
        assert kwargs["bucket_name"] == "radar-input"
        assert kwargs["endpoint_url"] == "http://seaweedfs:8333"

    def test_glm_s3_mode_builds_s3_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.GLM_FOLDER_INPUT = InputSourceConfig(
            mode="s3", input_dir="/tmp/glm", s3_bucket="glm-input"
        )
        with patch("factories.S3Client"):
            registry = create_data_source_registry(config)

        glm_source = registry.get("goes19_glm")
        assert isinstance(glm_source._repository, S3GlmFolderFileRepository)

    def test_goes19_local_mode_builds_local_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.GOES19_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/goes19")
        registry = create_data_source_registry(config)

        abi_source = registry.get("goes19_abi_c13")
        assert isinstance(abi_source._repository, LocalGoes19FileRepository)

    def test_goes19_s3_mode_passes_endpoint_and_prefix(self):
        """A self-hosted mirror needs both its URL and its sub-prefix."""
        config = self._build_config(tp=False, mslp=False)
        config.GOES19_INPUT = InputSourceConfig(
            mode="s3",
            input_dir="/tmp/goes19",
            s3_bucket="goes-mirror",
            s3_endpoint="http://minio.internal:9000",
            s3_prefix="mirror/goes/",
        )
        with patch("factories.S3Client") as mock_s3_cls:
            registry = create_data_source_registry(config)

        abi_source = registry.get("goes19_abi_c13")
        assert isinstance(abi_source._repository, S3Goes19FileRepository)
        assert abi_source._repository._prefix == "mirror/goes/"
        _, kwargs = mock_s3_cls.call_args
        assert kwargs["bucket_name"] == "goes-mirror"
        assert kwargs["endpoint_url"] == "http://minio.internal:9000"

    def test_input_client_receives_region_and_addressing_style(self):
        """Both are needed to reach a bucket outside us-east-1 or a virtual host."""
        config = self._build_config(tp=False, mslp=False)
        config.RADAR_INPUT = InputSourceConfig(
            mode="s3",
            input_dir="/tmp/radar",
            s3_bucket="radar-input",
            s3_region="sa-east-1",
            s3_addressing_style="virtual",
        )
        with patch("factories.S3Client") as mock_s3_cls:
            create_data_source_registry(config)

        _, kwargs = mock_s3_cls.call_args
        assert kwargs["region_name"] == "sa-east-1"
        assert kwargs["addressing_style"] == "virtual"

    def test_glm_local_mode_builds_local_repository(self):
        config = self._build_config(tp=False, mslp=False)
        registry = create_data_source_registry(config)

        assert isinstance(
            registry.get("goes19_glm")._repository, LocalGlmFolderFileRepository
        )

    def test_wrf_local_mode_builds_local_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.ENABLED_WRF_PRODUCTS = {"Colmax": True}
        registry = create_data_source_registry(config)

        assert isinstance(
            registry.get("wrf_Colmax")._repository, LocalWrfFileRepository
        )

    def test_wrf_s3_mode_builds_s3_repository(self):
        config = self._build_config(tp=False, mslp=False)
        config.ENABLED_WRF_PRODUCTS = {"Colmax": True}
        config.WRF_INPUT = InputSourceConfig(
            mode="s3",
            input_dir="/tmp/wrf",
            s3_bucket="wrf-input",
            s3_prefix="wrf_nc/",
        )
        with patch("factories.S3Client"):
            registry = create_data_source_registry(config)

        repository = registry.get("wrf_Colmax")._repository
        assert isinstance(repository, S3WrfFileRepository)
        assert repository._prefix == "wrf_nc/"

    def test_ecmwf_defaults_to_the_opendata_mirrors(self):
        config = self._build_config(tp=True, mslp=False)
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        source = registry.get(ECMWF_TP_CONFIG.producer_data_source_id)
        assert isinstance(source._repository, OpenDataEcmwfGribRepository)

    def test_ecmwf_local_mode_builds_local_repository(self):
        config = self._build_config(tp=True, mslp=False)
        config.ECMWF_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/ecmwf")
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        source = registry.get(ECMWF_TP_CONFIG.producer_data_source_id)
        assert isinstance(source._repository, LocalEcmwfGribRepository)

    def test_ecmwf_s3_mode_builds_s3_repository(self):
        config = self._build_config(tp=True, mslp=False)
        config.ECMWF_INPUT = InputSourceConfig(
            mode="s3", input_dir="/tmp/ecmwf", s3_bucket="models"
        )
        with patch("factories.S3Client"), patch(
            "factories.create_s3_client", return_value=MagicMock()
        ):
            registry = create_data_source_registry(config)

        source = registry.get(ECMWF_TP_CONFIG.producer_data_source_id)
        assert isinstance(source._repository, S3EcmwfGribRepository)

    def test_ecmwf_products_get_their_own_repositories(self):
        """Both products share one input root but must not read each other's runs."""
        config = self._build_config(tp=True, mslp=True)
        config.ECMWF_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/ecmwf")
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        tp = registry.get(ECMWF_TP_CONFIG.producer_data_source_id)._repository
        mslp = registry.get(ECMWF_MSLP_CONFIG.producer_data_source_id)._repository
        assert tp._product_dir != mslp._product_dir

    def test_gfs_defaults_to_the_nomads_fetcher(self):
        config = self._build_config(tp=False, mslp=False)
        self._enable_gfs(config)
        config.GFS_ACCESS = GfsAccessConfig(subset_endpoint="http://nomads/cgi")
        config.get_bounds.return_value = {
            "minx": -110.0,
            "miny": -60.0,
            "maxx": -30.0,
            "maxy": -15.0,
        }
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        assert isinstance(registry.get("gfs_producer")._repository, GfsGribFetcher)

    def test_gfs_local_mode_builds_local_repository(self):
        config = self._build_config(tp=False, mslp=False)
        self._enable_gfs(config)
        config.GFS_INPUT = InputSourceConfig(mode="local", input_dir="/tmp/gfs")
        with patch("factories.create_s3_client", return_value=MagicMock()):
            registry = create_data_source_registry(config)

        assert isinstance(
            registry.get("gfs_producer")._repository, LocalGfsGribRepository
        )

    def test_gfs_s3_mode_builds_s3_repository(self):
        config = self._build_config(tp=False, mslp=False)
        self._enable_gfs(config)
        config.GFS_INPUT = InputSourceConfig(
            mode="s3",
            input_dir="/tmp/gfs",
            s3_bucket="models",
            s3_prefix="grib/models/gfs/",
        )
        with patch("factories.create_s3_client", return_value=MagicMock()), patch(
            "factories.S3Client"
        ):
            registry = create_data_source_registry(config)

        repository = registry.get("gfs_producer")._repository
        assert isinstance(repository, S3GfsGribRepository)
        assert repository._prefix == "grib/models/gfs/"

    def test_goes19_defaults_to_noaa_s3_without_config(self):
        registry = create_data_source_registry(config=None)

        abi_source = registry.get("goes19_abi_c13")
        assert isinstance(abi_source._repository, S3Goes19FileRepository)
