"""Factory functions for constructing shared infrastructure objects from config."""

import logging
from pathlib import Path
from typing import Optional

from clients.rabbitmq_client import RabbitMQClient
from clients.s3_client import S3Client
from config import Config
from data_sources import (
    DataSourceRegistry,
    EcmwfPeriodDataSource,
    EcmwfProducerDataSource,
    GlmFolderDataSource,
    Goes19AbiDataSource,
    RadarDataSource,
    WrfDataSource,
)
from data_sources.glm_folder_repository import (
    GlmFolderFileRepository,
    LocalGlmFolderFileRepository,
    S3GlmFolderFileRepository,
)
from data_sources.goes19_repository import (
    GOES19_BUCKET_NAME,
    Goes19FileRepository,
    LocalGoes19FileRepository,
    S3Goes19FileRepository,
)
from data_sources.radar_repository import (
    LocalRadarFileRepository,
    RadarFileRepository,
    S3RadarFileRepository,
)
from data_sources.wrf_repository import (
    LocalWrfFileRepository,
    S3WrfFileRepository,
    WrfFileRepository,
)
from models.band_config import BAND_CONFIGS, get_band_config
from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG
from models.input_source_config import InputSourceConfig
from models.radar_config import RADAR_PRODUCT_CONFIGS
from models.wrf_config import WRF_PRODUCT_CONFIGS

logger = logging.getLogger(__name__)


def _create_input_s3_client(src: InputSourceConfig) -> S3Client:
    """Build an S3 client for one source's input bucket (anonymous if no creds)."""
    endpoint_url = None
    if src.s3_endpoint:
        protocol = "https" if src.s3_secure else "http"
        endpoint_url = f"{protocol}://{src.s3_endpoint}"
    return S3Client(
        bucket_name=src.s3_bucket,
        endpoint_url=endpoint_url,
        access_key=src.s3_access_key,
        secret_key=src.s3_secret_key,
    )


def _create_radar_repository(config: Config) -> RadarFileRepository:
    src = config.RADAR_INPUT
    if not src.is_s3:
        return LocalRadarFileRepository(Path(src.input_dir))
    return S3RadarFileRepository(_create_input_s3_client(src), prefix=src.s3_prefix)


def _create_glm_folder_repository(config: Config) -> GlmFolderFileRepository:
    src = config.GLM_FOLDER_INPUT
    if not src.is_s3:
        return LocalGlmFolderFileRepository(Path(src.input_dir))
    return S3GlmFolderFileRepository(_create_input_s3_client(src), prefix=src.s3_prefix)


def _create_wrf_repository(config: Config) -> WrfFileRepository:
    src = config.WRF_INPUT
    if not src.is_s3:
        return LocalWrfFileRepository(Path(src.input_dir))
    return S3WrfFileRepository(_create_input_s3_client(src), prefix=src.s3_prefix)


def _create_goes19_repository(config: Optional[Config]) -> Goes19FileRepository:
    # No config (some tests/paths) keeps the historical default: NOAA unsigned.
    if config is None:
        return S3Goes19FileRepository(S3Client(GOES19_BUCKET_NAME))
    src = config.GOES19_INPUT
    if not src.is_s3:
        return LocalGoes19FileRepository(Path(src.input_dir))
    return S3Goes19FileRepository(_create_input_s3_client(src))


def create_data_source_registry(config: Optional[Config] = None) -> DataSourceRegistry:
    """Create and populate the data source registry with all known sources."""
    registry = DataSourceRegistry()

    # BandConfigs that are produced as by-products of another source's processor
    # and therefore must NOT get their own DataSource registration. The
    # folder-based GLM pipeline registers exactly one source below, which emits
    # FED/TOE/MFA tiles in the same processor run.
    combined_products = {
        "glm_folder_fed",
        "glm_folder_toe",
        "glm_folder_mfa",
    }

    goes19_repo = _create_goes19_repository(config)
    for _band_id, band_config in BAND_CONFIGS.items():
        if band_config.band_id in combined_products:
            continue
        # Only ABI bands remain (band_13, band_9, band_2, ...).
        registry.register(Goes19AbiDataSource(band_config, goes19_repo))

    # Register the folder-based GLM data source (one entry covers FED/TOE/MFA).
    if config is not None:
        glm_repo = _create_glm_folder_repository(config)
        registry.register(
            GlmFolderDataSource(
                get_band_config("glm_folder_fed"),
                glm_repo,
                accum_minutes=config.GLM_ACCUM_MINUTES,
                produce_every_minutes=config.GLM_PRODUCE_EVERY_MINUTES,
            )
        )

    # Register radar data sources for each product
    if config is not None:
        repository = _create_radar_repository(config)
        for _product_id, product_config in RADAR_PRODUCT_CONFIGS.items():
            registry.register(RadarDataSource(product_config, repository))

    # Register WRF data sources for each enabled product
    if config is not None:
        wrf_repository = _create_wrf_repository(config)
        for product_id, product_config in WRF_PRODUCT_CONFIGS.items():
            if config.ENABLED_WRF_PRODUCTS.get(product_id, False):
                registry.register(WrfDataSource(product_config, wrf_repository))

    # Register ECMWF data sources (feature-flagged)
    if config is not None and config.ENABLE_ECMWF_PRECIPITATION:
        ecmwf_s3 = create_s3_client(config)
        registry.register(EcmwfProducerDataSource(ECMWF_TP_CONFIG, ecmwf_s3))
        registry.register(EcmwfPeriodDataSource(ECMWF_TP_CONFIG, ecmwf_s3))

    if config is not None and config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE:
        ecmwf_mslp_s3 = create_s3_client(config)
        registry.register(EcmwfProducerDataSource(ECMWF_MSLP_CONFIG, ecmwf_mslp_s3))
        registry.register(EcmwfPeriodDataSource(ECMWF_MSLP_CONFIG, ecmwf_mslp_s3))

    return registry


def create_rabbitmq_client(config: Config) -> RabbitMQClient:
    """Build and connect a RabbitMQ client from application config.

    Passes the light queue names so every role declares all work queues on
    connect (idempotent), removing any boot-order dependency between producer
    and the light-worker pool.
    """
    client = RabbitMQClient(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        username=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD,
        queue_name=config.RABBITMQ_QUEUE,
        dlq_name=config.RABBITMQ_DLQ,
        dlx_name=config.RABBITMQ_DLX,
        extra_queue_names=[
            config.RABBITMQ_RADAR_LIGHT_QUEUE,
            config.RABBITMQ_WRF_LIGHT_QUEUE,
        ],
    )
    client.connect(max_retries=10, retry_delay=5.0)
    return client


def create_s3_client(config: Config) -> S3Client:
    """Build an authenticated S3 client for tile storage.

    Uploads go through the plain S3 ``put_object`` API, so the backend (SeaweedFS
    gateway, MinIO, AWS S3) is swappable. Object expiry is handled by per-prefix
    bucket lifecycle rules (see ``S3Client.configure_lifecycle_policy``), not by
    a backend-specific per-object TTL.
    """
    return S3Client.create_with_credentials(
        bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
        endpoint=config.S3_TILES_DATA_ENDPOINT,
        access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
        secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
        secure=config.S3_TILES_DATA_SECURE,
        upload_concurrency=config.S3_UPLOAD_CONCURRENCY,
    )
