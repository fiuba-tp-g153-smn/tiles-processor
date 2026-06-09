"""Factory functions for constructing shared infrastructure objects from config."""

import logging
from pathlib import Path
from typing import Optional

from clients.rabbitmq_client import RabbitMQClient
from clients.s3_client import S3Client
from clients.seaweedfs_filer_uploader import SeaweedFsFilerUploader
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
from data_sources.glm_folder_repository import LocalGlmFolderFileRepository
from data_sources.radar_repository import LocalRadarFileRepository
from models.band_config import BAND_CONFIGS, get_band_config
from data_sources.wrf import LocalWrfFileRepository
from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG
from models.radar_config import RADAR_PRODUCT_CONFIGS
from models.wrf_config import WRF_PRODUCT_CONFIGS

logger = logging.getLogger(__name__)


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

    for _band_id, band_config in BAND_CONFIGS.items():
        if band_config.band_id in combined_products:
            continue
        # Only ABI bands remain (band_13, band_9, band_2, ...).
        registry.register(Goes19AbiDataSource(band_config))

    # Register the folder-based GLM data source (one entry covers FED/TOE/MFA).
    if config is not None:
        glm_repo = LocalGlmFolderFileRepository(Path(config.GLM_FOLDER_INPUT_DIR))
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
        radar_input_dir = Path(config.RADAR_INPUT_DIR)
        repository = LocalRadarFileRepository(radar_input_dir)
        for _product_id, product_config in RADAR_PRODUCT_CONFIGS.items():
            registry.register(RadarDataSource(product_config, repository))

    # Register WRF data sources for each enabled product
    if config is not None:
        wrf_input_dir = Path(config.WRF_INPUT_DIR)
        wrf_repository = LocalWrfFileRepository(wrf_input_dir)
        for product_id, product_config in WRF_PRODUCT_CONFIGS.items():
            if config.ENABLED_WRF_PRODUCTS.get(product_id, False):
                registry.register(WrfDataSource(product_config, wrf_repository))

    # Register ECMWF data sources (feature-flagged)
    if config is not None and config.ENABLE_ECMWF_PRECIPITATION:
        ecmwf_s3 = create_s3_client(config, with_ttl=False)
        registry.register(EcmwfProducerDataSource(ECMWF_TP_CONFIG, ecmwf_s3))
        registry.register(EcmwfPeriodDataSource(ECMWF_TP_CONFIG, ecmwf_s3))

    if config is not None and config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE:
        ecmwf_mslp_s3 = create_s3_client(config, with_ttl=False)
        registry.register(EcmwfProducerDataSource(ECMWF_MSLP_CONFIG, ecmwf_mslp_s3))
        registry.register(EcmwfPeriodDataSource(ECMWF_MSLP_CONFIG, ecmwf_mslp_s3))

    return registry


def create_rabbitmq_client(config: Config) -> RabbitMQClient:
    """Build and connect a RabbitMQ client from application config.

    Passes the light queue name so every role declares both work queues on
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
        light_queue_name=config.RABBITMQ_LIGHT_QUEUE,
    )
    client.connect(max_retries=10, retry_delay=5.0)
    return client


def create_s3_client(config: Config, *, with_ttl: str | None | bool = True) -> S3Client:
    """Build an authenticated S3 client for tile storage.

    Args:
        config: Application configuration.
        with_ttl: Controls the TTL passed to SeaweedFS.
            True  → use config.SEAWEEDFS_TILE_TTL (default for GOES/GLM)
            False → no TTL (kept for backward compatibility)
            str   → explicit TTL string (e.g. "168h" for radar)
            None  → no TTL (e.g. when SEAWEEDFS_RADAR_TILE_TTL is unset)
    """
    tile_uploader_overwritten = None
    if config.SEAWEEDFS_FILER_ENDPOINT:
        if with_ttl is True:
            ttl = config.SEAWEEDFS_TILE_TTL
        elif with_ttl is False:
            ttl = None
        else:
            ttl = with_ttl  # explicit str or None
        tile_uploader_overwritten = SeaweedFsFilerUploader(
            endpoint=config.SEAWEEDFS_FILER_ENDPOINT,
            bucket=config.S3_TILES_DATA_BUCKET_NAME,
            ttl=ttl,
            secure=config.S3_TILES_DATA_SECURE,
        )
        logger.info(
            "S3 tile uploads overwritten with %s (endpoint=%s, ttl=%s)",
            type(tile_uploader_overwritten).__name__,
            config.SEAWEEDFS_FILER_ENDPOINT,
            ttl,
        )
    else:
        logger.info("S3 tile uploads using standard S3 put_object")

    return S3Client.create_with_credentials(
        bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
        endpoint=config.S3_TILES_DATA_ENDPOINT,
        access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
        secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
        secure=config.S3_TILES_DATA_SECURE,
        tile_uploader_overwritten=tile_uploader_overwritten,
    )
