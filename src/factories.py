"""Factory functions for constructing shared infrastructure objects from config."""

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from clients.rabbitmq_client import RabbitMQClient
from clients.s3_client import S3Client
from clients.seaweedfs_filer_uploader import SeaweedFsFilerUploader
from config import Config
from data_sources import (
    DataSourceRegistry,
    Goes19AbiDataSource,
    Goes19GlmDataSource,
    RadarDataSource,
)
from data_sources.radar_repository import LocalRadarFileRepository
from models.band_config import BAND_CONFIGS
from models.radar_config import RADAR_PRODUCT_CONFIGS


def create_data_source_registry(config: Optional[Config] = None) -> DataSourceRegistry:
    """Create and populate the data source registry with all known sources."""
    registry = DataSourceRegistry()

    # Products computed as by-products of another source's processor (no separate download)
    combined_products = {"glm_toe", "glm_mfa"}

    for _band_id, band_config in BAND_CONFIGS.items():
        if band_config.band_id in combined_products:
            continue
        if band_config.band_id.startswith("glm_"):
            # Register GLM sources (lightning products)
            registry.register(Goes19GlmDataSource(band_config))
        else:
            # Register ABI sources (band 13, 9, 2, etc.)
            registry.register(Goes19AbiDataSource(band_config))

    # Register radar data sources for each product
    if config is not None:
        radar_input_dir = Path(config.RADAR_INPUT_DIR)
        repository = LocalRadarFileRepository(radar_input_dir)
        for _product_id, product_config in RADAR_PRODUCT_CONFIGS.items():
            registry.register(RadarDataSource(product_config, repository))

    return registry


def create_rabbitmq_client(config: Config) -> RabbitMQClient:
    """Build and connect a RabbitMQ client from application config."""
    client = RabbitMQClient(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        username=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD,
        queue_name=config.RABBITMQ_QUEUE,
        dlq_name=config.RABBITMQ_DLQ,
        dlx_name=config.RABBITMQ_DLX,
    )
    client.connect(max_retries=10, retry_delay=5.0)
    return client


def create_s3_client(config: Config) -> S3Client:
    """Build an authenticated S3 client for tile storage."""
    tile_uploader_overwritten = None
    if config.SEAWEEDFS_FILER_ENDPOINT:
        tile_uploader_overwritten = SeaweedFsFilerUploader(
            endpoint=config.SEAWEEDFS_FILER_ENDPOINT,
            bucket=config.S3_TILES_DATA_BUCKET_NAME,
            ttl=config.SEAWEEDFS_TILE_TTL,
            secure=config.S3_TILES_DATA_SECURE,
        )
        logger.info(
            "S3 tile uploads overwritten with %s (endpoint=%s, ttl=%s)",
            type(tile_uploader_overwritten).__name__,
            config.SEAWEEDFS_FILER_ENDPOINT,
            config.SEAWEEDFS_TILE_TTL,
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
