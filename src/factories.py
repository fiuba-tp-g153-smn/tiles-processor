"""Factory functions for constructing shared infrastructure objects from config."""

from pathlib import Path

from clients.rabbitmq_client import RabbitMQClient
from clients.s3_client import S3Client
from config import Config
from data_sources import (
    DataSourceRegistry,
    Goes19AbiDataSource,
    Goes19GlmDataSource,
    RadarDataSource,
    EcmwfDataSource,
)
from models.band_config import BAND_CONFIGS
from models.radar_config import RADAR_PRODUCT_CONFIGS
from models.ecmwf_config import ECMWF_CONFIGS


def create_data_source_registry(config: Config = None) -> DataSourceRegistry:
    """Create and populate the data source registry with all known sources."""
    registry = DataSourceRegistry()

    for _band_id, band_config in BAND_CONFIGS.items():
        if band_config.band_id.startswith("glm_"):
            # Register GLM sources (lightning products)
            registry.register(Goes19GlmDataSource(band_config))
        else:
            # Register ABI sources (band 13, 9, 2, etc.)
            registry.register(Goes19AbiDataSource(band_config))

    # Register radar data sources for each product
    if config is not None:
        radar_input_dir = Path(config.RADAR_INPUT_DIR)
        for _product_id, product_config in RADAR_PRODUCT_CONFIGS.items():
            registry.register(RadarDataSource(product_config, radar_input_dir))

        # Register ECMWF data sources
        minio_client = create_minio_client(config)
        for _product_id, ecmwf_config in ECMWF_CONFIGS.items():
            registry.register(EcmwfDataSource(ecmwf_config, minio_client))

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


def create_minio_client(config: Config) -> S3Client:
    """Build an authenticated S3 client for MinIO tile storage."""
    return S3Client.create_with_credentials(
        bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
        endpoint=config.S3_TILES_DATA_ENDPOINT,
        access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
        secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
        secure=config.S3_TILES_DATA_SECURE,
    )
