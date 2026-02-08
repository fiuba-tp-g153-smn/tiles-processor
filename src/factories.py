"""Factory functions for constructing shared infrastructure objects from config."""

from clients.rabbitmq_client import RabbitMQClient
from clients.s3_client import S3Client
from config import Config
from data_sources import DataSourceRegistry, Goes19DataSource, RadarDataSource
from models.band_config import BAND_CONFIGS


def create_data_source_registry() -> DataSourceRegistry:
    """Create and populate the data source registry with all known sources."""
    registry = DataSourceRegistry()

    for _band_id, band_config in BAND_CONFIGS.items():
        registry.register(Goes19DataSource(band_config))

    registry.register(RadarDataSource())
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
