"""Models for the RabbitMQ work queue system."""

from models.band_config import ProductConfig
from models.work_unit import WorkUnit

__all__ = ["ProductConfig", "WorkUnit"]
