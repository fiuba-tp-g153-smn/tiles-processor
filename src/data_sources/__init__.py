"""Data sources package for modular data fetching."""

from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from data_sources.registry import DataSourceRegistry
from data_sources.goes19 import Goes19DataSource
from data_sources.glm19 import Glm19DataSource
from data_sources.radar import RadarDataSource

__all__ = [
    "DataSource",
    "ImageInfo",
    "DiscoveryConfig",
    "DataSourceRegistry",
    "Goes19DataSource",
    "Glm19DataSource",
    "RadarDataSource",
]
