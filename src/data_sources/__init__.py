"""Data sources package for modular data fetching."""

from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from data_sources.registry import DataSourceRegistry
from data_sources.goes19 import Goes19DataSource

__all__ = [
    "DataSource",
    "ImageInfo",
    "DiscoveryConfig",
    "DataSourceRegistry",
    "Goes19DataSource",
]
