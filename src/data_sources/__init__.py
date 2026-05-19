"""Data sources package for modular data fetching."""

from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from data_sources.registry import DataSourceRegistry
from data_sources.goes19_abi import Goes19AbiDataSource
from data_sources.glm_folder import GlmFolderDataSource
from data_sources.radar import RadarDataSource
from data_sources.ecmwf_producer_source import EcmwfProducerDataSource
from data_sources.ecmwf_period_source import EcmwfPeriodDataSource

__all__ = [
    "DataSource",
    "ImageInfo",
    "DiscoveryConfig",
    "DataSourceRegistry",
    "Goes19AbiDataSource",
    "GlmFolderDataSource",
    "RadarDataSource",
    "EcmwfProducerDataSource",
    "EcmwfPeriodDataSource",
]
