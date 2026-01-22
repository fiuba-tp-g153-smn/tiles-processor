"""Processors package for image processing pipelines."""

from processors.base_processor import ImageProcessor
from processors.registry import ProcessorRegistry
from processors.goes_processor import GoesProcessor
from processors.band13_processor import Band13Processor
from processors.band9_processor import Band9Processor

# Register GOES processors
# Using GoesProcessor for both bands since the differentiation is handled by BandConfig
ProcessorRegistry.register("goes_band_13", GoesProcessor)
ProcessorRegistry.register("goes_band_9", GoesProcessor)

__all__ = [
    "ImageProcessor",
    "ProcessorRegistry",
    "GoesProcessor",
    "Band13Processor",
    "Band9Processor",
]
