"""Band 9 specific processor."""

from processors.goes_processor import GoesProcessor


class Band9Processor(GoesProcessor):
    """
    Processor for Band 9 (Mid-Level Water Vapor).
    Uses the WATER_VAPOR_PALETTE (handled by configuration).
    """
