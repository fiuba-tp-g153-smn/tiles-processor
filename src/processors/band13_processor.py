"""Band 13 specific processor."""

from processors.goes_processor import GoesProcessor


class Band13Processor(GoesProcessor):
    """
    Processor for Band 13 (Clean IR Longwave Window).
    Uses the CLOUD_TOPS_PALETTE (default in GoesProcessor).
    """

    pass
