"""Processing stage definitions for the work queue system."""

from enum import Enum
from typing import Optional


class Stage(Enum):
    """
    Processing stages for satellite image processing pipeline.

    Each stage represents a discrete unit of work that can be
    processed independently by a worker. Stages are executed
    sequentially, with each stage producing output that becomes
    input for the next stage.

    Stage Flow:
        DOWNLOAD -> GEOREFERENCE -> BRIGHTNESS_TEMPERATURE ->
        GEOTIFF -> TILES_AND_UPLOAD -> CLEANUP -> (complete)
    """

    DOWNLOAD = "DOWNLOAD"
    GEOREFERENCE = "GEOREFERENCE"
    BRIGHTNESS_TEMPERATURE = "BRIGHTNESS_TEMPERATURE"
    GEOTIFF = "GEOTIFF"
    TILES_AND_UPLOAD = "TILES_AND_UPLOAD"
    CLEANUP = "CLEANUP"

    @property
    def stage_number(self) -> int:
        """Return the numeric order of this stage (1-6)."""
        order = {
            Stage.DOWNLOAD: 1,
            Stage.GEOREFERENCE: 2,
            Stage.BRIGHTNESS_TEMPERATURE: 3,
            Stage.GEOTIFF: 4,
            Stage.TILES_AND_UPLOAD: 5,
            Stage.CLEANUP: 6,
        }
        return order[self]

    @property
    def next_stage(self) -> Optional["Stage"]:
        """Return the next stage in the pipeline, or None if this is the last stage."""
        transitions = {
            Stage.DOWNLOAD: Stage.GEOREFERENCE,
            Stage.GEOREFERENCE: Stage.BRIGHTNESS_TEMPERATURE,
            Stage.BRIGHTNESS_TEMPERATURE: Stage.GEOTIFF,
            Stage.GEOTIFF: Stage.TILES_AND_UPLOAD,
            Stage.TILES_AND_UPLOAD: Stage.CLEANUP,
            Stage.CLEANUP: None,
        }
        return transitions[self]

    @property
    def is_terminal(self) -> bool:
        """Return True if this is the final stage (no next stage)."""
        return self.next_stage is None

    @classmethod
    def from_string(cls, value: str) -> "Stage":
        """Create a Stage from its string value."""
        try:
            return cls(value.upper())
        except ValueError:
            valid = [s.value for s in cls]
            raise ValueError(f"Invalid stage '{value}'. Must be one of: {valid}")
