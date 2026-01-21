"""Processing stage definitions for the work queue system."""

from enum import Enum
from typing import Optional


class Stage(Enum):
    """
    Processing stages for satellite image processing pipeline.

    Each stage represents a discrete unit of work that can be
    processed independently by a worker.

    Stage Flow:
        DOWNLOAD -> PROCESS -> (complete)
    """

    DOWNLOAD = "DOWNLOAD"
    PROCESS = "PROCESS"

    @property
    def stage_number(self) -> int:
        """Return the numeric order of this stage (1-2)."""
        order = {
            Stage.DOWNLOAD: 1,
            Stage.PROCESS: 2,
        }
        return order[self]

    @property
    def next_stage(self) -> Optional["Stage"]:
        """Return the next stage in the pipeline, or None if this is the last stage."""
        transitions = {
            Stage.DOWNLOAD: Stage.PROCESS,
            Stage.PROCESS: None,
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
