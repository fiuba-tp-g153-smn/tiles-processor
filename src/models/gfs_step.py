"""The per-step context every GFS product reads out of its work unit."""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from models.work_unit import WorkUnit


@dataclass(frozen=True, slots=True)
class GfsStepContext:
    """One forecast step's identity, resolved from a rendering work unit.

    `GfsGribDownloader` fans out one unit per (step, product) carrying the same
    JSON `source_uri`, so every GFS processor starts by parsing the same three
    things out of it. Composing this instead of repeating the block keeps the
    two processors from drifting on what counts as a usable step — and keeps
    the S3 namespace format in exactly one place.
    """

    grib_path: Path
    cycle_ts: str  # S3 namespace for the cycle, e.g. 20260808T0000Z
    step_hours: int

    @classmethod
    def from_work_unit(
        cls, work_unit: WorkUnit, downloaded_file_path: str
    ) -> "GfsStepContext":
        """Parse a rendering unit, failing fast if the GRIB never landed.

        Raises:
            FileNotFoundError: the downloaded GRIB is missing. The work handler
                maps it to `SourceFileNotFoundError`, i.e. a visible terminal
                error rather than a retry loop that cannot succeed.
        """
        grib_path = Path(downloaded_file_path)
        if not grib_path.exists():
            raise FileNotFoundError(f"GRIB file not found: {grib_path}")

        meta = json.loads(work_unit.source_uri)
        return cls(
            grib_path=grib_path,
            cycle_ts=_fmt_cycle(meta["cycle"]),
            step_hours=int(meta["step_hours"]),
        )


def _fmt_cycle(cycle_iso: str) -> str:
    """Format the cycle timestamp used as the S3 namespace."""
    return datetime.fromisoformat(cycle_iso).strftime("%Y%m%dT%H%MZ")
