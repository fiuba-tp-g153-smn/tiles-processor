"""Work unit model for the RabbitMQ work queue system."""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Dict, Optional, Any

from models.stage import Stage
from models.band_config import BandConfig, get_band_config


@dataclass
class WorkUnitPaths:
    """
    File paths used throughout the processing pipeline.

    Each stage populates its output path, which becomes input for the next stage.
    All paths are relative to the shared filesystem mounted in Docker.
    """

    source_s3_uri: str  # Original S3 URI (noaa-goes19 bucket)
    local_netcdf: Optional[str] = None  # After DOWNLOAD
    georef_data: Optional[str] = None  # After GEOREFERENCE (pickle)
    temp_data: Optional[str] = None  # After BRIGHTNESS_TEMPERATURE (pickle)
    geotiff: Optional[str] = None  # After GEOTIFF
    tiles_dir: Optional[str] = None  # After TILES generation
    s3_tileset_prefix: Optional[str] = None  # After UPLOAD

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Serialize to dictionary."""
        return {
            "source_s3_uri": self.source_s3_uri,
            "local_netcdf": self.local_netcdf,
            "georef_data": self.georef_data,
            "temp_data": self.temp_data,
            "geotiff": self.geotiff,
            "tiles_dir": self.tiles_dir,
            "s3_tileset_prefix": self.s3_tileset_prefix,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkUnitPaths":
        """Deserialize from dictionary."""
        return cls(
            source_s3_uri=data["source_s3_uri"],
            local_netcdf=data.get("local_netcdf"),
            georef_data=data.get("georef_data"),
            temp_data=data.get("temp_data"),
            geotiff=data.get("geotiff"),
            tiles_dir=data.get("tiles_dir"),
            s3_tileset_prefix=data.get("s3_tileset_prefix"),
        )


@dataclass
class WorkUnit:
    """
    A unit of work representing a single processing stage for a satellite image.

    Work units are the messages passed through the RabbitMQ queue. Each work unit
    contains all the metadata needed to process a specific stage of the pipeline
    for a specific satellite image.

    Lifecycle:
        1. Producer creates work unit with stage=DOWNLOAD for new images
        2. Worker processes the stage, updates paths, creates next stage work unit
        3. Process continues until CLEANUP stage completes

    Attributes:
        work_unit_id: Unique identifier for this work unit
        image_id: Original filename from NOAA (unique per image)
        band_id: Band being processed (band_13, band_9)
        stage: Current processing stage
        paths: File paths populated by each stage
        bounds: Geographic bounding box for clipping
        created_at: Timestamp when work unit was created
        retry_count: Number of times this work unit has been retried
        max_retries: Maximum retry attempts before sending to DLQ
    """

    work_unit_id: str
    image_id: str
    band_id: str
    stage: Stage
    paths: WorkUnitPaths
    bounds: Dict[str, float]
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    retry_count: int = 0
    max_retries: int = 3

    @property
    def band_config(self) -> BandConfig:
        """Get the band configuration for this work unit."""
        return get_band_config(self.band_id)

    @property
    def stage_number(self) -> int:
        """Get the numeric stage number (1-6)."""
        return self.stage.stage_number

    @property
    def is_terminal(self) -> bool:
        """Check if this is the final stage."""
        return self.stage.is_terminal

    @property
    def can_retry(self) -> bool:
        """Check if this work unit can be retried."""
        return self.retry_count < self.max_retries

    def create_next_stage(self) -> Optional["WorkUnit"]:
        """
        Create a work unit for the next stage in the pipeline.

        Returns None if this is the terminal stage.
        """
        next_stage = self.stage.next_stage
        if next_stage is None:
            return None

        return WorkUnit(
            work_unit_id=str(uuid.uuid4()),
            image_id=self.image_id,
            band_id=self.band_id,
            stage=next_stage,
            paths=WorkUnitPaths(
                source_s3_uri=self.paths.source_s3_uri,
                local_netcdf=self.paths.local_netcdf,
                georef_data=self.paths.georef_data,
                temp_data=self.paths.temp_data,
                geotiff=self.paths.geotiff,
                tiles_dir=self.paths.tiles_dir,
                s3_tileset_prefix=self.paths.s3_tileset_prefix,
            ),
            bounds=self.bounds.copy(),
            retry_count=0,
            max_retries=self.max_retries,
        )

    def create_retry(self) -> "WorkUnit":
        """Create a copy of this work unit for retry with incremented retry_count."""
        return WorkUnit(
            work_unit_id=self.work_unit_id,  # Keep same ID for tracking
            image_id=self.image_id,
            band_id=self.band_id,
            stage=self.stage,
            paths=WorkUnitPaths(
                source_s3_uri=self.paths.source_s3_uri,
                local_netcdf=self.paths.local_netcdf,
                georef_data=self.paths.georef_data,
                temp_data=self.paths.temp_data,
                geotiff=self.paths.geotiff,
                tiles_dir=self.paths.tiles_dir,
                s3_tileset_prefix=self.paths.s3_tileset_prefix,
            ),
            bounds=self.bounds.copy(),
            created_at=self.created_at,
            retry_count=self.retry_count + 1,
            max_retries=self.max_retries,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize work unit to dictionary for JSON encoding."""
        return {
            "work_unit_id": self.work_unit_id,
            "image_id": self.image_id,
            "band_id": self.band_id,
            "stage": self.stage.value,
            "paths": self.paths.to_dict(),
            "bounds": self.bounds,
            "created_at": self.created_at,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
        }

    def to_json(self) -> str:
        """Serialize work unit to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkUnit":
        """Deserialize work unit from dictionary."""
        return cls(
            work_unit_id=data["work_unit_id"],
            image_id=data["image_id"],
            band_id=data["band_id"],
            stage=Stage.from_string(data["stage"]),
            paths=WorkUnitPaths.from_dict(data["paths"]),
            bounds=data["bounds"],
            created_at=data.get("created_at", datetime.now(UTC).isoformat()),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "WorkUnit":
        """Deserialize work unit from JSON string."""
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def create_download_work_unit(
        cls,
        source_s3_uri: str,
        band_id: str,
        bounds: Dict[str, float],
    ) -> "WorkUnit":
        """
        Factory method to create the initial DOWNLOAD work unit.

        This is used by the producer when discovering new images.

        Args:
            source_s3_uri: Full S3 URI to the source file (noaa-goes19 bucket)
            band_id: Band to process (band_13, band_9)
            bounds: Geographic bounding box for clipping

        Returns:
            WorkUnit configured for the DOWNLOAD stage
        """
        # Extract image_id from the S3 URI (filename without extension)
        image_id = source_s3_uri.split("/")[-1]

        return cls(
            work_unit_id=str(uuid.uuid4()),
            image_id=image_id,
            band_id=band_id,
            stage=Stage.DOWNLOAD,
            paths=WorkUnitPaths(source_s3_uri=source_s3_uri),
            bounds=bounds,
        )

    def __str__(self) -> str:
        return f"WorkUnit({self.image_id}, stage={self.stage.value}, retry={self.retry_count})"

    def __repr__(self) -> str:
        return self.__str__()
