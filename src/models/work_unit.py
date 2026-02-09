"""Work unit model for the RabbitMQ work queue system."""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Dict, Optional, Any

from models.band_config import ProductConfig, get_product_config


@dataclass
class WorkUnit:
    """
    A unit of work representing a satellite image to be processed.

    Work units are the messages passed through the RabbitMQ queue.
    Each work unit represents a complete processing task: download + process.

    Lifecycle:
        1. Producer creates work unit for new images discovered from a data source
        2. Worker downloads and processes the image in a single atomic operation
        3. Complete (ACK) or retry/DLQ on failure

    Attributes:
        work_unit_id: Unique identifier for this work unit
        image_id: Original filename from source (unique per image)
        data_source_id: ID of the data source (e.g., "goes19_band_13")
        source_uri: Full URI to the source file (e.g., S3 key)
        output_prefix: S3 prefix for output tiles
        bounds: Geographic bounding box for clipping
        processor_id: ID of the processor to use (e.g., "goes_band_13")
        product_id: Product being processed (e.g., "band_13", "ecmwf_total_precipitation")
        created_at: Timestamp when work unit was created
        retry_count: Number of times this work unit has been retried
        max_retries: Maximum retry attempts before sending to DLQ
    """

    work_unit_id: str
    image_id: str
    data_source_id: str
    source_uri: str
    output_prefix: str
    bounds: Dict[str, float]
    processor_id: str
    product_id: str
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    retry_count: int = 0
    max_retries: int = 3

    @property
    def product_config(self) -> ProductConfig:
        """Get the product configuration for this work unit."""
        return get_product_config(self.product_id)

    # Backwards compatibility alias
    @property
    def band_config(self) -> ProductConfig:
        """Deprecated: Use product_config instead."""
        return self.product_config

    @property
    def can_retry(self) -> bool:
        """Check if this work unit can be retried."""
        return self.retry_count < self.max_retries

    def create_retry(self) -> "WorkUnit":
        """Create a copy of this work unit for retry with incremented retry_count."""
        return WorkUnit(
            work_unit_id=self.work_unit_id,  # Keep same ID for tracking
            image_id=self.image_id,
            data_source_id=self.data_source_id,
            source_uri=self.source_uri,
            output_prefix=self.output_prefix,
            bounds=self.bounds.copy(),
            processor_id=self.processor_id,
            product_id=self.product_id,
            created_at=self.created_at,
            retry_count=self.retry_count + 1,
            max_retries=self.max_retries,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize work unit to dictionary for JSON encoding."""
        return {
            "work_unit_id": self.work_unit_id,
            "image_id": self.image_id,
            "data_source_id": self.data_source_id,
            "source_uri": self.source_uri,
            "output_prefix": self.output_prefix,
            "bounds": self.bounds,
            "processor_id": self.processor_id,
            "product_id": self.product_id,
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
            data_source_id=data["data_source_id"],
            source_uri=data["source_uri"],
            output_prefix=data["output_prefix"],
            bounds=data["bounds"],
            processor_id=data["processor_id"],
            product_id=data["product_id"],
            created_at=data.get("created_at", datetime.now(UTC).isoformat()),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "WorkUnit":
        """Deserialize work unit from JSON string."""
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def create(
        cls,
        image_id: str,
        source_uri: str,
        data_source_id: str,
        processor_id: str,
        output_prefix: str,
        bounds: Dict[str, float],
        product_id: str,
    ) -> "WorkUnit":
        """
        Factory method to create a new work unit.

        Args:
            image_id: Original filename/identifier
            source_uri: Full URI to the source file
            data_source_id: ID of the data source
            processor_id: ID of the processor to use
            output_prefix: S3 prefix for output tiles
            bounds: Geographic bounding box
            product_id: Product identifier for config lookup
        """
        return cls(
            work_unit_id=str(uuid.uuid4()),
            image_id=image_id,
            data_source_id=data_source_id,
            source_uri=source_uri,
            output_prefix=output_prefix,
            bounds=bounds,
            processor_id=processor_id,
            product_id=product_id,
        )

    def __str__(self) -> str:
        return (
            f"WorkUnit({self.image_id}, "
            f"source={self.data_source_id}, "
            f"processor={self.processor_id}, "
            f"retry={self.retry_count})"
        )

    def __repr__(self) -> str:
        return self.__str__()
