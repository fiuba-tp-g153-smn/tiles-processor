"""Weather Radar processor implementation (placeholder)."""

from logging import getLogger

from models.work_unit import WorkUnit
from processors.base_processor import ImageProcessor

logger = getLogger(__name__)


class RadarProcessor(ImageProcessor):
    """
    Processor for weather radar imagery.

    This is a placeholder implementation. To be implemented when
    integrating with weather radar data processing.
    """

    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """
        Process radar imagery and upload tiles to S3.

        TODO: Implement actual radar processing logic.

        Args:
            downloaded_file_path: Path to the downloaded radar file
            work_unit: The work unit containing metadata

        Raises:
            NotImplementedError: Always (placeholder implementation)
        """
        raise NotImplementedError(
            f"[radar] Radar processing not implemented for {work_unit.image_id}"
        )
