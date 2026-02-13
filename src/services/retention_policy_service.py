"""Service for managing tileset retention policies in MinIO."""

import logging

from clients.s3_client import S3Client

logger = logging.getLogger(__name__)


class RetentionPolicyService:  # pylint: disable=too-few-public-methods
    """
    Service for managing tileset retention policies in MinIO.

    Ensures that only the latest N tilesets are kept for each data product,
    automatically deleting older tilesets to prevent unbounded storage growth.

    This service follows the Single Responsibility Principle by isolating
    retention policy logic from processor implementations.
    """

    DEFAULT_RETENTION_COUNT = 26  # ~4 hours at 10-min intervals
    MAX_DELETE_PER_PASS = 10  # Safety limit to prevent mass deletions

    def __init__(self, s3_client: S3Client):
        """
        Initialize retention policy service.

        Args:
            s3_client: S3 client for MinIO operations
        """
        self._s3_client = s3_client

    async def enforce_retention(
        self, s3_prefix: str, retention_count: int = DEFAULT_RETENTION_COUNT
    ) -> int:
        """
        Enforce retention policy: keep only the latest N tilesets.

        Lists all tilesets under the given prefix, sorts them (assuming lexicographic
        sorting matches chronological order), and deletes the oldest tilesets beyond
        the retention count.

        Args:
            s3_prefix: The S3 prefix for the band (e.g., "band_13/tiles")
            retention_count: Number of tilesets to keep (default: 26)

        Returns:
            Number of tilesets successfully deleted

        Raises:
            Exception: If critical errors occur during listing (deletion errors are logged)
        """
        try:
            prefixes = await self._s3_client.list_prefixes(
                f"{s3_prefix}/", delimiter="/"
            )

            tileset_prefixes = sorted(
                [p for p in prefixes if p.rstrip("/").endswith("_tiles")]
            )

            total_count = len(tileset_prefixes)

            if total_count <= retention_count:
                logger.debug(
                    "Retention policy check: %d <= %d, no action needed.",
                    total_count,
                    retention_count,
                )
                return 0

            to_delete = tileset_prefixes[:-retention_count]

            if len(to_delete) > self.MAX_DELETE_PER_PASS:
                logger.warning(
                    "Limiting deletion to %d tilesets (wanted to delete %d)",
                    self.MAX_DELETE_PER_PASS,
                    len(to_delete),
                )
                to_delete = to_delete[: self.MAX_DELETE_PER_PASS]

            logger.info(
                "Retention policy: Deleting %d old tilesets (total: %d, keeping: %d)",
                len(to_delete),
                total_count,
                retention_count,
            )

            deleted_count = 0
            for prefix in to_delete:
                try:
                    await self._s3_client.delete_prefix(prefix)
                    deleted_count += 1
                    logger.info("Deleted old tileset: %s", prefix)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.debug("Could not delete tileset %s: %s", prefix, e)

            if deleted_count > 0:
                logger.info(
                    "Retention policy: Successfully deleted %d tilesets", deleted_count
                )

            return deleted_count

        except Exception as e:
            logger.error("Failed to enforce retention policy for %s: %s", s3_prefix, e)
            raise
