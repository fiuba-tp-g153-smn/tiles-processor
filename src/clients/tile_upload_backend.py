"""Protocol interface for tile upload backends."""

from typing import Protocol


class TileUploadBackend(Protocol):  # pylint: disable=too-few-public-methods
    """Structural interface for tile upload backends."""

    async def upload(self, key: str, content: bytes, content_type: str) -> None:
        """Upload a single tile."""
