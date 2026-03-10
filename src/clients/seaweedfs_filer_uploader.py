"""SeaweedFS Filer uploader — uploads tiles via the Filer REST API with per-object TTL."""

import asyncio

import requests


class SeaweedFsFilerUploader:  # pylint: disable=too-few-public-methods
    """
    Uploads tiles to SeaweedFS via the Filer REST API.

    The Filer REST API accepts a `?ttl=` query parameter that bakes a TTL
    into the volume assignment at creation time — the only per-file TTL
    mechanism SeaweedFS provides.

    Args:
        endpoint: Filer host:port (e.g. "seaweedfs:8888")
        bucket: Bucket (top-level Filer directory) to upload into
        ttl: TTL string in SeaweedFS format ("1m", "1h", "1d", …).
             Pass None to upload without TTL.
        secure: Use HTTPS instead of HTTP.
    """

    def __init__(
        self,
        endpoint: str,
        bucket: str,
        ttl: str | None = None,
        secure: bool = False,
    ) -> None:
        self._endpoint = endpoint
        self._bucket = bucket
        self._ttl = ttl
        self._secure = secure

    async def upload(self, key: str, content: bytes, content_type: str) -> None:
        """Upload a single tile to SeaweedFS via the Filer REST API."""
        protocol = "https" if self._secure else "http"
        ttl_param = f"?ttl={self._ttl}" if self._ttl else ""
        url = f"{protocol}://{self._endpoint}/buckets/{self._bucket}/{key}{ttl_param}"

        def _put() -> None:
            response = requests.put(
                url,
                data=content,
                headers={"Content-Type": content_type},
                timeout=30,
            )
            response.raise_for_status()

        await asyncio.to_thread(_put)
