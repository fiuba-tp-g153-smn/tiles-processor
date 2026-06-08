"""SeaweedFS Filer uploader — uploads tiles via the Filer REST API with per-object TTL."""

import asyncio

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SeaweedFsFilerUploader:  # pylint: disable=too-few-public-methods
    """
    Uploads tiles to SeaweedFS via the Filer REST API.

    The Filer REST API accepts a `?ttl=` query parameter that bakes a TTL
    into the volume assignment at creation time — the only per-file TTL
    mechanism SeaweedFS provides.

    A single keep-alive ``requests.Session`` is built once and reused for every
    upload, so the thousands of tiny tiles a product emits share a small pool of
    pooled TCP connections instead of opening (and leaking into TIME_WAIT) a
    fresh socket per file — which otherwise exhausts the host's ephemeral ports.

    Args:
        endpoint: Filer host:port (e.g. "seaweedfs:8888")
        bucket: Bucket (top-level Filer directory) to upload into
        ttl: TTL string in SeaweedFS format ("1m", "1h", "1d", …).
             Pass None to upload without TTL.
        secure: Use HTTPS instead of HTTP.
        pool_size: Max pooled connections; keep >= the caller's upload
            concurrency so connections are reused rather than discarded.
        max_retries: Connection/5xx retry budget per upload (PUT is idempotent).
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        endpoint: str,
        bucket: str,
        ttl: str | None = None,
        secure: bool = False,
        pool_size: int = 10,
        max_retries: int = 3,
    ) -> None:
        self._endpoint = endpoint
        self._bucket = bucket
        self._ttl = ttl
        self._secure = secure
        self._session = self._build_session(pool_size, max_retries)

    @staticmethod
    def _build_session(pool_size: int, max_retries: int) -> requests.Session:
        """Build a keep-alive session whose pool covers the upload concurrency."""
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=0,
            status=2,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset({"PUT"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=retry,
            pool_block=True,
        )
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _build_url(self, key: str) -> str:
        """Construct the Filer REST URL for a tile key, with optional TTL."""
        protocol = "https" if self._secure else "http"
        ttl_param = f"?ttl={self._ttl}" if self._ttl else ""
        return f"{protocol}://{self._endpoint}/buckets/{self._bucket}/{key}{ttl_param}"

    async def upload(self, key: str, content: bytes, content_type: str) -> None:
        """Upload a single tile to SeaweedFS via the Filer REST API."""
        url = self._build_url(key)

        def _put() -> None:
            # Per-request headers (never session.headers) and no cookies, so
            # sharing this Session across asyncio.to_thread worker threads is
            # safe: urllib3's connection pool is the thread-safe part.
            response = self._session.put(
                url, data=content, headers={"Content-Type": content_type}, timeout=30
            )
            response.raise_for_status()

        await asyncio.to_thread(_put)

    def close(self) -> None:
        """Release pooled connections. Instance lifetime = subprocess lifetime."""
        self._session.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # pylint: disable=broad-exception-caught
            pass
