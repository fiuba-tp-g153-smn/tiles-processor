"""
S3 Client for async downloads and uploads.

Supports both:
- Unsigned access for public buckets (e.g., NOAA's noaa-goes19)
- Authenticated access for private buckets (e.g., s3 for tile storage)
"""

import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import aioboto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from clients.heavy_upload import etag_failure_reason, file_md5, validated_size
from models.input_source_config import normalize_s3_endpoint_url

logger = logging.getLogger(__name__)


# Default concurrency for the dedicated upload semaphore (separate from the
# download semaphore). Sized to match max_pool_connections so concurrent tile
# PUTs never starve the connection pool. Env-overridable via S3_UPLOAD_CONCURRENCY.
DEFAULT_UPLOAD_CONCURRENCY = 32

# Heavy objects (COG/GRIB/GeoJSON) upload as ONE PUT, streamed from disk, never
# as a multipart transfer — see clients/heavy_upload.py for why that is
# load-bearing rather than a performance choice, and for the guards. Keep this
# module free of TransferConfig.
#
# The lane is split by size so a handful of 46 MiB GRIBs cannot head-of-line
# block thousands of sub-MiB COGs sharing the client. Sizing rationale and the
# measured distribution behind the 6 MiB boundary live with the tunables, in
# Config.S3_LARGE_OBJECT_THRESHOLD_MB.
DEFAULT_HEAVY_UPLOAD_CONCURRENCY = 16
DEFAULT_LARGE_UPLOAD_CONCURRENCY = 4
DEFAULT_LARGE_OBJECT_THRESHOLD_MB = 6

# Cap any single S3 op stalled by gateway contention at seconds, not botocore's
# 60s default read timeout (which turned contended LISTs into ~60s blocks).
# 'standard' mode adds bounded exponential-backoff retries so a contention burst
# can pass before retry; a heavy PUT gets its own longer budget on its own client
# (see DEFAULT_HEAVY_READ_TIMEOUT_S) because read_timeout is per-client.
_CONNECT_TIMEOUT_S = 5
_READ_TIMEOUT_S = 30
DEFAULT_HEAVY_READ_TIMEOUT_S = 120
_MAX_ATTEMPTS = 3

# Client profiles. Two boto clients, so the tile lane keeps its 30 s response
# budget while a heavy PUT gets a longer one and a private connection pool.
_PROFILE_DEFAULT = "default"
_PROFILE_HEAVY = "heavy"


# Per-prefix object retention, in days. Sub-day expiries (radar 6h, GRIB 3h)
# are rounded up to the S3 lifecycle minimum of 1 day — the portability cost of
# expressing expiry as standard per-prefix bucket lifecycle rules instead of
# SeaweedFS-only per-object TTLs. S3 Filter.Prefix is a literal startswith, so
# "tiles/band_" covers band_2/9/13 and "tiles/glm_" covers fed/toe/mfa.
def _build_lifecycle_rules(retention_map: dict[str, int]) -> list[dict]:
    """Build one non-overlapping S3 lifecycle rule per explicit prefix.

    No empty-prefix catch-all: overlap-resolution semantics differ across
    AWS/MinIO/SeaweedFS, and every uploader writes under one of the enumerated
    prefixes. Rules are sorted by prefix for deterministic output.
    """
    return [
        {
            "ID": f"expire-{prefix.replace('/', '-')}",
            "Status": "Enabled",
            "Expiration": {"Days": max(1, days)},
            "Filter": {"Prefix": prefix},
        }
        for prefix, days in sorted(retention_map.items())
    ]


class S3Client:
    """
    Async S3 client for downloads and uploads.

    For public buckets (downloads):
        client = S3Client("noaa-goes19")

    For private buckets with auth (uploads):
        client = S3Client.create_with_credentials(
            bucket_name="tiles-data",
            endpoint="s3-service:9000",
            access_key="s3admin",
            secret_key="s3admin",
        )
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        bucket_name: str,
        endpoint_url: str | None = None,
        max_concurrent_downloads: int = 6,
        access_key: str | None = None,
        secret_key: str | None = None,
        upload_concurrency: int = DEFAULT_UPLOAD_CONCURRENCY,
        heavy_upload_concurrency: int = DEFAULT_HEAVY_UPLOAD_CONCURRENCY,
        heavy_read_timeout_s: int = DEFAULT_HEAVY_READ_TIMEOUT_S,
        large_upload_concurrency: int = DEFAULT_LARGE_UPLOAD_CONCURRENCY,
        large_object_threshold_mb: int = DEFAULT_LARGE_OBJECT_THRESHOLD_MB,
        region_name: str | None = None,
        addressing_style: str = "path",
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name: S3 bucket name
            endpoint_url: Full S3 endpoint URL (optional, for S3-compatible
                services). Callers holding a ``host:port`` string should pass it
                through ``normalize_s3_endpoint_url`` first.
            max_concurrent_downloads: Maximum number of concurrent downloads
            access_key: S3 access key (optional, for authenticated access)
            secret_key: S3 secret key (optional, for authenticated access)
            upload_concurrency: Maximum number of concurrent tile uploads
                (separate from downloads); also sizes the connection pool.
            heavy_upload_concurrency: Maximum number of concurrent heavy
                (COG/GRIB/GeoJSON) uploads below the large-object threshold.
            heavy_read_timeout_s: Response wait for a heavy PUT, on the heavy
                client only.
            large_upload_concurrency: Maximum number of concurrent uploads at or
                above the large-object threshold, so a few very large bodies
                cannot head-of-line block the many small ones.
            large_object_threshold_mb: Size at which an upload moves to the
                large lane.
            region_name: AWS region for endpoint resolution and SigV4 signing.
                None lets botocore resolve it (us-east-1 by default), which is
                right for gateways that ignore the region and for us-east-1
                buckets, but wrong for an authenticated bucket in any other
                region — the signature would be built for the wrong one.
            addressing_style: "path" (default) suits every S3-compatible gateway
                addressed as host:port; "virtual"/"auto" are for endpoints that
                only answer on <bucket>.<host>.
        """
        self._bucket_name = bucket_name
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._addressing_style = addressing_style
        self._max_concurrent_downloads = max_concurrent_downloads
        self._upload_concurrency = upload_concurrency
        self._heavy_upload_concurrency = heavy_upload_concurrency
        self._heavy_read_timeout_s = heavy_read_timeout_s
        self._large_upload_concurrency = large_upload_concurrency
        self._large_object_threshold_bytes = large_object_threshold_mb * 1024 * 1024
        self._semaphore = asyncio.Semaphore(self._max_concurrent_downloads)
        self._upload_semaphore = asyncio.Semaphore(self._upload_concurrency)
        # Two gates inside the heavy lane, bounded independently of the tile
        # lane's: sub-threshold bodies get width, at-or-above ones get a narrow
        # gate so they cannot monopolise the gateway (see the size note above).
        self._heavy_upload_semaphore = asyncio.Semaphore(self._heavy_upload_concurrency)
        self._large_upload_semaphore = asyncio.Semaphore(self._large_upload_concurrency)
        self._session = aioboto3.Session()
        self._access_key = access_key
        self._secret_key = secret_key
        self._backend_label = "S3"
        # One aioboto3 client per profile, reused across a loop's calls (warm
        # connection pool). Lazily created in _get_client and recreated if the
        # running loop changes (e.g. worker startup's throwaway loop → persistent
        # loop). Each exit stack owns its client so aclose() can close them all.
        self._clients: dict[str, Any] = {}
        self._exit_stacks: dict[str, AsyncExitStack] = {}
        self._client_loops: dict[str, asyncio.AbstractEventLoop] = {}
        # Serializes concurrent first-use creation; rebound per loop, per profile.
        self._client_locks: dict[str, asyncio.Lock] = {}
        self._lock_loops: dict[str, asyncio.AbstractEventLoop] = {}

    @property
    def bucket_name(self) -> str:
        """The single bucket this client reads and writes."""
        return self._bucket_name

    @classmethod
    def create_with_credentials(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        cls,
        bucket_name: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        max_concurrent_operations: int = 10,
        upload_concurrency: int = DEFAULT_UPLOAD_CONCURRENCY,
        heavy_upload_concurrency: int = DEFAULT_HEAVY_UPLOAD_CONCURRENCY,
        heavy_read_timeout_s: int = DEFAULT_HEAVY_READ_TIMEOUT_S,
        large_upload_concurrency: int = DEFAULT_LARGE_UPLOAD_CONCURRENCY,
        large_object_threshold_mb: int = DEFAULT_LARGE_OBJECT_THRESHOLD_MB,
        region_name: str | None = None,
        addressing_style: str = "path",
    ) -> "S3Client":
        """
        Factory method to create an authenticated S3 client for S3.

        Args:
            bucket_name: Target bucket name
            endpoint: S3 endpoint, either "host:port" (``secure`` picks the
                scheme) or a complete "http(s)://host[:port][/path]" URL
            access_key: Access key (username)
            secret_key: Secret key (password)
            secure: Use HTTPS (default: False)
            max_concurrent_operations: Max parallel downloads
            upload_concurrency: Max parallel tile uploads (also sizes the pool)
            heavy_upload_concurrency: Max parallel sub-threshold heavy uploads
            heavy_read_timeout_s: Response wait for a heavy PUT
            large_upload_concurrency: Max parallel at-or-above-threshold uploads
            large_object_threshold_mb: Size at which the large lane takes over
            region_name: AWS region, or None to let botocore resolve one
            addressing_style: "path", "virtual" or "auto"
        """
        endpoint_url = normalize_s3_endpoint_url(endpoint, secure)
        return cls(
            bucket_name=bucket_name,
            endpoint_url=endpoint_url,
            max_concurrent_downloads=max_concurrent_operations,
            access_key=access_key,
            secret_key=secret_key,
            upload_concurrency=upload_concurrency,
            heavy_upload_concurrency=heavy_upload_concurrency,
            large_upload_concurrency=large_upload_concurrency,
            large_object_threshold_mb=large_object_threshold_mb,
            heavy_read_timeout_s=heavy_read_timeout_s,
            region_name=region_name,
            addressing_style=addressing_style,
        )

    def _get_client_kwargs(
        self, authenticated: bool = False, profile: str = _PROFILE_DEFAULT
    ) -> dict:
        """Get kwargs for creating S3 client based on auth mode.

        The configured addressing style and a connection pool sized to this
        client's concurrency are applied on both branches: path-style (the
        default) is required for S3-compatible gateways addressed as host:port
        (SeaweedFS, MinIO), and the pool keeps concurrent operations from
        contending for a single default connection.

        The heavy profile differs only in pool size and read timeout: a
        multi-MiB PUT needs a longer response budget than the 30 s the tile lane
        wants, and botocore fixes read_timeout per client.
        """
        heavy = profile == _PROFILE_HEAVY
        kwargs: dict[str, Any] = {"endpoint_url": self._endpoint_url}
        if self._region_name:
            kwargs["region_name"] = self._region_name
        pool = (
            # Both heavy sub-lanes share this client, so the pool covers both.
            self._heavy_upload_concurrency + self._large_upload_concurrency
            if heavy
            else max(self._max_concurrent_downloads, self._upload_concurrency)
        )
        boto_kwargs: dict[str, Any] = {
            "s3": {"addressing_style": self._addressing_style},
            "max_pool_connections": max(1, pool),
            "connect_timeout": _CONNECT_TIMEOUT_S,
            "read_timeout": self._heavy_read_timeout_s if heavy else _READ_TIMEOUT_S,
            "retries": {"max_attempts": _MAX_ATTEMPTS, "mode": "standard"},
        }
        if authenticated and self._access_key and self._secret_key:
            kwargs["aws_access_key_id"] = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        else:
            boto_kwargs["signature_version"] = UNSIGNED
        kwargs["config"] = BotoConfig(**boto_kwargs)
        return kwargs

    def _lock_for_loop(
        self, loop: asyncio.AbstractEventLoop, profile: str = _PROFILE_DEFAULT
    ) -> asyncio.Lock:
        """Return a creation lock bound to ``loop``, rebinding on loop change.

        Synchronous and await-free, so concurrent callers on the same loop
        observe the same lock instance (no interleave); cross-loop use is always
        sequential here, so rebinding is safe.
        """
        if (
            profile not in self._client_locks
            or self._lock_loops.get(profile) is not loop
        ):
            self._client_locks[profile] = asyncio.Lock()
            self._lock_loops[profile] = loop
        return self._client_locks[profile]

    async def _get_client(self, profile: str = _PROFILE_DEFAULT):
        """Return a cached aioboto3 S3 client for the running loop.

        Created on first use per profile and reused across the loop's calls
        (warm pool). If the running loop differs from the one the client was
        bound to, the stale client is dropped (it cannot be closed from another
        loop) and a fresh one is created.
        """
        loop = asyncio.get_running_loop()
        cached = self._clients.get(profile)
        if cached is not None and self._client_loops.get(profile) is loop:
            return cached
        async with self._lock_for_loop(loop, profile):
            cached = self._clients.get(profile)
            if cached is not None and self._client_loops.get(profile) is loop:
                return cached
            if cached is not None:
                logger.debug(
                    "Discarding S3 client (%s) bound to a previous event loop",
                    profile,
                )
                self._drop_client(profile)
            # The exit stack keeps the client open beyond this call (reused
            # across the loop) and owns its eventual close in aclose().
            stack = AsyncExitStack()
            client = await stack.enter_async_context(
                self._session.client(
                    "s3",
                    **self._get_client_kwargs(authenticated=True, profile=profile),
                )
            )
            self._clients[profile] = client
            self._exit_stacks[profile] = stack
            self._client_loops[profile] = loop
            return client

    def _drop_client(self, profile: str) -> None:
        """Forget a profile's client without closing it (foreign-loop safe)."""
        self._clients.pop(profile, None)
        self._exit_stacks.pop(profile, None)
        self._client_loops.pop(profile, None)

    @asynccontextmanager
    async def _client_session(
        self, profile: str = _PROFILE_DEFAULT
    ) -> AsyncIterator[Any]:
        """Yield the reused client without closing it (close is lifecycle-owned)."""
        yield await self._get_client(profile)

    async def aclose(self) -> None:
        """Close the cached client and its pool. Call on producer/worker shutdown.

        Only awaits the close when running on the client's own loop; otherwise
        the references are dropped (a client cannot be closed from a foreign loop).
        """
        if not self._exit_stacks:
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        for profile in list(self._exit_stacks):
            stack = self._exit_stacks[profile]
            if running is not None and self._client_loops.get(profile) is running:
                try:
                    await stack.aclose()
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.debug("Error closing S3 client (%s): %s", profile, e)
            self._drop_client(profile)

    async def __aenter__(self) -> "S3Client":
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.aclose()

    async def download_to_file(
        self,
        s3_key: str,
        dest_path: Path,
        retries: int = 3,
    ) -> None:
        """
        Stream-download an S3 object to a local file with buffered writes.

        Avoids loading the entire file into memory. Chunks are accumulated
        in a 20 MB buffer before flushing to disk to reduce I/O syscalls.

        Args:
            s3_key: The S3 key (path) of the file to download
            dest_path: Local file path to write to
            retries: Number of retry attempts

        Raises:
            RuntimeError: If download fails after all retries
        """
        flush_size = 20 * 1024 * 1024  # 20 MB
        read_chunk = 65_536  # 64 KB

        async with self._client_session() as s3_client:
            for attempt in range(retries):
                try:
                    async with self._semaphore:
                        response = await s3_client.get_object(
                            Bucket=self._bucket_name, Key=s3_key
                        )
                        stream = response["Body"]
                        buffer = bytearray()
                        with open(dest_path, "wb") as f:
                            while True:
                                chunk = await stream.read(read_chunk)
                                if not chunk:
                                    break
                                buffer.extend(chunk)
                                if len(buffer) >= flush_size:
                                    f.write(buffer)
                                    buffer.clear()
                            if buffer:
                                f.write(buffer)
                                buffer.clear()

                    logger.info("Downloaded to file: %s", s3_key)
                    return

                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Attempt %d/%d failed for %s: %s",
                        attempt + 1,
                        retries,
                        s3_key,
                        e,
                    )
                    if dest_path.exists():
                        dest_path.unlink()
                    if attempt == retries - 1:
                        raise RuntimeError(
                            f"Failed to download {s3_key} after {retries} attempts"
                        ) from e
                    await asyncio.sleep(1)

    async def download_single_file(
        self,
        s3_key: str,
        retries: int = 3,
    ) -> bytes | None:
        """
        Download a single file from S3 and return its content.

        This is a convenience method that handles the S3 client context internally.

        Args:
            s3_key: The S3 key (path) of the file to download
            retries: Number of retry attempts

        Returns:
            File content as bytes, or None if download failed
        """
        async with self._client_session() as s3_client:
            _, content = await self._download_file_internal(
                s3_client, s3_key, retries=retries
            )
            return content

    async def _download_file_internal(
        self,
        s3_client,
        relative_file_path: str,
        retries: int = 3,
        local_cache_dir: Path | None = None,
    ) -> tuple[str, bytes | None]:
        for attempt in range(retries):
            try:
                async with self._semaphore:
                    response = await s3_client.get_object(
                        Bucket=self._bucket_name, Key=relative_file_path
                    )
                    async with response["Body"] as stream:
                        content = await stream.read()

                    if local_cache_dir:
                        file_name = Path(relative_file_path).name
                        cache_path = local_cache_dir / file_name
                        # Write to cache asynchronously
                        await asyncio.to_thread(cache_path.write_bytes, content)
                        logger.info("✓ Downloaded and cached: %s", relative_file_path)
                    else:
                        logger.info(
                            "✓ Downloaded: %s (%d bytes)",
                            relative_file_path,
                            len(content),
                        )

                    return relative_file_path, content
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.warning(
                    "⚠ Attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    retries,
                    relative_file_path,
                    str(e),
                )
                if attempt == retries - 1:
                    logger.error(
                        "✗ Error downloading %s after %d attempts. Ignoring file.",
                        relative_file_path,
                        retries,
                    )
                    return relative_file_path, None
                await asyncio.sleep(1)
        return relative_file_path, None

    async def download_folder(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        folder_path: str,
        file_pattern: str = "",
        file_filter: Callable[[str], bool] | None = None,
        local_cache_dir: Path | None = None,
        skip_if: Callable[[str], bool] | None = None,
    ) -> dict[str, bytes | None]:
        """
        Download files from a folder.

        Args:
            folder_path: S3 folder path
            file_pattern: Pattern to match in file names
            file_filter: Optional function to filter file paths before downloading
            local_cache_dir: Optional directory to check/store cached files
            skip_if: Optional function to skip download if true (returns None for content)

        Returns:
            One entry per matched key. A key mapped to ``None`` was either
            skipped by ``skip_if`` or failed to download (each hard failure is
            logged at WARNING); a key mapped to ``bytes`` downloaded/cached
            successfully. Callers must not assume every entry has content.
        """
        file_paths = await self._get_folder_file_paths(folder_path, file_pattern)

        logger.info(
            "Found %d files matching pattern '%s' in %s",
            len(file_paths),
            file_pattern,
            folder_path,
        )

        # Apply additional filter if provided
        if file_filter is not None:
            file_paths = [fp for fp in file_paths if file_filter(fp)]

        files: dict[str, bytes | None] = {}
        files_to_download = []

        # Check skip_if first (e.g., if tiles already exist)
        # Then check cache if enabled
        for fp in file_paths:
            # Check if we should skip this file completely (e.g. output already exists)
            if skip_if and skip_if(fp):
                logger.info("Skipping download for %s: check condition met", fp)
                files[fp] = None
                continue

            if local_cache_dir:
                file_name = Path(fp).name
                cache_path = local_cache_dir / file_name
                if cache_path.exists():
                    try:
                        # Read from cache asynchronously
                        content = await asyncio.to_thread(cache_path.read_bytes)
                        files[fp] = content
                        logger.info("✓ Loaded from cache: %s", fp)
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        logger.warning("Error reading from cache %s: %s", cache_path, e)
                        files_to_download.append(fp)
                else:
                    files_to_download.append(fp)
            else:
                files_to_download.append(fp)

        if not files_to_download:
            return files

        # Use authenticated=True so it uses credentials if available,
        # otherwise falls back to UNSIGNED
        async with self._client_session() as s3_client:
            tasks = [
                self._download_file_internal(
                    s3_client, fp, local_cache_dir=local_cache_dir
                )
                for fp in files_to_download
            ]
            results: list[tuple[str, bytes | None]] = list(
                await asyncio.gather(*tasks, return_exceptions=False)
            )

            for file_path, downloaded in results:
                if downloaded is None:
                    logger.warning(
                        "Download failed for %s; recording it as unavailable "
                        "(None) so the caller sees the gap",
                        file_path,
                    )
                files[file_path] = downloaded

        available = sum(1 for content in files.values() if content is not None)
        logger.info(
            "Download/Cache load completed: %d/%d files available",
            available,
            len(file_paths),
        )
        return files

    async def head_exists(self, key: str) -> bool:
        """Return True if the exact object exists (HEAD 200), False on 404.

        Direct O(1) existence check — avoids a prefix LIST + filter when the
        caller already knows the full key (and avoids a heavy scan that competes
        with concurrent uploads). Non-404 errors propagate so a transient gateway
        failure is never silently read as "missing".
        """
        async with self._client_session() as s3_client:
            try:
                await s3_client.head_object(Bucket=self._bucket_name, Key=key)
                return True
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code in ("404", "NoSuchKey", "NotFound"):
                    return False
                raise

    async def list_files(self, folder_path: str, file_pattern: str) -> list[str]:
        """
        List files in an S3 folder matching a pattern.

        Args:
            folder_path: S3 folder path prefix
            file_pattern: Substring to match in file keys

        Returns:
            List of matching S3 keys
        """
        return await self._get_folder_file_paths(folder_path, file_pattern)

    async def _get_folder_file_paths(
        self, folder_path: str, file_pattern: str
    ) -> list[str]:
        file_paths = []
        try:
            # Use authenticated=True so it uses credentials if available
            async with self._client_session() as s3_client:
                logger.debug(
                    "Listing objects in bucket '%s' with prefix '%s'",
                    self._bucket_name,
                    folder_path,
                )

                # Use paginator to handle more than 1000 objects
                paginator = s3_client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=self._bucket_name, Prefix=folder_path
                ):
                    contents = page.get("Contents", [])
                    logger.debug("Page returned %d objects", len(contents))

                    for obj in contents:
                        key = obj["Key"]
                        if not key.endswith("/") and file_pattern in key:
                            file_paths.append(key)

                logger.debug(
                    "Total files found with pattern '%s': %d",
                    file_pattern,
                    len(file_paths),
                )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error getting file paths in %s: %s", folder_path, str(e))
            raise

        return file_paths

    # =========================================================================
    # Upload Methods (for authenticated access to S3)
    # =========================================================================

    async def upload_directory(self, local_dir: Path, s3_prefix: str) -> int:
        """
        Upload a directory recursively to S3.

        Args:
            local_dir: Local directory path to upload
            s3_prefix: S3 key prefix (e.g., "tiles/band_13/tileset_id")

        Returns:
            Number of files uploaded
        """
        if not local_dir.exists():
            logger.warning("Directory does not exist: %s", local_dir)
            return 0

        files_to_upload = []
        for file_path in local_dir.rglob("*"):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
                files_to_upload.append((file_path, s3_key))

        if not files_to_upload:
            logger.info("No files to upload in %s", local_dir)
            return 0

        logger.info(
            "Uploading %d files via %s to %s/%s",
            len(files_to_upload),
            self._backend_label,
            self._bucket_name,
            s3_prefix,
        )

        async with self._client_session() as s3_client:
            tasks = [
                self._upload_file_with_limit(s3_client, file_path, s3_key)
                for file_path, s3_key in files_to_upload
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        failed_count = len(results) - success_count

        if failed_count > 0:
            logger.error(
                "Upload to %s/%s completed with %d failures out of %d "
                "(per-file errors at DEBUG)",
                self._bucket_name,
                s3_prefix,
                failed_count,
                len(results),
            )
        else:
            logger.info(
                "Successfully uploaded %d files via %s",
                success_count,
                self._backend_label,
            )

        return success_count

    async def _upload_file_with_limit(
        self, s3_client, file_path: Path, s3_key: str
    ) -> bool:
        """Upload a single file bounded by the dedicated upload semaphore."""
        async with self._upload_semaphore:
            return await self._upload_file(s3_client, file_path, s3_key)

    async def _upload_file(self, s3_client, file_path: Path, s3_key: str) -> bool:
        """Upload a single file via S3 put_object."""
        try:
            content = await asyncio.to_thread(file_path.read_bytes)
            content_type = self._get_content_type(file_path)

            await s3_client.put_object(
                Bucket=self._bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type,
            )

            logger.debug("Uploaded via %s: %s", self._backend_label, s3_key)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            # Per-file detail at DEBUG only: a transient backend outage can fail
            # thousands of tile uploads; upload_directory logs a single summary.
            logger.debug("Failed to upload %s to %s: %s", file_path, s3_key, e)
            return False

    async def upload_file(self, key: str, file_path: Path) -> bool:
        """Upload one heavy local file (COG/GRIB/GeoJSON) as a single PUT.

        Streamed from disk — never the whole file in RAM — and deliberately
        never multipart: on SeaweedFS only the PutObject path resolves the
        bucket's lifecycle Expiration.Days rule into a volume TTL, so a
        multipart object would be written with no expiry at all. The response
        ETag is verified against the body's MD5, which both checks integrity and
        proves the object did not go multipart (a multipart ETag is
        ``<hex32>-<parts>``). Bounded by whichever heavy sub-lane its size
        selects (see ``_lane_for``).

        Args:
            key: Destination object key (e.g., "cog/band_13/image.tif").
            file_path: Local path of the file to upload.

        Returns:
            ``True`` when upload succeeds and verifies, ``False`` otherwise.
        """
        try:
            size = await asyncio.to_thread(validated_size, file_path)
            content_md5, md5_hex = await asyncio.to_thread(file_md5, file_path)
            etag = await self._put_streaming(key, file_path, content_md5, size)
            return self._verify_heavy_etag(key, etag, md5_hex, size)
        except Exception as e:  # pylint: disable=broad-exception-caught
            # ERROR, not DEBUG: unlike one tile among thousands, a dropped COG
            # is a product the visualizer and point-value reads depend on.
            logger.error(
                "Heavy upload failed: %s -> %s/%s (%s: %s)",
                file_path,
                self._bucket_name,
                key,
                type(e).__name__,
                e,
            )
            return False

    def _lane_for(self, size: int) -> tuple[asyncio.Semaphore, str]:
        """Pick the heavy sub-lane for ``size``; returns (gate, label for logs).

        At or above the threshold an object takes the narrow lane, so a burst of
        very large bodies cannot starve the many small ones sharing the client.
        """
        if size >= self._large_object_threshold_bytes:
            return self._large_upload_semaphore, "large"
        return self._heavy_upload_semaphore, "heavy"

    async def _put_streaming(
        self, key: str, file_path: Path, content_md5: str, size: int
    ) -> str | None:
        """PUT ``file_path`` as one streamed request; return the response ETag.

        The body is an open file handle, so botocore streams it rather than
        buffering, and can rewind it for its own bounded retries. ``ContentMD5``
        is supplied so no checksum pass has to re-read the body on the loop.
        """
        s3_client = await self._get_client(_PROFILE_HEAVY)
        gate, lane = self._lane_for(size)
        logger.debug("Heavy upload %s via the %s lane (%d bytes)", key, lane, size)
        async with gate:
            handle = await asyncio.to_thread(file_path.open, "rb")
            try:
                response = await s3_client.put_object(
                    Bucket=self._bucket_name,
                    Key=key,
                    Body=handle,
                    ContentType=self._get_content_type(file_path),
                    ContentMD5=content_md5,
                )
            finally:
                await asyncio.to_thread(handle.close)
        return response.get("ETag")

    def _verify_heavy_etag(
        self, key: str, etag: str | None, md5_hex: str, size: int
    ) -> bool:
        """Log and report whether the object landed as a single, intact PUT."""
        reason = etag_failure_reason(etag, md5_hex)
        if reason is not None:
            logger.error("Heavy upload of %s %s", key, reason)
            return False
        logger.debug(
            "Uploaded via %s (single PUT, %d bytes): %s",
            self._backend_label,
            size,
            key,
        )
        return True

    async def delete_prefix(self, s3_prefix: str) -> int:
        """
        Delete all objects under a given S3 prefix.

        Args:
            s3_prefix: S3 key prefix to delete (e.g., "tiles/band_13/old_tileset")

        Returns:
            Number of objects deleted
        """
        logger.info("Deleting objects under s3://%s/%s", self._bucket_name, s3_prefix)

        async with self._client_session() as s3_client:
            objects_to_delete = []

            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self._bucket_name, Prefix=s3_prefix
            ):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if not objects_to_delete:
                logger.info("No objects found under %s", s3_prefix)
                return 0

            # Delete in batches of 1000 (S3 limit)
            deleted_count = 0
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                await s3_client.delete_objects(
                    Bucket=self._bucket_name,
                    Delete={"Objects": batch},  # type: ignore[typeddict-item]
                )
                deleted_count += len(batch)

            logger.info("Deleted %d objects from S3", deleted_count)
            return deleted_count

    async def list_prefixes(self, prefix: str, delimiter: str = "/") -> list[str]:
        """
        List common prefixes (directories) under a given prefix.

        Args:
            prefix: S3 key prefix to list under
            delimiter: Delimiter for grouping (default: "/")

        Returns:
            List of common prefixes (directory-like paths)
        """
        prefixes = []

        async with self._client_session() as s3_client:
            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self._bucket_name, Prefix=prefix, Delimiter=delimiter
            ):
                for common_prefix in page.get("CommonPrefixes", []):
                    prefixes.append(common_prefix["Prefix"])

        return prefixes

    async def ensure_bucket_exists(self) -> bool:
        """
        Ensure the target bucket exists, creating it if necessary.

        Returns:
            True if bucket exists or was created successfully
        """
        try:
            async with self._client_session() as s3_client:
                try:
                    await s3_client.head_bucket(Bucket=self._bucket_name)
                    logger.debug("Bucket '%s' exists", self._bucket_name)
                    return True
                except Exception:  # pylint: disable=broad-exception-caught
                    logger.info("Creating bucket '%s'", self._bucket_name)
                    await s3_client.create_bucket(Bucket=self._bucket_name)
                    return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Failed to ensure bucket exists: %s", e)
            return False

    async def configure_lifecycle_policy(self, retention_map: dict[str, int]) -> bool:
        """
        Configure S3 lifecycle policy to automatically expire old objects.

        Emits one expiration rule per prefix in ``retention_map`` (resolved from
        each source's ``retention_days`` in settings.json), so each product
        family expires on its own schedule via portable S3 bucket lifecycle
        rules — no application-level reaper, no SeaweedFS-specific TTL, and no
        empty-prefix catch-all (overlap semantics differ across S3 backends).

        Args:
            retention_map: ``{S3 key-prefix: retention days}``. Every uploader
                writes under one of these prefixes.

        Returns:
            True if lifecycle policy was configured successfully

        Note:
            S3 lifecycle policies are checked periodically (typically every 24 hours),
            so objects may not be deleted exactly at the expiration time.
        """
        try:
            rules = _build_lifecycle_rules(retention_map)
            async with self._client_session() as s3_client:
                await s3_client.put_bucket_lifecycle_configuration(
                    Bucket=self._bucket_name,
                    LifecycleConfiguration={"Rules": rules},  # type: ignore[arg-type]
                )

                logger.info(
                    "Configured %d per-prefix lifecycle rules for bucket '%s'",
                    len(rules),
                    self._bucket_name,
                )
                return True

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to configure lifecycle policy for bucket '%s': %s",
                self._bucket_name,
                e,
            )
            return False

    @staticmethod
    def _get_content_type(file_path: Path) -> str:
        """Get MIME type for a file based on extension."""
        extension = file_path.suffix.lower()
        content_types = {
            ".webp": "image/webp",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".html": "text/html",
            ".json": "application/json",
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
        }
        return content_types.get(extension, "application/octet-stream")
