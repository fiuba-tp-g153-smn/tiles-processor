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
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Default concurrency for the dedicated upload semaphore (separate from the
# download semaphore). Sized to match max_pool_connections so concurrent tile
# PUTs never starve the connection pool. Env-overridable via S3_UPLOAD_CONCURRENCY.
DEFAULT_UPLOAD_CONCURRENCY = 32

# Single objects at/above this size upload as parallel multipart transfers,
# streamed from disk (never the whole file in RAM). Smaller ones are one PUT.
_MULTIPART_THRESHOLD_BYTES = 8 * 1024 * 1024
# Parts in flight per large single-object transfer.
_MULTIPART_MAX_CONCURRENCY = 8

# Cap any single S3 op stalled by gateway contention at seconds, not botocore's
# 60s default read timeout (which turned contended LISTs into ~60s blocks).
# read_timeout is per-socket-read, so a progressing multipart transfer resets it
# each chunk and won't false-abort a slow-but-moving upload; 'standard' mode adds
# bounded exponential-backoff retries so a contention burst can pass before retry.
_CONNECT_TIMEOUT_S = 5
_READ_TIMEOUT_S = 30
_MAX_ATTEMPTS = 3


# Per-prefix object retention, in days. Sub-day expiries (radar 6h, GRIB 3h)
# are rounded up to the S3 lifecycle minimum of 1 day — the portability cost of
# expressing expiry as standard per-prefix bucket lifecycle rules instead of
# SeaweedFS-only per-object TTLs. S3 Filter.Prefix is a literal startswith, so
# "tiles/band_" covers band_2/9/13 and "tiles/glm_" covers fed/toe/mfa.
TILE_LIFECYCLE_RETENTION_DAYS = {
    "tiles/band_": 1,
    "cog/band_": 1,
    "tiles/glm_": 1,
    "cog/glm_": 1,
    "tiles/radar": 1,
    "cog/radar": 1,
    "tiles/wrf": 2,
    "cog/wrf": 2,
    "geojson/wrf": 2,
    "tiles/models/ecmwf": 2,
    "cog/models/ecmwf": 2,
    "geojson/models/ecmwf": 2,
    "grib/models/ecmwf": 1,
}


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
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name: S3 bucket name
            endpoint_url: S3 endpoint URL (optional, for S3-compatible services)
            max_concurrent_downloads: Maximum number of concurrent downloads
            access_key: S3 access key (optional, for authenticated access)
            secret_key: S3 secret key (optional, for authenticated access)
            upload_concurrency: Maximum number of concurrent uploads (separate
                from downloads); also sizes the connection pool.
        """
        self._bucket_name = bucket_name
        self._endpoint_url = endpoint_url
        self._max_concurrent_downloads = max_concurrent_downloads
        self._upload_concurrency = upload_concurrency
        self._semaphore = asyncio.Semaphore(self._max_concurrent_downloads)
        self._upload_semaphore = asyncio.Semaphore(self._upload_concurrency)
        self._session = aioboto3.Session()
        self._access_key = access_key
        self._secret_key = secret_key
        self._backend_label = "S3"
        self._transfer_config = TransferConfig(
            multipart_threshold=_MULTIPART_THRESHOLD_BYTES,
            max_concurrency=_MULTIPART_MAX_CONCURRENCY,
        )
        # One aioboto3 client reused across a loop's calls (warm connection
        # pool). Lazily created in _get_client and recreated if the running loop
        # changes (e.g. worker startup's throwaway loop → persistent loop). The
        # exit stack owns the client context so it can be closed in aclose().
        self._client: Any = None
        self._exit_stack: AsyncExitStack | None = None
        self._client_loop: asyncio.AbstractEventLoop | None = None
        # Serializes concurrent first-use creation; rebound per loop.
        self._client_lock: asyncio.Lock | None = None
        self._lock_loop: asyncio.AbstractEventLoop | None = None

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
    ) -> "S3Client":
        """
        Factory method to create an authenticated S3 client for S3.

        Args:
            bucket_name: Target bucket name
            endpoint: S3 endpoint (host:port, e.g., "s3-service:9000")
            access_key: Access key (username)
            secret_key: Secret key (password)
            secure: Use HTTPS (default: False)
            max_concurrent_operations: Max parallel downloads
            upload_concurrency: Max parallel uploads (also sizes the pool)
        """
        protocol = "https" if secure else "http"
        endpoint_url = f"{protocol}://{endpoint}"
        return cls(
            bucket_name=bucket_name,
            endpoint_url=endpoint_url,
            max_concurrent_downloads=max_concurrent_operations,
            access_key=access_key,
            secret_key=secret_key,
            upload_concurrency=upload_concurrency,
        )

    def _get_client_kwargs(self, authenticated: bool = False) -> dict:
        """Get kwargs for creating S3 client based on auth mode.

        Path-style addressing and a connection pool sized to this client's
        concurrency are applied on both branches: path-style is required for
        S3-compatible gateways addressed as host:port (SeaweedFS, MinIO), and
        the pool keeps concurrent operations from contending for a single
        default connection.
        """
        kwargs: dict[str, Any] = {"endpoint_url": self._endpoint_url}
        pool = max(
            self._max_concurrent_downloads,
            self._upload_concurrency,
            _MULTIPART_MAX_CONCURRENCY,
        )
        boto_kwargs: dict[str, Any] = {
            "s3": {"addressing_style": "path"},
            "max_pool_connections": pool,
            "connect_timeout": _CONNECT_TIMEOUT_S,
            "read_timeout": _READ_TIMEOUT_S,
            "retries": {"max_attempts": _MAX_ATTEMPTS, "mode": "standard"},
        }
        if authenticated and self._access_key and self._secret_key:
            kwargs["aws_access_key_id"] = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        else:
            boto_kwargs["signature_version"] = UNSIGNED
        kwargs["config"] = BotoConfig(**boto_kwargs)
        return kwargs

    def _lock_for_loop(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        """Return a creation lock bound to ``loop``, rebinding on loop change.

        Synchronous and await-free, so concurrent callers on the same loop
        observe the same lock instance (no interleave); cross-loop use is always
        sequential here, so rebinding is safe.
        """
        if self._client_lock is None or self._lock_loop is not loop:
            self._client_lock = asyncio.Lock()
            self._lock_loop = loop
        return self._client_lock

    async def _get_client(self):
        """Return a cached aioboto3 S3 client for the running loop.

        Created on first use and reused across the loop's calls (warm pool). If
        the running loop differs from the one the client was bound to, the stale
        client is dropped (it cannot be closed from another loop) and a fresh one
        is created.
        """
        loop = asyncio.get_running_loop()
        if self._client is not None and self._client_loop is loop:
            return self._client
        async with self._lock_for_loop(loop):
            if self._client is not None and self._client_loop is loop:
                return self._client
            if self._client is not None:
                logger.debug("Discarding S3 client bound to a previous event loop")
                self._client = self._exit_stack = self._client_loop = None
            # The exit stack keeps the client open beyond this call (reused
            # across the loop) and owns its eventual close in aclose().
            stack = AsyncExitStack()
            self._client = await stack.enter_async_context(
                self._session.client(
                    "s3", **self._get_client_kwargs(authenticated=True)
                )
            )
            self._exit_stack = stack
            self._client_loop = loop
            return self._client

    @asynccontextmanager
    async def _client_session(self) -> AsyncIterator[Any]:
        """Yield the reused client without closing it (close is lifecycle-owned)."""
        yield await self._get_client()

    async def aclose(self) -> None:
        """Close the cached client and its pool. Call on producer/worker shutdown.

        Only awaits the close when running on the client's own loop; otherwise
        the references are dropped (a client cannot be closed from a foreign loop).
        """
        stack = self._exit_stack
        if stack is None:
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is not None and self._client_loop is running:
            try:
                await stack.aclose()
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.debug("Error closing S3 client: %s", e)
        self._client = self._exit_stack = self._client_loop = None

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
                if downloaded is not None:
                    files[file_path] = downloaded

        logger.info(
            "Download/Cache load completed: %d/%d files available",
            len(files),
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
        """Upload a single local file via the managed transfer API.

        Large objects (COG/GRIB) upload as parallel multipart transfers streamed
        from disk — never the whole file in RAM — while small files fall back to a
        single PUT automatically. Bounded by the dedicated upload semaphore.

        Args:
            key: Destination object key (e.g., "cog/band_13/image.tif").
            file_path: Local path of the file to upload.

        Returns:
            ``True`` when upload succeeds, ``False`` when it fails.
        """
        try:
            s3_client = await self._get_client()
            async with self._upload_semaphore:
                await s3_client.upload_file(
                    str(file_path),
                    self._bucket_name,
                    key,
                    ExtraArgs={"ContentType": self._get_content_type(file_path)},
                    Config=self._transfer_config,
                )
            logger.debug(
                "Uploaded via %s (managed transfer): %s", self._backend_label, key
            )
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to upload %s to %s: %s", file_path, key, e)
            return False

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

    async def configure_lifecycle_policy(self, retention_days: int) -> bool:
        """
        Configure S3 lifecycle policy to automatically expire old objects.

        Emits one per-prefix expiration rule (see TILE_LIFECYCLE_RETENTION_DAYS)
        so each product family expires on its own schedule via portable S3 bucket
        lifecycle rules — no application-level reaper or SeaweedFS-specific TTL.

        Args:
            retention_days: Default retention, logged for reference only.
                Per-prefix retention comes from TILE_LIFECYCLE_RETENTION_DAYS.

        Returns:
            True if lifecycle policy was configured successfully

        Note:
            S3 lifecycle policies are checked periodically (typically every 24 hours),
            so objects may not be deleted exactly at the expiration time.
        """
        try:
            rules = _build_lifecycle_rules(TILE_LIFECYCLE_RETENTION_DAYS)
            async with self._client_session() as s3_client:
                await s3_client.put_bucket_lifecycle_configuration(
                    Bucket=self._bucket_name,
                    LifecycleConfiguration={"Rules": rules},  # type: ignore[arg-type]
                )

                logger.info(
                    "Configured %d per-prefix lifecycle rules for bucket '%s' "
                    "(default retention %d days)",
                    len(rules),
                    self._bucket_name,
                    retention_days,
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
