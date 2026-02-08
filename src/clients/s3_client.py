"""
S3 Client for async downloads and uploads.

Supports both:
- Unsigned access for public buckets (e.g., NOAA's noaa-goes19)
- Authenticated access for private buckets (e.g., MinIO for tile storage)
"""

import aioboto3
import asyncio
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
import logging
from typing import Callable, Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class S3Client:
    """
    Async S3 client for downloads and uploads.

    For public buckets (downloads):
        client = S3Client("noaa-goes19")

    For private buckets with auth (uploads):
        client = S3Client.create_with_credentials(
            bucket_name="tiles-data",
            endpoint="minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
        )
    """

    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str = None,
        max_concurrent_downloads: int = 6,
        access_key: str = None,
        secret_key: str = None,
        secure: bool = False,
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name: S3 bucket name
            endpoint_url: S3 endpoint URL (optional, for S3-compatible services)
            max_concurrent_downloads: Maximum number of concurrent operations
            access_key: AWS/MinIO access key (optional, for authenticated access)
            secret_key: AWS/MinIO secret key (optional, for authenticated access)
            secure: Use HTTPS (default: False for local MinIO)
        """
        self._bucket_name = bucket_name
        self._endpoint_url = endpoint_url
        self._max_concurrent_downloads = max_concurrent_downloads
        self._semaphore = asyncio.Semaphore(self._max_concurrent_downloads)
        self._session = aioboto3.Session()
        self._access_key = access_key
        self._secret_key = secret_key
        self._secure = secure

    @classmethod
    def create_with_credentials(
        cls,
        bucket_name: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        max_concurrent_operations: int = 10,
    ) -> "S3Client":
        """
        Factory method to create an authenticated S3 client for MinIO/S3.

        Args:
            bucket_name: Target bucket name
            endpoint: S3 endpoint (host:port, e.g., "minio:9000")
            access_key: Access key (username)
            secret_key: Secret key (password)
            secure: Use HTTPS (default: False)
            max_concurrent_operations: Max parallel operations
        """
        protocol = "https" if secure else "http"
        endpoint_url = f"{protocol}://{endpoint}"
        return cls(
            bucket_name=bucket_name,
            endpoint_url=endpoint_url,
            max_concurrent_downloads=max_concurrent_operations,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def _get_client_kwargs(self, authenticated: bool = False) -> dict:
        """Get kwargs for creating S3 client based on auth mode."""
        kwargs = {"endpoint_url": self._endpoint_url}
        if authenticated and self._access_key and self._secret_key:
            kwargs["aws_access_key_id"] = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        else:
            kwargs["config"] = BotoConfig(signature_version=UNSIGNED)
        return kwargs

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

        async with self._session.client(
            "s3", **self._get_client_kwargs(authenticated=True)
        ) as s3_client:
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

                    logger.info(f"Downloaded to file: {s3_key}")
                    return

                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt + 1}/{retries} failed for {s3_key}: {e}"
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
    ) -> Optional[bytes]:
        """
        Download a single file from S3 and return its content.

        This is a convenience method that handles the S3 client context internally.

        Args:
            s3_key: The S3 key (path) of the file to download
            retries: Number of retry attempts

        Returns:
            File content as bytes, or None if download failed
        """
        async with self._session.client(
            "s3", **self._get_client_kwargs(authenticated=True)
        ) as s3_client:
            _, content = await self._download_file_internal(
                s3_client, s3_key, retries=retries
            )
            return content

    async def _download_file_internal(
        self,
        s3_client,
        relative_file_path: str,
        retries: int = 3,
        local_cache_dir: Optional[Path] = None,
    ) -> tuple:
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
                        logger.info(f"✓ Downloaded and cached: {relative_file_path}")
                    else:
                        logger.info(
                            f"✓ Downloaded: {relative_file_path} ({len(content)} bytes)"
                        )

                    return relative_file_path, content
            except Exception as e:
                logger.warning(
                    f"⚠ Attempt {attempt + 1}/{retries} failed for {relative_file_path}: {str(e)}"
                )
                if attempt == retries - 1:
                    logger.error(
                        f"✗ Error downloading {relative_file_path} after {retries} attempts. Ignoring file."
                    )
                    return relative_file_path, None
                await asyncio.sleep(1)

    async def download_folder(
        self,
        folder_path: str,
        file_pattern: str = "",
        file_filter: Callable[[str], bool] = None,
        local_cache_dir: Optional[Path] = None,
        skip_if: Callable[[str], bool] = None,
    ) -> Dict[str, Optional[bytes]]:
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
            f"Found {len(file_paths)} files matching pattern '{file_pattern}' in {folder_path}"
        )

        # Apply additional filter if provided
        if file_filter is not None:
            original_count = len(file_paths)
            file_paths = [fp for fp in file_paths if file_filter(fp)]

        files = {}
        files_to_download = []

        # Check skip_if first (e.g., if tiles already exist)
        # Then check cache if enabled
        for fp in file_paths:
            # Check if we should skip this file completely (e.g. output already exists)
            if skip_if and skip_if(fp):
                logger.info(f"Skipping download for {fp}: check condition met")
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
                        logger.info(f"✓ Loaded from cache: {fp}")
                    except Exception as e:
                        logger.warning(f"Error reading from cache {cache_path}: {e}")
                        files_to_download.append(fp)
                else:
                    files_to_download.append(fp)
            else:
                files_to_download.append(fp)

        if not files_to_download:
            return files

        # Use authenticated=True so it uses credentials if available, otherwise falls back to UNSIGNED
        async with self._session.client(
            "s3", **self._get_client_kwargs(authenticated=True)
        ) as s3_client:
            tasks = [
                self._download_file_internal(
                    s3_client, fp, local_cache_dir=local_cache_dir
                )
                for fp in files_to_download
            ]
            results = await asyncio.gather(*tasks, return_exceptions=False)

            for file_path, content in results:
                if content is not None:
                    files[file_path] = content

        logger.info(
            f"Download/Cache load completed: {len(files)}/{len(file_paths)} files available"
        )
        return files

    async def _get_folder_file_paths(
        self, folder_path: str, file_pattern: str
    ) -> List[str]:
        file_paths = []
        try:
            # Use authenticated=True so it uses credentials if available
            async with self._session.client(
                "s3", **self._get_client_kwargs(authenticated=True)
            ) as s3_client:
                logger.debug(
                    f"Listing objects in bucket '{self._bucket_name}' with prefix '{folder_path}'"
                )

                # Use paginator to handle more than 1000 objects
                paginator = s3_client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=self._bucket_name, Prefix=folder_path
                ):
                    contents = page.get("Contents", [])
                    logger.debug(f"Page returned {len(contents)} objects")

                    for obj in contents:
                        key = obj["Key"]
                        if not key.endswith("/") and file_pattern in key:
                            file_paths.append(key)

                logger.debug(
                    f"Total files found with pattern '{file_pattern}': {len(file_paths)}"
                )

        except Exception as e:
            logger.error(f"Error getting file paths in {folder_path}: {str(e)}")
            raise e

        return file_paths

    # =========================================================================
    # Upload Methods (for authenticated access to MinIO/S3)
    # =========================================================================

    async def upload_directory(self, local_dir: Path, s3_prefix: str) -> int:
        """
        Upload a directory recursively to S3.

        Args:
            local_dir: Local directory path to upload
            s3_prefix: S3 key prefix (e.g., "band_13/tiles/tileset_tiles")

        Returns:
            Number of files uploaded
        """
        if not local_dir.exists():
            logger.warning(f"Directory does not exist: {local_dir}")
            return 0

        files_to_upload = []
        for file_path in local_dir.rglob("*"):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
                files_to_upload.append((file_path, s3_key))

        if not files_to_upload:
            logger.info(f"No files to upload in {local_dir}")
            return 0

        logger.info(
            f"Uploading {len(files_to_upload)} files to s3://{self._bucket_name}/{s3_prefix}"
        )

        async with self._session.client(
            "s3", **self._get_client_kwargs(authenticated=True)
        ) as s3_client:
            tasks = [
                self._upload_file_with_limit(s3_client, file_path, s3_key)
                for file_path, s3_key in files_to_upload
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        failed_count = len(results) - success_count

        if failed_count > 0:
            logger.warning(
                f"Upload completed with {failed_count} failures out of {len(results)}"
            )
        else:
            logger.info(f"Successfully uploaded {success_count} files to S3")

        return success_count

    async def _upload_file_with_limit(
        self, s3_client, file_path: Path, s3_key: str
    ) -> bool:
        """Upload a single file with semaphore-controlled concurrency."""
        async with self._semaphore:
            return await self._upload_file(s3_client, file_path, s3_key)

    async def _upload_file(self, s3_client, file_path: Path, s3_key: str) -> bool:
        """Upload a single file to S3."""
        try:
            content = await asyncio.to_thread(file_path.read_bytes)
            content_type = self._get_content_type(file_path)

            await s3_client.put_object(
                Bucket=self._bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type,
            )
            logger.debug(f"Uploaded: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {file_path} to {s3_key}: {e}")
            return False

    async def delete_prefix(self, s3_prefix: str) -> int:
        """
        Delete all objects under a given S3 prefix.

        Args:
            s3_prefix: S3 key prefix to delete (e.g., "band_13/tiles/old_tileset")

        Returns:
            Number of objects deleted
        """
        logger.info(f"Deleting objects under s3://{self._bucket_name}/{s3_prefix}")

        async with self._session.client(
            "s3", **self._get_client_kwargs(authenticated=True)
        ) as s3_client:
            objects_to_delete = []

            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self._bucket_name, Prefix=s3_prefix
            ):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if not objects_to_delete:
                logger.info(f"No objects found under {s3_prefix}")
                return 0

            # Delete in batches of 1000 (S3 limit)
            deleted_count = 0
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                await s3_client.delete_objects(
                    Bucket=self._bucket_name, Delete={"Objects": batch}
                )
                deleted_count += len(batch)

            logger.info(f"Deleted {deleted_count} objects from S3")
            return deleted_count

    async def list_prefixes(self, prefix: str, delimiter: str = "/") -> List[str]:
        """
        List common prefixes (directories) under a given prefix.

        Args:
            prefix: S3 key prefix to list under
            delimiter: Delimiter for grouping (default: "/")

        Returns:
            List of common prefixes (directory-like paths)
        """
        prefixes = []

        async with self._session.client(
            "s3", **self._get_client_kwargs(authenticated=True)
        ) as s3_client:
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
            async with self._session.client(
                "s3", **self._get_client_kwargs(authenticated=True)
            ) as s3_client:
                try:
                    await s3_client.head_bucket(Bucket=self._bucket_name)
                    logger.debug(f"Bucket '{self._bucket_name}' exists")
                    return True
                except Exception:
                    logger.info(f"Creating bucket '{self._bucket_name}'")
                    await s3_client.create_bucket(Bucket=self._bucket_name)
                    return True
        except Exception as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
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
