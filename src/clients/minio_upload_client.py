"""
MinIO S3 Upload Client.

Provides async upload functionality for tiles to a MinIO S3 bucket.
Used by tiles-processor to upload generated tile directories after processing.
"""

import aioboto3
import asyncio
import logging
import os
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


class MinioUploadClient:
    """
    Async MinIO upload client for tile storage.

    Uploads tile directories to MinIO S3 bucket with concurrency control.
    Supports recursive directory uploads and object deletion for cleanup.

    Attributes:
        _endpoint: MinIO endpoint URL (e.g., "minio:9000")
        _access_key: MinIO access key
        _secret_key: MinIO secret key
        _bucket: Target bucket name
        _secure: Whether to use HTTPS
        _max_concurrent_uploads: Maximum parallel uploads
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
        max_concurrent_uploads: int = 10,
    ):
        self._endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._secure = secure
        self._max_concurrent_uploads = max_concurrent_uploads
        self._semaphore = asyncio.Semaphore(max_concurrent_uploads)
        self._session = aioboto3.Session()

    def _get_endpoint_url(self) -> str:
        protocol = "https" if self._secure else "http"
        return f"{protocol}://{self._endpoint}"

    async def upload_directory(
        self,
        local_dir: Path,
        s3_prefix: str,
    ) -> int:
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
            f"Uploading {len(files_to_upload)} files to s3://{self._bucket}/{s3_prefix}"
        )

        async with self._session.client(
            "s3",
            endpoint_url=self._get_endpoint_url(),
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
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
                Bucket=self._bucket,
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
            s3_prefix: S3 key prefix to delete (e.g., "band_13/tiles/old_tileset_tiles")

        Returns:
            Number of objects deleted
        """
        logger.info(f"Deleting objects under s3://{self._bucket}/{s3_prefix}")

        async with self._session.client(
            "s3",
            endpoint_url=self._get_endpoint_url(),
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as s3_client:
            objects_to_delete = []

            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=s3_prefix):
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
                    Bucket=self._bucket, Delete={"Objects": batch}
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
            "s3",
            endpoint_url=self._get_endpoint_url(),
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as s3_client:
            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self._bucket, Prefix=prefix, Delimiter=delimiter
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
                "s3",
                endpoint_url=self._get_endpoint_url(),
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            ) as s3_client:
                try:
                    await s3_client.head_bucket(Bucket=self._bucket)
                    logger.debug(f"Bucket '{self._bucket}' exists")
                    return True
                except Exception:
                    logger.info(f"Creating bucket '{self._bucket}'")
                    await s3_client.create_bucket(Bucket=self._bucket)
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
