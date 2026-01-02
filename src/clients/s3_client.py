import aioboto3
import asyncio
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
import logging
from typing import Dict, List


logger = logging.getLogger(__name__)


class S3Client:
    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str = None,
        max_concurrent_downloads: int = 5,
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name: S3 bucket name
            endpoint_url: S3 endpoint URL (optional, for S3-compatible services)
            max_concurrent_downloads: Maximum number of concurrent downloads
        """
        self._bucket_name = bucket_name
        self._endpoint_url = endpoint_url
        self._max_concurrent_downloads = max_concurrent_downloads
        self._semaphore = asyncio.Semaphore(self._max_concurrent_downloads)
        self._session = aioboto3.Session()

    async def download_file(
        self, s3_client, relative_file_path: str, retries: int = 3
    ) -> tuple:
        for attempt in range(retries):
            try:
                async with self._semaphore:
                    response = await s3_client.get_object(
                        Bucket=self._bucket_name, Key=relative_file_path
                    )
                    async with response["Body"] as stream:
                        content = await stream.read()
                    logger.info(
                        f"✓ Downloaded: {relative_file_path} ({len(content)} bytes)"
                    )
                    return relative_file_path, content
            except Exception as e:
                logger.warning(
                    f"⚠ Attempt {attempt}/{retries} failed for {relative_file_path}: {str(e)}"
                )
                if attempt == retries:
                    logger.error(
                        f"✗ Error downloading {relative_file_path} after {retries} attempts. Ignoring file."
                    )
                    return relative_file_path, None
                await asyncio.sleep(1)

    async def download_folder(
        self, folder_path: str, file_pattern: str = ""
    ) -> Dict[str, bytes]:
        file_paths = await self._get_folder_file_paths(folder_path, file_pattern)
        logger.info(f"Found {len(file_paths)} files in {folder_path}")

        files = {}
        async with self._session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            config=BotoConfig(signature_version=UNSIGNED),
        ) as s3_client:
            tasks = [self.download_file(s3_client, fp) for fp in file_paths]
            results = await asyncio.gather(*tasks, return_exceptions=False)

            for file_path, content in results:
                if content is not None:
                    files[file_path] = content

        logger.info(
            f"Download completed: {len(files)}/{len(file_paths)} files downloaded successfully"
        )
        return files

    async def _get_folder_file_paths(
        self, folder_path: str, file_pattern: str
    ) -> List[str]:
        file_paths = []
        try:
            async with self._session.client(
                "s3",
                endpoint_url=self._endpoint_url,
                config=BotoConfig(signature_version=UNSIGNED),
            ) as s3_client:
                response = await s3_client.list_objects_v2(
                    Bucket=self._bucket_name, Prefix=folder_path
                )
                for obj in response.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith("/") and file_pattern in key:
                        file_paths.append(key)
        except Exception as e:
            logger.error(f"Error getting file paths in {folder_path}: {str(e)}")
            raise e

        return file_paths
