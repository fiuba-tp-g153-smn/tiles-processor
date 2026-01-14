import aioboto3
import asyncio
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
import logging
from typing import Callable, Dict, List, Optional
import os
from pathlib import Path


logger = logging.getLogger(__name__)


class S3Client:
    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str = None,
        max_concurrent_downloads: int = 6,
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
    ) -> Dict[str, bytes]:
        """
        Download files from a folder.

        Args:
            folder_path: S3 folder path
            file_pattern: Pattern to match in file names
            file_filter: Optional function to filter file paths before downloading
            local_cache_dir: Optional directory to check/store cached files
        """
        file_paths = await self._get_folder_file_paths(folder_path, file_pattern)
        
        logger.info(f"Found {len(file_paths)} files matching pattern '{file_pattern}' in {folder_path}")
        
        # Apply additional filter if provided
        if file_filter is not None:
            original_count = len(file_paths)
            file_paths = [fp for fp in file_paths if file_filter(fp)]

        files = {}
        files_to_download = []

        # Check cache if enabled
        if local_cache_dir:
            for fp in file_paths:
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
            files_to_download = file_paths

        if not files_to_download:
            return files
            
        async with self._session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            config=BotoConfig(signature_version=UNSIGNED),
        ) as s3_client:
            tasks = [self.download_file(s3_client, fp, local_cache_dir=local_cache_dir) for fp in files_to_download]
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
            async with self._session.client(
                "s3",
                endpoint_url=self._endpoint_url,
                config=BotoConfig(signature_version=UNSIGNED),
            ) as s3_client:
                logger.debug(f"Listing objects in bucket '{self._bucket_name}' with prefix '{folder_path}'")
                
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
                            
                logger.debug(f"Total files found with pattern '{file_pattern}': {len(file_paths)}")
                
        except Exception as e:
            logger.error(f"Error getting file paths in {folder_path}: {str(e)}")
            raise e

        return file_paths