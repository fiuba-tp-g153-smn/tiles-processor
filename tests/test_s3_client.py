import asyncio
import sys
import os
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from clients.s3_client import S3Client


class TestS3ClientDownloadFile:
    """Tests for S3Client.download_single_file method."""

    @pytest.mark.asyncio
    async def test_download_single_file_success(self):
        """Test successful file download on first attempt."""
        client = S3Client(bucket_name="test-bucket")
        mock_s3_client = AsyncMock()

        # Mock successful response with proper async context manager
        mock_stream = AsyncMock()
        mock_stream.read = AsyncMock(return_value=b"file content")

        mock_body = MagicMock()
        mock_body.__aenter__ = AsyncMock(return_value=mock_stream)
        mock_body.__aexit__ = AsyncMock(return_value=None)

        mock_s3_client.get_object = AsyncMock(return_value={"Body": mock_body})

        # Mock the session.client context manager
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)

        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = mock_ctx
            result = await client.download_single_file("path/to/file.nc")

        assert result == b"file content"
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="path/to/file.nc"
        )

    @pytest.mark.asyncio
    async def test_download_single_file_retry_then_success(self):
        """Test retry logic: fail twice, succeed on third attempt."""
        client = S3Client(bucket_name="test-bucket")
        mock_s3_client = AsyncMock()

        # Mock: fail twice, then succeed with proper async context manager
        mock_stream = AsyncMock()
        mock_stream.read = AsyncMock(return_value=b"success content")

        mock_body = MagicMock()
        mock_body.__aenter__ = AsyncMock(return_value=mock_stream)
        mock_body.__aexit__ = AsyncMock(return_value=None)

        mock_s3_client.get_object = AsyncMock(
            side_effect=[
                Exception("Connection timeout"),
                Exception("Connection reset"),
                {"Body": mock_body},
            ]
        )

        # Mock the session.client context manager
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with patch.object(client, "_session") as mock_session:
                mock_session.client.return_value = mock_ctx
                result = await client.download_single_file("file.nc", retries=3)

        assert result == b"success content"
        assert mock_s3_client.get_object.call_count == 3

    @pytest.mark.asyncio
    async def test_download_single_file_exhausts_retries(self):
        """Test that exhausting all retries returns None for content."""
        client = S3Client(bucket_name="test-bucket")
        mock_s3_client = AsyncMock()

        # Mock: always fail
        mock_s3_client.get_object = AsyncMock(
            side_effect=Exception("Persistent failure")
        )

        # Mock the session.client context manager
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with patch.object(client, "_session") as mock_session:
                mock_session.client.return_value = mock_ctx
                result = await client.download_single_file("file.nc", retries=3)

        assert result is None
        assert mock_s3_client.get_object.call_count == 3

    @pytest.mark.asyncio
    async def test_download_single_file_respects_semaphore(self):
        """Test that semaphore limits concurrent downloads."""
        client = S3Client(bucket_name="test-bucket", max_concurrent_downloads=2)
        mock_s3_client = AsyncMock()

        concurrent_count = 0
        max_concurrent = 0

        async def mock_get_object(*args, **kwargs):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.05)
            concurrent_count -= 1

            # Proper async context manager mock
            mock_stream = AsyncMock()
            mock_stream.read = AsyncMock(return_value=b"content")

            mock_body = MagicMock()
            mock_body.__aenter__ = AsyncMock(return_value=mock_stream)
            mock_body.__aexit__ = AsyncMock(return_value=None)

            return {"Body": mock_body}

        mock_s3_client.get_object = mock_get_object

        # Mock the session.client context manager
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)

        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = mock_ctx
            # Launch 5 concurrent downloads
            tasks = [client.download_single_file(f"file{i}.nc") for i in range(5)]
            await asyncio.gather(*tasks)

        assert max_concurrent <= 2


class TestS3ClientDownloadFolder:
    """Tests for S3Client.download_folder method."""

    @pytest.mark.asyncio
    async def test_download_folder_with_filter(self):
        """Test file filtering in download_folder."""
        client = S3Client(bucket_name="test-bucket")

        with patch.object(
            client, "_get_folder_file_paths", new_callable=AsyncMock
        ) as mock_get_paths:
            mock_get_paths.return_value = [
                "folder/file_10.nc",
                "folder/file_20.nc",
                "folder/file_30.nc",
                "folder/file_40.nc",
            ]

            with patch.object(client, "_session") as mock_session:
                mock_s3_client = AsyncMock()
                mock_body = AsyncMock()
                mock_body.read = AsyncMock(return_value=b"content")
                mock_s3_client.get_object = AsyncMock(return_value={"Body": mock_body})

                mock_session.client.return_value.__aenter__.return_value = (
                    mock_s3_client
                )

                # Filter: only files with minute >= 30
                def minute_filter(path):
                    minute = int(path.split("_")[1].split(".")[0])
                    return minute >= 30

                result = await client.download_folder(
                    "folder/", file_pattern=".nc", file_filter=minute_filter
                )

                # Should only download file_30.nc and file_40.nc
                assert len(result) == 2
                assert "folder/file_30.nc" in result
                assert "folder/file_40.nc" in result

    @pytest.mark.asyncio
    async def test_download_folder_empty_returns_empty_dict(self):
        """Test that empty folder returns empty dict."""
        client = S3Client(bucket_name="test-bucket")

        with patch.object(
            client, "_get_folder_file_paths", new_callable=AsyncMock
        ) as mock_get_paths:
            mock_get_paths.return_value = []

            result = await client.download_folder("empty/folder/", file_pattern=".nc")

            assert result == {}


class MockAsyncPaginator:
    """Helper class to mock async paginator."""

    def __init__(self, pages):
        self._pages = pages
        self._index = 0

    def paginate(self, **kwargs):
        return self

    def __aiter__(self):
        self._index = 0
        return self

    async def __anext__(self):
        if self._index >= len(self._pages):
            raise StopAsyncIteration
        page = self._pages[self._index]
        self._index += 1
        return page


class MockAsyncContextManager:
    """Helper to create async context manager for s3 client."""

    def __init__(self, s3_client):
        self._s3_client = s3_client

    async def __aenter__(self):
        return self._s3_client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class TestS3ClientGetFolderFilePaths:
    """Tests for S3Client._get_folder_file_paths method."""

    @pytest.mark.asyncio
    async def test_get_folder_file_paths_pagination(self):
        """Test that paginator handles multiple pages correctly."""
        client = S3Client(bucket_name="test-bucket")

        # Create mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_paginator.return_value = MockAsyncPaginator(
            [
                {"Contents": [{"Key": "folder/file1.nc"}, {"Key": "folder/file2.nc"}]},
                {"Contents": [{"Key": "folder/file3.nc"}]},
            ]
        )

        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = MockAsyncContextManager(mock_s3_client)

            result = await client._get_folder_file_paths("folder/", ".nc")

            assert len(result) == 3
            assert "folder/file1.nc" in result
            assert "folder/file2.nc" in result
            assert "folder/file3.nc" in result

    @pytest.mark.asyncio
    async def test_get_folder_file_paths_filters_directories(self):
        """Test that directory entries (ending with /) are filtered out."""
        client = S3Client(bucket_name="test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.get_paginator.return_value = MockAsyncPaginator(
            [
                {
                    "Contents": [
                        {"Key": "folder/"},  # Directory - should be filtered
                        {"Key": "folder/file.nc"},
                        {"Key": "folder/subdir/"},  # Directory - should be filtered
                    ]
                }
            ]
        )

        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = MockAsyncContextManager(mock_s3_client)

            result = await client._get_folder_file_paths("folder/", ".nc")

            assert len(result) == 1
            assert "folder/file.nc" in result

    @pytest.mark.asyncio
    async def test_get_folder_file_paths_pattern_matching(self):
        """Test that file pattern matching works correctly."""
        client = S3Client(bucket_name="test-bucket")

        mock_s3_client = MagicMock()
        mock_s3_client.get_paginator.return_value = MockAsyncPaginator(
            [
                {
                    "Contents": [
                        {"Key": "folder/C13_G19_file.nc"},
                        {"Key": "folder/C09_G19_file.nc"},
                        {"Key": "folder/other_file.nc"},
                    ]
                }
            ]
        )

        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = MockAsyncContextManager(mock_s3_client)

            result = await client._get_folder_file_paths("folder/", "C13_G19")

            assert len(result) == 1
            assert "folder/C13_G19_file.nc" in result
