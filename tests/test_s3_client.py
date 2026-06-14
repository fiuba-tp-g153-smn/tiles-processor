import asyncio
import logging
import sys
import os
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from botocore import UNSIGNED
from botocore.exceptions import ClientError
from clients.s3_client import (
    S3Client,
    TILE_LIFECYCLE_RETENTION_DAYS,
    _CONNECT_TIMEOUT_S,
    _MAX_ATTEMPTS,
    _READ_TIMEOUT_S,
    _build_lifecycle_rules,
)


class _AsyncClientContext:
    """Async context manager wrapper for a mocked S3 client."""

    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, exc_type, exc, tb):
        return False


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


class TestS3ClientDownloadToFile:
    """Tests for S3Client.download_to_file method."""

    def _make_stream(self, data: bytes, chunk_size: int = 65_536):
        """Create a mock stream that yields data in chunks via read(amt)."""
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
        chunks.append(b"")  # EOF sentinel

        mock_stream = AsyncMock()
        mock_stream.read = AsyncMock(side_effect=chunks)
        return mock_stream

    def _make_session_ctx(self, mock_s3_client):
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)
        return mock_ctx

    @pytest.mark.asyncio
    async def test_download_to_file_success(self, tmp_path):
        """Test successful streaming download writes correct content."""
        client = S3Client(bucket_name="test-bucket")
        dest = tmp_path / "output.nc"
        payload = b"A" * 100_000

        mock_s3_client = AsyncMock()
        mock_s3_client.get_object = AsyncMock(
            return_value={"Body": self._make_stream(payload, chunk_size=30_000)}
        )

        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = self._make_session_ctx(mock_s3_client)
            await client.download_to_file("path/to/file.nc", dest)

        assert dest.read_bytes() == payload
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="path/to/file.nc"
        )

    @pytest.mark.asyncio
    async def test_download_to_file_flushes_at_buffer_threshold(self, tmp_path):
        """Test that writes happen in ~20 MB buffered flushes, not per-chunk."""
        client = S3Client(bucket_name="test-bucket")
        dest = tmp_path / "output.nc"

        # 45 MB payload → should produce 2 buffer flushes + 1 final flush
        payload = b"X" * (45 * 1024 * 1024)

        mock_s3_client = AsyncMock()
        mock_s3_client.get_object = AsyncMock(
            return_value={"Body": self._make_stream(payload)}
        )

        write_sizes = []
        original_open = open

        class TrackedFile:
            def __init__(self, f):
                self._f = f

            def write(self, data):
                write_sizes.append(len(data))
                return self._f.write(data)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return self._f.__exit__(*args)

        def tracking_open(path, mode, **kwargs):
            f = original_open(path, mode, **kwargs)
            return TrackedFile(f)

        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = self._make_session_ctx(mock_s3_client)
            with patch("builtins.open", side_effect=tracking_open):
                await client.download_to_file("file.nc", dest)

        # Should have 3 writes: two ~20 MB flushes + one ~5 MB remainder
        assert len(write_sizes) == 3
        assert all(size >= 5 * 1024 * 1024 for size in write_sizes)

    @pytest.mark.asyncio
    async def test_download_to_file_retry_cleans_partial_file(self, tmp_path):
        """Test that partial file is deleted on failure before retry."""
        client = S3Client(bucket_name="test-bucket")
        dest = tmp_path / "output.nc"

        mock_s3_client = AsyncMock()
        mock_s3_client.get_object = AsyncMock(
            side_effect=[
                Exception("Connection reset"),
                {"Body": self._make_stream(b"good data")},
            ]
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with patch.object(client, "_session") as mock_session:
                mock_session.client.return_value = self._make_session_ctx(
                    mock_s3_client
                )
                await client.download_to_file("file.nc", dest, retries=2)

        assert dest.read_bytes() == b"good data"
        assert mock_s3_client.get_object.call_count == 2

    @pytest.mark.asyncio
    async def test_download_to_file_exhausts_retries(self, tmp_path):
        """Test that RuntimeError is raised after all retries are exhausted."""
        client = S3Client(bucket_name="test-bucket")
        dest = tmp_path / "output.nc"

        mock_s3_client = AsyncMock()
        mock_s3_client.get_object = AsyncMock(
            side_effect=Exception("Persistent failure")
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with patch.object(client, "_session") as mock_session:
                mock_session.client.return_value = self._make_session_ctx(
                    mock_s3_client
                )
                with pytest.raises(RuntimeError, match="Failed to download"):
                    await client.download_to_file("file.nc", dest, retries=3)

        assert not dest.exists()
        assert mock_s3_client.get_object.call_count == 3


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


class TestS3ClientUploadFile:
    """Tests for S3Client.upload_file (managed multipart transfer)."""

    @staticmethod
    def _make_client():
        s3_client = S3Client(
            bucket_name="tiles-data",
            endpoint_url="http://s3:9000",
            access_key="user",
            secret_key="pass",
        )
        boto_client = AsyncMock()
        s3_client._session.client = lambda *a, **k: _AsyncClientContext(boto_client)  # type: ignore[attr-defined]
        return s3_client, boto_client

    @pytest.mark.asyncio
    async def test_upload_file_uses_managed_transfer_with_multipart_config(
        self, tmp_path
    ):
        """upload_file routes through the managed transfer API (not put_object)."""
        file_path = tmp_path / "sample.tif"
        file_path.write_bytes(b"abc")
        s3_client, boto_client = self._make_client()

        uploaded = await s3_client.upload_file("cog/band_13/image.tif", file_path)

        assert uploaded is True
        boto_client.put_object.assert_not_awaited()
        boto_client.upload_file.assert_awaited_once()
        args, kwargs = boto_client.upload_file.call_args
        assert args[0] == str(file_path)
        assert args[1] == "tiles-data"
        assert args[2] == "cog/band_13/image.tif"
        assert kwargs["ExtraArgs"]["ContentType"] == "image/tiff"
        assert kwargs["Config"] is s3_client._transfer_config
        assert kwargs["Config"].multipart_threshold == 8 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_upload_file_returns_false_on_transfer_failure(self, tmp_path):
        """upload_file should not raise and should return False on transfer errors."""
        file_path = tmp_path / "sample.tif"
        file_path.write_bytes(b"abc")
        s3_client, boto_client = self._make_client()
        boto_client.upload_file.side_effect = RuntimeError("boom")

        uploaded = await s3_client.upload_file("cog/band_13/image.tif", file_path)

        assert uploaded is False


class TestS3ClientReuse:
    """The aioboto3 client is created once per loop and reused across calls."""

    @pytest.mark.asyncio
    async def test_client_created_once_and_reused_within_a_loop(self, tmp_path):
        file_path = tmp_path / "f.tif"
        file_path.write_bytes(b"abc")
        s3_client = S3Client(
            bucket_name="tiles-data",
            endpoint_url="http://s3:9000",
            access_key="user",
            secret_key="pass",
        )
        boto_client = AsyncMock()
        create = MagicMock(side_effect=lambda *a, **k: _AsyncClientContext(boto_client))
        s3_client._session.client = create  # type: ignore[attr-defined]

        await s3_client.upload_file("cog/a.tif", file_path)
        await s3_client.upload_file("cog/b.tif", file_path)

        assert create.call_count == 1  # one warm client across both uploads

    def test_client_recreated_on_a_different_loop(self):
        s3_client = S3Client(
            bucket_name="tiles-data",
            endpoint_url="http://s3:9000",
            access_key="user",
            secret_key="pass",
        )
        boto_client = AsyncMock()
        create = MagicMock(side_effect=lambda *a, **k: _AsyncClientContext(boto_client))
        s3_client._session.client = create  # type: ignore[attr-defined]

        asyncio.run(s3_client._get_client())
        asyncio.run(s3_client._get_client())

        assert create.call_count == 2  # loop-aware recreate


class TestS3ClientUploadConcurrency:
    """S3_UPLOAD_CONCURRENCY sizes the upload semaphore and the connection pool."""

    def test_upload_concurrency_flows_to_semaphore_and_pool(self):
        client = S3Client.create_with_credentials(
            bucket_name="tiles-data",
            endpoint="s3:9000",
            access_key="user",
            secret_key="pass",
            max_concurrent_operations=10,
            upload_concurrency=20,
        )
        assert client._upload_concurrency == 20
        assert client._upload_semaphore._value == 20
        cfg = client._get_client_kwargs(authenticated=True)["config"]
        assert cfg.max_pool_connections == 20  # max(10, 20, 8)


class TestS3ClientUploadDirectory:
    """Tests for S3Client.upload_directory method."""

    @pytest.mark.asyncio
    async def test_upload_directory_caps_error_logging(self, tmp_path, caplog):
        """A failing backend must not emit one ERROR per file (the 100k-line bug).

        Per-file failures are logged at DEBUG; upload_directory emits a single
        ERROR summary regardless of how many tiles failed.
        """
        tile_dir = tmp_path / "barbs" / "12" / "1"
        tile_dir.mkdir(parents=True)
        for i in range(5):
            (tile_dir / f"{i}.json").write_bytes(b"{}")

        s3_client = S3Client(
            bucket_name="tiles-data",
            endpoint_url="http://s3:9000",
            access_key="user",
            secret_key="pass",
        )
        boto_client = AsyncMock()
        boto_client.put_object.side_effect = RuntimeError("s3 down")
        s3_client._session.client = lambda *args, **kwargs: _AsyncClientContext(boto_client)  # type: ignore[attr-defined]

        with caplog.at_level(logging.DEBUG, logger="clients.s3_client"):
            uploaded = await s3_client.upload_directory(tmp_path, "geojson/wrf/x")

        s3_records = [r for r in caplog.records if r.name == "clients.s3_client"]
        errors = [r for r in s3_records if r.levelno == logging.ERROR]
        debug_failures = [
            r
            for r in s3_records
            if r.levelno == logging.DEBUG and "Failed to upload" in r.message
        ]

        assert uploaded == 0
        assert len(errors) == 1  # single summary, not one-per-file
        assert len(debug_failures) == 5


class TestBuildLifecycleRules:
    """Tests for the pure per-prefix lifecycle-rule builder."""

    def test_one_rule_per_prefix_and_no_empty_catchall(self):
        rules = _build_lifecycle_rules(TILE_LIFECYCLE_RETENTION_DAYS)

        assert len(rules) == len(TILE_LIFECYCLE_RETENTION_DAYS)
        prefixes = [r["Filter"]["Prefix"] for r in rules]
        assert "" not in prefixes  # R1: no empty-prefix catch-all
        assert set(prefixes) == set(TILE_LIFECYCLE_RETENTION_DAYS)

    def test_expected_prefix_to_days_mapping(self):
        days_by_prefix = {
            r["Filter"]["Prefix"]: r["Expiration"]["Days"]
            for r in _build_lifecycle_rules(TILE_LIFECYCLE_RETENTION_DAYS)
        }
        assert days_by_prefix["tiles/radar"] == 1
        assert days_by_prefix["tiles/wrf"] == 2
        assert days_by_prefix["grib/models/ecmwf"] == 1
        assert days_by_prefix["geojson/models/ecmwf"] == 2

    def test_sub_day_retention_rounds_up_to_one_and_ids_unique(self):
        rules = _build_lifecycle_rules(
            {"tiles/radar": 0, "tiles/wrf": 2, "cog/radar": -3}
        )
        assert all(r["Status"] == "Enabled" for r in rules)
        assert all(r["Expiration"]["Days"] >= 1 for r in rules)
        ids = [r["ID"] for r in rules]
        assert len(ids) == len(set(ids))

    def test_real_map_has_unique_ids_and_is_sorted(self):
        rules = _build_lifecycle_rules(TILE_LIFECYCLE_RETENTION_DAYS)
        ids = [r["ID"] for r in rules]
        prefixes = [r["Filter"]["Prefix"] for r in rules]
        assert len(ids) == len(set(ids))
        assert prefixes == sorted(prefixes)  # deterministic ordering


class TestS3ClientGetClientKwargs:
    """Tests for path-style + connection-pool addressing on both auth branches."""

    def test_authenticated_uses_path_style_pool_and_no_unsigned(self):
        client = S3Client(
            bucket_name="tiles-data",
            endpoint_url="http://s3:9000",
            max_concurrent_downloads=7,
            access_key="user",
            secret_key="pass",
            upload_concurrency=40,
        )
        kwargs = client._get_client_kwargs(authenticated=True)

        assert kwargs["aws_access_key_id"] == "user"
        assert kwargs["aws_secret_access_key"] == "pass"
        cfg = kwargs["config"]
        assert cfg.s3["addressing_style"] == "path"
        assert cfg.max_pool_connections == 40  # max(downloads=7, uploads=40, 8)
        assert cfg.signature_version != UNSIGNED
        assert cfg.connect_timeout == _CONNECT_TIMEOUT_S
        assert cfg.read_timeout == _READ_TIMEOUT_S
        assert cfg.retries == {"max_attempts": _MAX_ATTEMPTS, "mode": "standard"}

    def test_unauthenticated_uses_path_style_pool_and_unsigned(self):
        client = S3Client(
            bucket_name="noaa-goes19", max_concurrent_downloads=6, upload_concurrency=12
        )
        kwargs = client._get_client_kwargs(authenticated=False)

        assert "aws_access_key_id" not in kwargs
        cfg = kwargs["config"]
        assert cfg.s3["addressing_style"] == "path"
        assert cfg.max_pool_connections == 12  # max(downloads=6, uploads=12, 8)
        assert cfg.signature_version == UNSIGNED
        assert cfg.connect_timeout == _CONNECT_TIMEOUT_S
        assert cfg.read_timeout == _READ_TIMEOUT_S
        assert cfg.retries == {"max_attempts": _MAX_ATTEMPTS, "mode": "standard"}


class TestS3ClientHeadExists:
    """head_exists: HEAD 200 → True, 404-class → False, other errors propagate."""

    @staticmethod
    def _session_ctx(mock_s3_client):
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)
        return mock_ctx

    async def _head_exists(self, head_object_mock, key="grib/x/20260217T0000Z.grib"):
        client = S3Client(bucket_name="tiles-data")
        mock_s3_client = AsyncMock()
        mock_s3_client.head_object = head_object_mock
        with patch.object(client, "_session") as mock_session:
            mock_session.client.return_value = self._session_ctx(mock_s3_client)
            return await client.head_exists(key)

    @pytest.mark.asyncio
    async def test_head_200_returns_true(self):
        result = await self._head_exists(AsyncMock(return_value={}))
        assert result is True

    @pytest.mark.asyncio
    async def test_head_404_returns_false(self):
        err = ClientError({"Error": {"Code": "404"}}, "HeadObject")
        result = await self._head_exists(AsyncMock(side_effect=err))
        assert result is False

    @pytest.mark.asyncio
    async def test_head_other_error_propagates(self):
        err = ClientError({"Error": {"Code": "500"}}, "HeadObject")
        with pytest.raises(ClientError):
            await self._head_exists(AsyncMock(side_effect=err))
