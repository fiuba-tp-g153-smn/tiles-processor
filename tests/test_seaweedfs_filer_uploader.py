import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
import requests

from clients.seaweedfs_filer_uploader import SeaweedFsFilerUploader


def _make_uploader(**kwargs) -> SeaweedFsFilerUploader:
    defaults = dict(endpoint="seaweedfs:8888", bucket="tiles-data")
    defaults.update(kwargs)
    return SeaweedFsFilerUploader(**defaults)


class TestSessionReuse:
    """The whole point of the fix: one pooled session reused across uploads."""

    @pytest.mark.asyncio
    async def test_upload_reuses_single_session(self):
        uploader = _make_uploader()
        session_before = uploader._session
        uploader._session.put = MagicMock(return_value=MagicMock())

        for i in range(5):
            await uploader.upload(f"k/{i}.json", b"{}", "application/json")

        # Same Session instance throughout, called once per upload.
        assert uploader._session is session_before
        assert uploader._session.put.call_count == 5

    def test_adapter_pool_covers_concurrency(self):
        uploader = _make_uploader(pool_size=10, max_retries=3)
        adapter = uploader._session.get_adapter("http://seaweedfs:8888")

        assert adapter._pool_maxsize == 10
        assert adapter.max_retries.total == 3
        assert "PUT" in adapter.max_retries.allowed_methods


class TestUrlBuilding:
    """URL construction with and without the per-object TTL query param."""

    def test_build_url_with_ttl(self):
        uploader = _make_uploader(ttl="6h")
        url = uploader._build_url("geojson/wrf/x/barbs/12/1/2.json")
        assert url == (
            "http://seaweedfs:8888/buckets/tiles-data/"
            "geojson/wrf/x/barbs/12/1/2.json?ttl=6h"
        )

    def test_build_url_without_ttl(self):
        uploader = _make_uploader(ttl=None)
        url = uploader._build_url("a/b.json")
        assert url == "http://seaweedfs:8888/buckets/tiles-data/a/b.json"

    def test_build_url_secure_uses_https(self):
        uploader = _make_uploader(secure=True)
        assert uploader._build_url("a.json").startswith("https://")


class TestUploadBehavior:
    @pytest.mark.asyncio
    async def test_upload_passes_content_type_and_ttl_url(self):
        uploader = _make_uploader(ttl="1m")
        uploader._session.put = MagicMock(return_value=MagicMock())

        await uploader.upload("a/b.json", b"payload", "application/json")

        _, kwargs = uploader._session.put.call_args
        assert uploader._session.put.call_args.args[0].endswith("a/b.json?ttl=1m")
        assert kwargs["headers"] == {"Content-Type": "application/json"}
        assert kwargs["data"] == b"payload"

    @pytest.mark.asyncio
    async def test_upload_propagates_http_error(self):
        uploader = _make_uploader()
        response = MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError("500")
        uploader._session.put = MagicMock(return_value=response)

        with pytest.raises(requests.HTTPError):
            await uploader.upload("a/b.json", b"{}", "application/json")


class TestClose:
    def test_close_closes_session(self):
        uploader = _make_uploader()
        uploader._session.close = MagicMock()

        uploader.close()

        uploader._session.close.assert_called_once()
