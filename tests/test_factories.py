import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from config import Config
from factories import create_s3_client


@pytest.fixture
def config_with_filer(monkeypatch):
    monkeypatch.setenv("SEAWEEDFS_FILER_ENDPOINT", "http://filer:8888")
    monkeypatch.setenv("SEAWEEDFS_TILE_TTL", "72h")
    return Config()


class TestCreateS3Client:
    def test_with_ttl_true_passes_configured_ttl(self, config_with_filer):
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer, with_ttl=True)
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] == config_with_filer.SEAWEEDFS_TILE_TTL

    def test_with_ttl_false_passes_none(self, config_with_filer):
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer, with_ttl=False)
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] is None

    def test_default_with_ttl_is_true(self, config_with_filer):
        with patch("factories.S3Client.create_with_credentials") as mock_create:
            mock_create.return_value = MagicMock()
            with patch("factories.SeaweedFsFilerUploader") as mock_uploader_cls:
                create_s3_client(config_with_filer)
                _, kwargs = mock_uploader_cls.call_args
                assert kwargs["ttl"] == config_with_filer.SEAWEEDFS_TILE_TTL
