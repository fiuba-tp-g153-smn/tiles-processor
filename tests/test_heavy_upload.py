"""Guards for single-PUT heavy uploads, tested without any transport."""

import os
import sys
from base64 import b64encode
from hashlib import md5

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from clients.heavy_upload import (
    MAX_SINGLE_PUT_BYTES,
    etag_failure_reason,
    file_md5,
    is_multipart_etag,
    validated_size,
)
from exceptions import EmptyUploadError, UploadTooLargeError


class TestIsMultipartEtag:
    """A "-<parts>" suffix marks the path that skips the lifecycle TTL."""

    @pytest.mark.parametrize("etag", ["a" * 32 + "-2", "b" * 32 + "-6", "x-10"])
    def test_detects_multipart(self, etag):
        assert is_multipart_etag(etag) is True

    @pytest.mark.parametrize("etag", ["a" * 32, "", "deadbeef", "abc-def"])
    def test_ignores_single_put(self, etag):
        assert is_multipart_etag(etag) is False


class TestValidatedSize:
    def test_returns_size_for_a_normal_object(self, tmp_path):
        p = tmp_path / "cog.tif"
        p.write_bytes(b"x" * 2048)
        assert validated_size(p) == 2048

    def test_rejects_empty_file(self, tmp_path):
        """A 0-byte COG means the generating step failed silently."""
        p = tmp_path / "empty.tif"
        p.write_bytes(b"")
        with pytest.raises(EmptyUploadError):
            validated_size(p)

    def test_rejects_above_the_single_put_ceiling(self, tmp_path, monkeypatch):
        """Beyond 5 GiB only multipart could deliver it, and multipart has no TTL."""
        p = tmp_path / "huge.grib2"
        p.write_bytes(b"x")
        monkeypatch.setattr(
            type(p),
            "stat",
            lambda _self: type("S", (), {"st_size": MAX_SINGLE_PUT_BYTES + 1})(),
        )
        with pytest.raises(UploadTooLargeError):
            validated_size(p)


class TestFileMd5:
    def test_matches_hashlib_in_both_encodings(self, tmp_path):
        body = b"heavy-cog-bytes" * 1000
        p = tmp_path / "cog.tif"
        p.write_bytes(body)

        b64, hexd = file_md5(p)

        assert hexd == md5(body, usedforsecurity=False).hexdigest()
        assert b64 == b64encode(md5(body, usedforsecurity=False).digest()).decode()

    def test_streams_bodies_larger_than_one_chunk(self, tmp_path):
        """Chunked reads must produce the same digest as a single read."""
        body = os.urandom(3 * 1024 * 1024 + 17)
        p = tmp_path / "big.tif"
        p.write_bytes(body)

        assert file_md5(p)[1] == md5(body, usedforsecurity=False).hexdigest()


class TestEtagFailureReason:
    MD5 = "a" * 32

    def test_accepts_a_matching_bare_etag(self):
        assert etag_failure_reason(f'"{self.MD5}"', self.MD5) is None

    def test_accepts_a_missing_etag(self):
        """Backends that omit the header are not treated as a failure."""
        assert etag_failure_reason(None, self.MD5) is None
        assert etag_failure_reason("", self.MD5) is None

    def test_flags_multipart_before_integrity(self):
        """Multipart is the important diagnosis even when the hash also differs."""
        reason = etag_failure_reason(f'"{self.MD5}-4"', self.MD5)
        assert reason is not None and "MULTIPART" in reason
        assert "never expire" in reason

    def test_flags_a_mismatched_etag(self):
        reason = etag_failure_reason('"' + "b" * 32 + '"', self.MD5)
        assert reason is not None and "integrity" in reason
