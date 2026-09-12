"""Shared helpers for S3-backed file repositories."""

from fnmatch import fnmatch
from pathlib import Path

from models.input_source_config import S3_URI_SCHEME


def filter_keys_by_glob(keys: list[str], filename_glob: str) -> list[str]:
    """Keep keys whose basename matches the glob; return them sorted."""
    return sorted(k for k in keys if fnmatch(Path(k).name, filename_glob))


def join_s3_prefix(prefix: str, path: str) -> str:
    """Join a configured key prefix with a path below it.

    Both halves may or may not carry slashes; the result never starts with one
    and never doubles one, so it is usable directly as a LIST prefix.
    """
    tail = path.lstrip("/")
    head = prefix.strip("/")
    return f"{head}/{tail}" if head else tail


def strip_s3_scheme(uri: str, expected_bucket: str | None = None) -> str:
    """Return the bare S3 key from either a plain key or an ``s3://bucket/key`` URI.

    An ``S3Client`` is bound to one bucket, so a URI naming a *different* bucket
    cannot be honoured. Passing ``expected_bucket`` turns that into an error
    instead of silently reading the same key out of the configured bucket.

    Raises:
        ValueError: the URI names a bucket other than ``expected_bucket``.
    """
    if not uri.lower().startswith(S3_URI_SCHEME):
        return uri
    remainder = uri[len(S3_URI_SCHEME) :]
    bucket, _, key = remainder.partition("/")
    if expected_bucket is not None and bucket != expected_bucket:
        raise ValueError(
            f"S3 URI {uri!r} names bucket '{bucket}', but this repository reads "
            f"from '{expected_bucket}'"
        )
    return key or bucket
