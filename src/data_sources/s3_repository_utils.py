"""Shared helpers for S3-backed file repositories."""

from fnmatch import fnmatch
from pathlib import Path


def filter_keys_by_glob(keys: list[str], filename_glob: str) -> list[str]:
    """Keep keys whose basename matches the glob; return them sorted."""
    return sorted(k for k in keys if fnmatch(Path(k).name, filename_glob))


def strip_s3_scheme(uri: str) -> str:
    """Return the bare S3 key from either a plain key or an s3://bucket/key URI."""
    if not uri.startswith("s3://"):
        return uri
    parts = uri.removeprefix("s3://").split("/", 1)
    return parts[1] if len(parts) > 1 else parts[0]
