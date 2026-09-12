"""Per-source input configuration: local folder or S3 bucket with the same layout."""

import re
from dataclasses import dataclass
from urllib.parse import urlparse

INPUT_MODE_LOCAL = "local"
INPUT_MODE_S3 = "s3"
# Upstream-API modes: a source that fetches from its own provider rather than
# from a file layout. Each is valid for exactly one source.
INPUT_MODE_OPENDATA = "opendata"  # ECMWF Open Data mirrors
INPUT_MODE_NOMADS = "nomads"  # NOAA NOMADS grib_filter CGI

S3_URI_SCHEME = "s3://"
URL_SCHEMES = ("http", "https")
ADDRESSING_STYLES = ("path", "virtual", "auto")


def has_url_scheme(endpoint: str) -> bool:
    """True when the endpoint already carries an ``http(s)://`` scheme."""
    return endpoint.lower().startswith(("http://", "https://"))


def normalize_s3_endpoint_url(endpoint: str | None, secure: bool = False) -> str | None:
    """Turn a configured endpoint into the full URL botocore expects.

    Accepts either ``host[:port][/path]`` — where ``secure`` picks the scheme —
    or an already-complete ``http(s)://host[:port][/path]`` URL, so a
    self-hosted gateway (SeaweedFS, MinIO, RustFS) can be configured exactly as
    it is written down. ``None``/empty means "no endpoint", and botocore then
    resolves the public AWS one.

    Raises:
        ValueError: the value cannot be read as an http(s) endpoint.
    """
    if not endpoint or not endpoint.strip():
        return None

    candidate = endpoint.strip()
    if any(char.isspace() for char in candidate):
        raise ValueError(f"endpoint contains whitespace: {endpoint!r}")
    scheme_match = re.match(r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://", candidate)
    if scheme_match and not has_url_scheme(candidate):
        raise ValueError(
            f"endpoint scheme must be http or https, got "
            f"'{scheme_match.group('scheme')}' in {endpoint!r}"
        )
    if not scheme_match:
        candidate = f"{'https' if secure else 'http'}://{candidate}"

    parsed = urlparse(candidate)
    if parsed.scheme not in URL_SCHEMES:
        raise ValueError(
            f"endpoint scheme must be http or https, got '{parsed.scheme}' "
            f"in {endpoint!r}"
        )
    if not parsed.netloc:
        raise ValueError(f"endpoint has no host: {endpoint!r}")
    if parsed.query or parsed.fragment:
        raise ValueError(f"endpoint must not carry a query or fragment: {endpoint!r}")
    return candidate.rstrip("/")


def split_s3_bucket_uri(value: str) -> tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)``.

    A bare bucket name passes through unchanged with an empty prefix, so both
    spellings are accepted wherever a bucket is configured.

    Raises:
        ValueError: an ``s3://`` URI with no bucket.
    """
    if not value.lower().startswith(S3_URI_SCHEME):
        return value, ""
    remainder = value[len(S3_URI_SCHEME) :].lstrip("/")
    bucket, _, prefix = remainder.partition("/")
    if not bucket:
        raise ValueError(f"s3 URI has no bucket: {value!r}")
    return bucket, prefix


def normalize_s3_prefix(prefix: str) -> str:
    """Normalize a key prefix to ``''`` or a single-trailing-slash form.

    A prefix without the trailing slash also matches sibling keys that merely
    start with the same characters (``radar`` would match ``radar_old/...``),
    so the slash is added once here instead of at every call site.
    """
    trimmed = prefix.strip().strip("/")
    return f"{trimmed}/" if trimmed else ""


@dataclass(frozen=True, slots=True)
class InputSourceConfig:
    """How a data source reads its input files.

    mode "local" reads from input_dir; mode "s3" reads from s3_bucket under
    s3_prefix, with the same folder structure as the local layout. Credentials
    come from env vars; both unset means anonymous/unsigned access (e.g. NOAA).
    """

    mode: str  # "local" | "s3"
    input_dir: str
    s3_bucket: str | None = None
    # host:port, or a complete http(s):// URL; None → AWS default endpoint
    s3_endpoint: str | None = None
    s3_prefix: str = ""
    s3_secure: bool = False
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str | None = None
    s3_addressing_style: str = "path"

    @property
    def is_s3(self) -> bool:
        """True when input is read from S3 instead of a local folder."""
        return self.mode == INPUT_MODE_S3

    @property
    def is_local(self) -> bool:
        """True when input is read from a local folder."""
        return self.mode == INPUT_MODE_LOCAL

    @property
    def endpoint_url(self) -> str | None:
        """The endpoint as a full URL, or None for the AWS default.

        Raises:
            ValueError: ``s3_endpoint`` is not a readable http(s) endpoint.
            Config validates this at startup, so callers can treat it as safe.
        """
        return normalize_s3_endpoint_url(self.s3_endpoint, self.s3_secure)
