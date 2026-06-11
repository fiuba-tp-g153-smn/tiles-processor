"""Per-source input configuration: local folder or S3 bucket with the same layout."""

from dataclasses import dataclass

INPUT_MODE_LOCAL = "local"
INPUT_MODE_S3 = "s3"


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
    s3_endpoint: str | None = None  # host:port; None → AWS default endpoint
    s3_prefix: str = ""
    s3_secure: bool = False
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    @property
    def is_s3(self) -> bool:
        """True when input is read from S3 instead of a local folder."""
        return self.mode == INPUT_MODE_S3
