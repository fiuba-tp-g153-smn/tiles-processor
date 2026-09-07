"""Pure guards for single-PUT (heavy) object uploads.

Kept free of client state so each rule is testable on its own, and so the
invariant they enforce is readable without the surrounding transport code:
a heavy object must land as ONE PutObject, intact.

That matters beyond correctness. SeaweedFS resolves an S3 lifecycle
``Expiration.Days`` rule into a volume TTL only on the PutObject path — upstream
deferred ``UploadPart``/``CompleteMultipartUpload`` in seaweedfs PR #9377 and
never landed the follow-up — so a multipart-uploaded object is stored with no
expiry and never returns its volume slots. In Sept 2026 that accumulated
141.8 GiB of immortal COGs and exhausted the cluster's 900 volume slots,
failing 100 % of writes.
"""

import hashlib
import re
from base64 import b64encode
from pathlib import Path

from exceptions import EmptyUploadError, UploadTooLargeError

# S3 caps a single PUT at 5 GiB. Above that an object is undeliverable without
# multipart, so we fail loudly rather than silently store one with no TTL.
MAX_SINGLE_PUT_BYTES = 5 * 1024 * 1024 * 1024

# Read size for off-loop hashing of a heavy body.
HASH_CHUNK_BYTES = 1024 * 1024

# A multipart-completed object's ETag is "<md5-of-part-md5s>-<part count>"; a
# single PUT's is a bare MD5. The suffix is standard across AWS/MinIO/SeaweedFS,
# which makes it a backend-portable signal that a write took the multipart path
# and therefore carries no lifecycle-derived TTL.
_MULTIPART_ETAG_RE = re.compile(r"-\d+$")


def is_multipart_etag(etag: str) -> bool:
    """True when ``etag`` has the ``-<parts>`` suffix of a multipart upload."""
    return bool(_MULTIPART_ETAG_RE.search(etag))


def validated_size(file_path: Path) -> int:
    """Return the file size, rejecting inputs a single PUT must not carry."""
    size = file_path.stat().st_size
    if size == 0:
        raise EmptyUploadError(f"refusing to upload 0-byte file: {file_path}")
    if size > MAX_SINGLE_PUT_BYTES:
        raise UploadTooLargeError(
            f"{file_path} is {size} bytes, above the {MAX_SINGLE_PUT_BYTES} "
            "single-PUT limit; multipart is disabled because it would write "
            "the object with no lifecycle TTL"
        )
    return size


def file_md5(file_path: Path) -> tuple[str, str]:
    """Hash ``file_path``. Returns ``(base64 for ContentMD5, hex for the ETag)``.

    Callers run this off the event loop: it reads the whole body, in chunks.
    """
    digest = hashlib.md5(usedforsecurity=False)
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return b64encode(digest.digest()).decode("ascii"), digest.hexdigest()


def etag_failure_reason(etag: str | None, md5_hex: str) -> str | None:
    """Why the response ETag is unacceptable, or ``None`` when it is fine.

    Checked on every heavy upload: a silent multipart fallback is precisely how
    the Sept 2026 leak went unnoticed for 24 days. ETag == MD5 holds for
    single-PUT objects without SSE; under SSE-KMS/SSE-C the integrity arm of
    this check would need to become conditional.
    """
    normalized = (etag or "").strip('"')
    if is_multipart_etag(normalized):
        return (
            f"returned a MULTIPART ETag ({normalized}): the object carries NO "
            "lifecycle TTL and will never expire. Multipart must stay disabled."
        )
    if normalized and normalized != md5_hex:
        return f"failed integrity check: ETag {normalized} != MD5 {md5_hex}"
    return None
