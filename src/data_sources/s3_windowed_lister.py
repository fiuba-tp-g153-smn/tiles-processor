"""Lists an S3 input prefix once per discovery window, optionally by partition."""

import asyncio
import logging

from botocore.exceptions import BotoCoreError, ClientError

from clients.s3_client import S3Client
from data_sources.s3_repository_utils import join_s3_prefix
from models.listing_window import ListingWindow
from models.s3_key_template import WILDCARD, expand_time_prefixes

logger = logging.getLogger(__name__)


class S3WindowedKeyLister:  # pylint: disable=too-few-public-methods
    """Returns the input files under a prefix, listing at most once per window.

    One repository serves every product source of a radar family (six for
    SINARAME, four for INTA), and they all ask for the same window in a tick:
    same tick time, same max age. Without this each one re-lists the whole
    prefix, so a bucket keeping a day of history was paginated six times every
    tick to keep the last ~2 h. The newest window's keys are kept (one entry)
    and the next tick's window replaces them; a failed listing is not kept, so
    the next source retries it.

    With ``key_template`` set (see ``s3_key_template``) only the partitions
    the window overlaps are listed first. If that listing fails or holds no
    file ending in ``key_suffix`` — a template that does not match the bucket, or
    folders named in another timezone — the whole prefix is listed instead and
    a warning says so, so a wrong template costs one extra listing rather than
    every radar file.

    ``key_suffix`` is one suffix or a tuple of them, matched in any case — the
    same contract as ``str.endswith`` (INTA ships ``.vol`` and ``.azi``).
    """

    LIST_CONCURRENCY = 8

    def __init__(
        self,
        s3_client: S3Client,
        prefix: str = "",
        key_template: str | None = None,
        *,
        key_suffix: str | tuple[str, ...],
    ) -> None:
        self._s3_client = s3_client
        self._prefix = prefix
        self._key_template = key_template
        suffixes = (key_suffix,) if isinstance(key_suffix, str) else key_suffix
        self._key_suffixes = tuple(suffix.lower() for suffix in suffixes)
        self._suffix_label = "/".join(self._key_suffixes)
        self._lock = asyncio.Lock()
        self._cached_window: ListingWindow | None = None
        self._cached_keys: list[str] = []

    async def list_keys(self, window: ListingWindow | None = None) -> list[str]:
        """Sorted keys ending in ``key_suffix`` (any case).

        ``None`` lists the whole prefix and is never cached.
        """
        if window is None:
            return await self._list_all()
        async with self._lock:
            if window != self._cached_window:
                self._cached_keys = await self._list_window(window)
                self._cached_window = window
            return list(self._cached_keys)

    async def _list_window(self, window: ListingWindow) -> list[str]:
        template = self._key_template
        if template is None:
            return await self._list_all()
        try:
            keys = await self._list_partitions(template, window)
        except (BotoCoreError, ClientError, OSError) as exc:
            reason = f"failed ({exc})"
        else:
            if keys:
                return keys
            reason = f"found no {self._suffix_label} files"
        logger.warning(
            "Template '%s' listing of '%s' for %s..%s %s; listing the whole prefix "
            "instead",
            template,
            self._prefix,
            window.start.isoformat(),
            window.end.isoformat(),
            reason,
        )
        return await self._list_all()

    async def _list_all(self) -> list[str]:
        keys = await self._s3_client.list_files(self._prefix, file_pattern="")
        return self._keep_input_files(keys)

    async def _list_partitions(self, template: str, window: ListingWindow) -> list[str]:
        expanded = [
            join_s3_prefix(self._prefix, part)
            for part in expand_time_prefixes(template, window)
        ]
        prefixes = await self._resolve_wildcards(expanded)
        keys = self._keep_input_files(await self._list_prefixes(prefixes))
        logger.info(
            "Listed %d %s files under %d partition prefixes of '%s' for %s..%s",
            len(keys),
            self._suffix_label,
            len(prefixes),
            self._prefix,
            window.start.isoformat(),
            window.end.isoformat(),
        )
        return keys

    def _keep_input_files(self, keys: list[str]) -> list[str]:
        return sorted(k for k in keys if k.lower().endswith(self._key_suffixes))

    async def _resolve_wildcards(self, prefixes: list[str]) -> list[str]:
        """Replace each ``*`` segment by the folders that exist at that level."""
        resolved: list[str] = []
        children: dict[str, list[str]] = {}
        pending = list(prefixes)
        while pending:
            prefix = pending.pop()
            head, star, tail = prefix.partition(WILDCARD)
            if not star:
                resolved.append(prefix)
                continue
            if head not in children:
                children[head] = await self._s3_client.list_prefixes(head, "/")
            pending.extend(child + tail.lstrip("/") for child in children[head])
        return sorted(resolved)

    async def _list_prefixes(self, prefixes: list[str]) -> list[str]:
        semaphore = asyncio.Semaphore(self.LIST_CONCURRENCY)

        async def list_one(prefix: str) -> list[str]:
            async with semaphore:
                return await self._s3_client.list_files(prefix, file_pattern="")

        listings = await asyncio.gather(*(list_one(p) for p in prefixes))
        return [key for listing in listings for key in listing]
