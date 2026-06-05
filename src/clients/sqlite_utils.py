"""Shared SQLite connection helper for the local WAL-mode stores.

Both :class:`ProgressTracker` and :class:`MetricsRepository` open short-lived
connections to a local database file with the same settings and must close them
explicitly — sqlite3's ``with conn`` is a *transaction* manager (commit/rollback)
that does NOT close the handle.

Settings:
- ``timeout=30`` doubles as the busy timeout (wait, don't fail, on write contention).
- ``isolation_level=None`` (autocommit): every write is a single statement, so
  there is no read-then-upgrade transaction and no ``BEGIN IMMEDIATE`` deadlock.
- ``synchronous=NORMAL`` is set per-connection (it is not persisted like the WAL
  journal mode): a safe, faster setting under WAL.

The database file must live on a local volume shared by all processes on the same
host; SQLite file locking / WAL do not work over a network filesystem (NFS/SMB).
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def sqlite_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Open a short-lived autocommit/WAL connection and always close it on exit."""
    conn = sqlite3.connect(
        str(db_path),
        timeout=30.0,  # Wait up to 30s for the write lock (busy timeout)
        isolation_level=None,  # Autocommit mode
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
    finally:
        conn.close()
