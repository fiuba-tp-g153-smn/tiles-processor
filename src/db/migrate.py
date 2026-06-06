"""Apply Alembic migrations to the tiles-processor SQLite databases.

Each database is an independent Alembic history (a named section in
``alembic.ini``); the connection URL is injected here because the paths come from
runtime config. Used both by the ``migrate`` entrypoint mode and by the test
fixtures, so migrations are applied exactly the same way everywhere.
"""

import fcntl
import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig

from config import Config

# alembic.ini lives at the repo root (this file is src/db/migrate.py).
_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


def _enable_wal(db_path: Path) -> None:
    """Persist WAL journal mode on the file (autocommit; never inside a txn)."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    finally:
        conn.close()


def _config_for(section: str, db_path: Path) -> AlembicConfig:
    """Build an Alembic config for one named DB section with the URL injected."""
    cfg = AlembicConfig(str(_ALEMBIC_INI))
    cfg.config_ini_section = section
    cfg.set_section_option(section, "sqlalchemy.url", f"sqlite:///{db_path.resolve()}")
    return cfg


def run_migrations(metrics_db_path: Path, progress_db_path: Path) -> None:
    """Upgrade both databases to ``head``, creating parent dirs as needed."""
    for section, db_path in (
        ("metrics", metrics_db_path),
        ("progress", progress_db_path),
    ):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        command.upgrade(_config_for(section, db_path), "head")
        _enable_wal(db_path)


def run_migrations_from_config(config: Config) -> None:
    """Upgrade both databases using the paths derived from the app config."""
    run_migrations(
        Path(config.METRICS_DB_PATH),
        Path(config.TMP_DIR) / "progress_tracker.db",
    )


def ensure_migrations(config: Config) -> None:
    """Apply migrations at process startup, serialized across processes.

    A POSIX ``flock`` on a lockfile in the shared volume guarantees only one
    process migrates at a time; the rest block briefly and then ``upgrade head``
    no-ops at the stamped version. This is race-free because SQLite already pins
    every process to the same host and local volume, so a same-host advisory lock
    is exactly the right coordination primitive.
    """
    lock_path = Path(config.TMP_DIR) / ".migrate.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)  # released when the fd closes
        run_migrations_from_config(config)
