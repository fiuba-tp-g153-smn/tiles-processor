# Database migrations (Alembic)

The two SQLite stores are schema-managed by Alembic:

| Section (`-n`) | Database                                               | Tables             |
| -------------- | ------------------------------------------------------ | ------------------ |
| `metrics`      | `${METRICS_DB_PATH}` (default `${TMP_DIR}/metrics.db`) | `job_metrics`      |
| `progress`     | `${TMP_DIR}/progress_tracker.db`                       | `processed_images` |

Each is an **independent history** (its own `versions/`, `env.py` and
`alembic_version` table). The connection URL is **injected at runtime** by
`src/db/migrate.py` (the DB paths are dynamic), so `alembic.ini` leaves
`sqlalchemy.url` blank.

The repositories (`MetricsRepository`, `ProgressTracker`) no longer create their
own schema — Alembic is the single source of truth. Migrations are applied by:

- **production / compose:** the one-shot `migrate` service runs
  `python3 src/main.py migrate` (→ `upgrade head` for both DBs) before the
  producer / workers / dashboard start (`depends_on … service_completed_successfully`);
- **locally:** `python3 src/main.py migrate`;
- **tests:** the `migrated_dbs` fixture in `tests/conftest.py`.

Baselines are **idempotent** (they skip table creation if it already exists), so
running against a pre-existing database just stamps the baseline revision — no
manual `alembic stamp` needed.

## Add a migration (e.g. a new column, backfilling every existing row)

1. Author a revision (no DB connection needed to create the file):

   ```bash
   alembic -c alembic.ini -n metrics revision -m "add foo"
   ```

2. Edit the generated file in `migrations/metrics/versions/`. The body is free —
   any SQL and any Python computation:

   ```python
   def upgrade() -> None:
       op.add_column("job_metrics", sa.Column("foo", sa.Float()))
       # backfill ALL existing rows with a calculation:
       op.execute("UPDATE job_metrics SET foo = total_s - download_s WHERE foo IS NULL")
       # …or compute per-row in Python:
       #   bind = op.get_bind()
       #   for row in bind.execute(sa.text("SELECT id, ... FROM job_metrics")):
       #       bind.execute(sa.text("UPDATE job_metrics SET foo = :v WHERE id = :id"),
       #                    {"v": compute(row), "id": row.id})

   def downgrade() -> None:
       op.drop_column("job_metrics", "foo")
   ```

3. `black migrations/ --check` (Black runs on these files), then apply with
   `python3 src/main.py migrate`. The next deploy runs it automatically.

### SQLite note

Plain `ADD COLUMN` works directly. Other changes (drop/rename/retype a column)
need `op.batch_alter_table(...)` (SQLite rebuilds the table); `render_as_batch=True`
is already set in `env.py`. Also: SQLite `ADD COLUMN` defaults must be constant,
and you can't add a `NOT NULL` column without a default.
