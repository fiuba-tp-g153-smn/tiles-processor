"""baseline: job_metrics schema

Revision ID: metrics_0001
Revises:
Create Date: 2026-06-05

Idempotent baseline mirroring the schema that ``MetricsRepository._init_db`` used
to create. It creates ``job_metrics`` + indexes only when the table is absent, so
``alembic upgrade head`` adopts a pre-existing database (one created by the old
``CREATE TABLE IF NOT EXISTS`` path, with no ``alembic_version`` table) by simply
stamping this revision rather than failing on a duplicate table.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "metrics_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if sa.inspect(bind).has_table("job_metrics"):
        return  # adopt an existing database: just stamp this revision

    op.create_table(
        "job_metrics",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("work_unit_id", sa.Text),
        sa.Column("image_id", sa.Text, nullable=False),
        sa.Column("data_source_id", sa.Text, nullable=False),
        sa.Column("processor_id", sa.Text),
        sa.Column("band_id", sa.Text),
        sa.Column("job_type", sa.Text, nullable=False),
        sa.Column("product_label", sa.Text),
        sa.Column("image_timestamp", sa.Text),
        sa.Column("outcome", sa.Text, nullable=False),
        sa.Column("worker_host", sa.Text),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("finished_at", sa.Text, nullable=False),
        sa.Column("retry_count", sa.Integer, server_default="0"),
        sa.Column("error_message", sa.Text),
        sa.Column("download_s", sa.Float),
        sa.Column("process_s", sa.Float),
        sa.Column("total_s", sa.Float),
        sa.Column("stage_timings_json", sa.Text),
        sqlite_autoincrement=True,
    )
    op.create_index(
        "idx_metrics_type_finished", "job_metrics", ["job_type", "finished_at"]
    )
    op.create_index("idx_metrics_finished", "job_metrics", ["finished_at"])
    op.create_index("idx_metrics_outcome", "job_metrics", ["outcome"])


def downgrade() -> None:
    op.drop_table("job_metrics")
