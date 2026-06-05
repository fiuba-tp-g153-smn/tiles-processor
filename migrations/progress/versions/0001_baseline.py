"""baseline: processed_images schema

Revision ID: progress_0001
Revises:
Create Date: 2026-06-05

Idempotent baseline mirroring the schema that ``ProgressTracker._init_db`` used to
create. It creates ``processed_images`` + its index only when the table is absent,
so ``alembic upgrade head`` adopts a pre-existing database (no ``alembic_version``
table) by stamping this revision instead of failing on a duplicate table.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "progress_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if sa.inspect(bind).has_table("processed_images"):
        return  # adopt an existing database: just stamp this revision

    op.create_table(
        "processed_images",
        sa.Column("image_id", sa.Text, nullable=False),
        sa.Column("band_id", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("image_id", "band_id"),
    )
    op.create_index("idx_created_at", "processed_images", ["created_at"])


def downgrade() -> None:
    op.drop_table("processed_images")
