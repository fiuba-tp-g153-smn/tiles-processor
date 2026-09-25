"""relabel radar job types by network instead of by the last station

Revision ID: metrics_0003
Revises: metrics_0002
Create Date: 2026-09-25

A radar job type (``radar_sinarame_dbzh``) spans every station of its network,
and the dashboard labels each job-type row with its latest job. Labels used to
carry that job's station (``Radar RMA9 zdr · …``), so a row read as if it were
one radar; since 2026-09-25 they name the network (``Radar SINARAME zdr · …``).
Rows written before that deploy keep the old wording, and a job type whose
newest row is old still shows a station on the dashboard.

This rewrites every radar row onto the current label, so each job type reads
the same whichever row is newest. The station is not lost: it stays in
``image_id`` (``RMA9_zdr_20260925T113040Z``), which is what the job detail shows.

The mapping is frozen here on purpose, as in metrics_0002: a migration has to
keep doing the same thing forever, so it must not import the config registries.
Job types outside it (a product added later) are left alone.

``downgrade`` is a no-op: the old label only restated the station already kept
in ``image_id``, and nothing reads it back.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "metrics_0003"
down_revision: Union[str, None] = "metrics_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Product id -> long name, as the labeler wrote them on 2026-09-25.
_SINARAME = {
    "dbzh": "Horizontal Reflectivity",
    "dbzh-450km": "Horizontal Reflectivity (450 km)",
    "vrad": "Radial Velocity",
    "rhohv": "Cross-correlation Coefficient",
    "zdr": "Differential Reflectivity",
    "kdp": "Specific Differential Phase",
}
_INTA = {
    "dbzh": "Horizontal Reflectivity",
    "zdr": "Differential Reflectivity",
    "rhohv": "Cross-correlation Coefficient",
    "kdp": "Specific Differential Phase",
}

# (job-type prefix, network name, products)
_NETWORKS = (
    ("radar_sinarame_", "SINARAME", _SINARAME),
    ("radar_inta_", "INTA", _INTA),
)

_RELABEL = sa.text(
    "UPDATE job_metrics SET product_label = :label "
    "WHERE job_type = :job_type AND product_label IS NOT :label"
)

_REQUIRED_COLUMNS = frozenset(("job_type", "product_label"))


def _labels() -> list[dict[str, str]]:
    """One ``{job_type, label}`` per radar product, in the current wording."""
    return [
        {
            "job_type": f"{prefix}{product_id}",
            "label": f"Radar {network} {product_id} · {long_name}",
        }
        for prefix, network, products in _NETWORKS
        for product_id, long_name in products.items()
    ]


def upgrade() -> None:
    bind = op.get_bind()
    columns = {col["name"] for col in sa.inspect(bind).get_columns("job_metrics")}
    if not _REQUIRED_COLUMNS <= columns:
        return  # a pre-Alembic table without labels has nothing to relabel
    for params in _labels():
        bind.execute(_RELABEL, params)


def downgrade() -> None:
    """No-op: the station the old label carried is still in ``image_id``."""
