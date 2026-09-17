"""rename the pre-refactor product ids still stored in job_metrics

Revision ID: metrics_0002
Revises: metrics_0001
Create Date: 2026-09-17

``job_type`` is the work unit's ``data_source_id`` frozen at write time, and the
explicit-product-naming refactor renamed every source id (live 2026-09-14).
Rows recorded before that deploy therefore keep the old ids, so the dashboard
lists each product twice — idle under its old id, live under the new one — with
the history split between them.

This rewrites those rows onto the current ids, merging each product's history
back into one entry. ``image_id`` is deliberately left alone: it is the upstream
file's own name (e.g. ``RMA9_DBZH_20260914T193000Z``), not an id we renamed.

The mapping is frozen here on purpose. A migration has to keep doing the same
thing forever, so it must not import the config registries, which keep moving.

``downgrade`` is a no-op: once renamed, these rows are indistinguishable from the
ones written natively under the current ids, so splitting them again would be
guesswork.
"""

from dataclasses import dataclass
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "metrics_0002"
down_revision: Union[str, None] = "metrics_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


@dataclass(frozen=True)
class _Rename:
    """One product's legacy → current ids.

    ``job_type`` and ``data_source_id`` are set outright (they are always equal).
    The other three are ``(old, new)`` substring swaps, because they embed the
    product token inside a longer value: ``glm_fed`` (per GLM band),
    ``Radar RMA9 DBZH · …`` (per radar station). ``None`` leaves the column as is.
    """

    legacy: str
    current: str
    processor: Union[tuple[str, str], None] = None
    band: Union[tuple[str, str], None] = None
    label: Union[tuple[str, str], None] = None


_RENAMES: tuple[_Rename, ...] = (
    _Rename(
        legacy="goes19_abi_band_13",
        current="goes19_abi_c13",
        processor=("goes_band_13", "goes19_abi_c13"),
        band=("band_13", "goes19_abi_c13"),
        label=("GOES ABI band_13 · ", "GOES-19 ABI c13 · "),
    ),
    _Rename(
        legacy="goes19_abi_band_9",
        current="goes19_abi_c09",
        processor=("goes_band_9", "goes19_abi_c09"),
        band=("band_9", "goes19_abi_c09"),
        label=("GOES ABI band_9 · ", "GOES-19 ABI c09 · "),
    ),
    _Rename(
        legacy="goes19_abi_band_2",
        current="goes19_abi_c02",
        processor=("goes_band_2", "goes19_abi_c02"),
        band=("band_2", "goes19_abi_c02"),
        label=("GOES ABI band_2 · ", "GOES-19 ABI c02 · "),
    ),
    # GLM: one job emits FED/TOE/MFA, so the band/processor suffix varies.
    _Rename(
        legacy="glm_folder",
        current="goes19_glm",
        processor=("glm_", "goes19_glm_"),
        band=("glm_folder_", "goes19_glm_"),
    ),
    _Rename(
        legacy="radar_DBZH",
        current="radar_sinarame_dbzh",
        processor=("radar", "radar_sinarame"),
        band=("radar_DBZH", "radar_sinarame_dbzh"),
        label=(" DBZH · ", " dbzh · "),
    ),
    _Rename(
        legacy="radar_DBZH_450KM",
        current="radar_sinarame_dbzh-450km",
        processor=("radar", "radar_sinarame"),
        band=("radar_DBZH_450KM", "radar_sinarame_dbzh-450km"),
        label=(" DBZH_450KM · ", " dbzh-450km · "),
    ),
    _Rename(
        legacy="radar_KDP",
        current="radar_sinarame_kdp",
        processor=("radar", "radar_sinarame"),
        band=("radar_KDP", "radar_sinarame_kdp"),
        label=(" KDP · ", " kdp · "),
    ),
    _Rename(
        legacy="radar_RHOHV",
        current="radar_sinarame_rhohv",
        processor=("radar", "radar_sinarame"),
        band=("radar_RHOHV", "radar_sinarame_rhohv"),
        label=(" RHOHV · ", " rhohv · "),
    ),
    _Rename(
        legacy="radar_VRAD",
        current="radar_sinarame_vrad",
        processor=("radar", "radar_sinarame"),
        band=("radar_VRAD", "radar_sinarame_vrad"),
        label=(" VRAD · ", " vrad · "),
    ),
    _Rename(
        legacy="radar_ZDR",
        current="radar_sinarame_zdr",
        processor=("radar", "radar_sinarame"),
        band=("radar_ZDR", "radar_sinarame_zdr"),
        label=(" ZDR · ", " zdr · "),
    ),
    _Rename(
        legacy="wrf_AguaPrecipitable",
        current="wrf_arg4k_agua-precipitable",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_AguaPrecipitable", "wrf_arg4k_agua-precipitable"),
        label=("WRF AguaPrecipitable · ", "WRF agua-precipitable · "),
    ),
    _Rename(
        legacy="wrf_CAPE_BRN",
        current="wrf_arg4k_cape-brn",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_CAPE_BRN", "wrf_arg4k_cape-brn"),
        label=("WRF CAPE_BRN · ", "WRF cape-brn · "),
    ),
    _Rename(
        legacy="wrf_Campo900hPa",
        current="wrf_arg4k_campo-900hpa",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_Campo900hPa", "wrf_arg4k_campo-900hpa"),
        label=("WRF Campo900hPa · ", "WRF campo-900hpa · "),
    ),
    _Rename(
        legacy="wrf_Colmax",
        current="wrf_arg4k_colmax",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_Colmax", "wrf_arg4k_colmax"),
        label=("WRF Colmax · ", "WRF colmax · "),
    ),
    _Rename(
        legacy="wrf_CortanteNivelesBajos",
        current="wrf_arg4k_cortante-niveles-bajos",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_CortanteNivelesBajos", "wrf_arg4k_cortante-niveles-bajos"),
        label=("WRF CortanteNivelesBajos · ", "WRF cortante-niveles-bajos · "),
    ),
    _Rename(
        legacy="wrf_Granizo",
        current="wrf_arg4k_granizo",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_Granizo", "wrf_arg4k_granizo"),
        label=("WRF Granizo · ", "WRF granizo · "),
    ),
    _Rename(
        legacy="wrf_JetCapasBajas",
        current="wrf_arg4k_jet-capas-bajas",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_JetCapasBajas", "wrf_arg4k_jet-capas-bajas"),
        label=("WRF JetCapasBajas · ", "WRF jet-capas-bajas · "),
    ),
    _Rename(
        legacy="wrf_MUCAPE",
        current="wrf_arg4k_mucape",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_MUCAPE", "wrf_arg4k_mucape"),
        label=("WRF MUCAPE · ", "WRF mucape · "),
    ),
    _Rename(
        legacy="wrf_Precipitacion1h",
        current="wrf_arg4k_precipitacion-1h",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_Precipitacion1h", "wrf_arg4k_precipitacion-1h"),
        label=("WRF Precipitacion1h · ", "WRF precipitacion-1h · "),
    ),
    _Rename(
        legacy="wrf_Rafagas",
        current="wrf_arg4k_rafagas",
        processor=("wrf", "wrf_arg4k"),
        band=("wrf_Rafagas", "wrf_arg4k_rafagas"),
        label=("WRF Rafagas · ", "WRF rafagas · "),
    ),
    # ECMWF: the same token spells the processor (``ecmwf_tp_processor``), the
    # band (``ecmwf_tp``) and the label (``ECMWF tp_period``).
    _Rename(
        legacy="ecmwf_tp_period",
        current="ecmwf_ifs_total_precipitation_period",
        processor=("ecmwf_tp", "ecmwf_ifs_total_precipitation"),
        band=("ecmwf_tp", "ecmwf_ifs_total_precipitation"),
        label=("ECMWF tp_period", "ECMWF total_precipitation_period"),
    ),
    _Rename(
        legacy="ecmwf_tp_producer",
        current="ecmwf_ifs_total_precipitation_producer",
        processor=("ecmwf_tp", "ecmwf_ifs_total_precipitation"),
        band=("ecmwf_tp", "ecmwf_ifs_total_precipitation"),
        label=("ECMWF tp_producer", "ECMWF total_precipitation_producer"),
    ),
    _Rename(
        legacy="ecmwf_mslp_period",
        current="ecmwf_ifs_mean_sea_level_pressure_period",
        processor=("ecmwf_mslp", "ecmwf_ifs_mean_sea_level_pressure"),
        band=("ecmwf_mslp", "ecmwf_ifs_mean_sea_level_pressure"),
        label=("ECMWF mslp_period", "ECMWF mean_sea_level_pressure_period"),
    ),
    _Rename(
        legacy="ecmwf_mslp_producer",
        current="ecmwf_ifs_mean_sea_level_pressure_producer",
        processor=("ecmwf_mslp", "ecmwf_ifs_mean_sea_level_pressure"),
        band=("ecmwf_mslp", "ecmwf_ifs_mean_sea_level_pressure"),
        label=("ECMWF mslp_producer", "ECMWF mean_sea_level_pressure_producer"),
    ),
)


def _statement(rename: _Rename) -> str:
    """``UPDATE`` for one product, touching only the columns it defines."""
    assignments = ["job_type = :current", "data_source_id = :current"]
    if rename.processor:
        assignments.append("processor_id = REPLACE(processor_id, :proc_old, :proc_new)")
    if rename.band:
        assignments.append("band_id = REPLACE(band_id, :band_old, :band_new)")
    if rename.label:
        assignments.append(
            "product_label = REPLACE(product_label, :label_old, :label_new)"
        )
    return f"UPDATE job_metrics SET {', '.join(assignments)} WHERE job_type = :legacy"


def _params(rename: _Rename) -> dict[str, str]:
    """Bind parameters matching the assignments ``_statement`` emitted."""
    params = {"legacy": rename.legacy, "current": rename.current}
    for prefix, swap in (
        ("proc", rename.processor),
        ("band", rename.band),
        ("label", rename.label),
    ):
        if swap:
            params[f"{prefix}_old"], params[f"{prefix}_new"] = swap
    return params


# Every column this revision writes. The baseline adopts a pre-existing table
# as-is, so a database migrated from before Alembic may not have all of them.
_REQUIRED_COLUMNS = frozenset(
    ("job_type", "data_source_id", "processor_id", "band_id", "product_label")
)


def upgrade() -> None:
    bind = op.get_bind()
    columns = {col["name"] for col in sa.inspect(bind).get_columns("job_metrics")}
    if not _REQUIRED_COLUMNS <= columns:
        return  # nothing to rename in a table that never had these ids
    for rename in _RENAMES:
        bind.execute(sa.text(_statement(rename)), _params(rename))


def downgrade() -> None:
    """No-op: the renamed rows can no longer be told apart from current ones."""
