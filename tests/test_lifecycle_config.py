"""Tests for resolve_retention_map (per-source settings -> {prefix: days})."""

import pytest

from models.lifecycle_config import (
    DEFAULT_RETENTION_DAYS,
    SOURCE_LIFECYCLE_PREFIXES,
    resolve_retention_map,
)


def _all_prefixes():
    return {p for kinds in SOURCE_LIFECYCLE_PREFIXES.values() for p in kinds.values()}


def test_int_form_applies_uniformly_across_a_sources_prefixes():
    got = resolve_retention_map({"wrf-arg4k": {"retention_days": 3}})
    assert got["tiles/wrf-arg4k"] == 3
    assert got["cog/wrf-arg4k"] == 3
    assert got["geojson/wrf-arg4k"] == 3


def test_object_form_overrides_one_kind_and_defaults_the_rest():
    got = resolve_retention_map(
        {"ecmwf-ifs": {"retention_days": {"default": 2, "grib": 1}}}
    )
    assert got["tiles/ecmwf-ifs"] == 2
    assert got["cog/ecmwf-ifs"] == 2
    assert got["geojson/ecmwf-ifs"] == 2
    assert got["grib/ecmwf-ifs"] == 1


def test_absent_source_falls_back_to_default():
    got = resolve_retention_map({})
    assert set(got) == _all_prefixes()  # every known prefix still covered
    assert all(days == DEFAULT_RETENTION_DAYS for days in got.values())


def test_object_form_without_default_uses_module_default_for_missing_kinds():
    got = resolve_retention_map({"wrf-arg4k": {"retention_days": {"geojson": 4}}})
    assert got["geojson/wrf-arg4k"] == 4
    assert got["tiles/wrf-arg4k"] == DEFAULT_RETENTION_DAYS


def test_rejects_unknown_override_kind():
    with pytest.raises(ValueError, match="unknown keys"):
        resolve_retention_map({"wrf-arg4k": {"retention_days": {"tilez": 2}}})


def test_rejects_zero_or_negative_days():
    with pytest.raises(ValueError, match=">= 1"):
        resolve_retention_map({"radar-sinarame": {"retention_days": 0}})


def test_rejects_non_integer_days():
    with pytest.raises(ValueError, match=">= 1"):
        resolve_retention_map({"radar-sinarame": {"retention_days": 1.5}})


def test_rejects_boolean_days():
    # bool is an int subclass; it must not be accepted as a day count.
    with pytest.raises(ValueError, match=">= 1"):
        resolve_retention_map({"radar-sinarame": {"retention_days": True}})


def test_every_written_prefix_is_covered_by_a_lifecycle_rule():
    """No uploader may write under a prefix that no expiration rule matches.

    SeaweedFS stamps a volume TTL from the matching lifecycle rule on the
    PutObject path only. An object written under an uncovered prefix is stored
    with no expiry and never returns its volume slots, which is what exhausted
    the cluster's 900 slots in Sept 2026. A prefix rename that forgets this map
    reintroduces exactly that, silently. Covers every family, so each rename is
    checked rather than only the one the author remembered.
    """
    import json
    from pathlib import Path

    from models.band_config import BAND_CONFIGS
    from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG
    from models.gfs_config import GFS_PRODUCT_CONFIGS
    from models.lifecycle_config import resolve_retention_map
    from models.radar_config import RADAR_PRODUCT_CONFIGS
    from models.wrf_config import WRF_PRODUCT_CONFIGS

    settings = json.loads((Path(__file__).parent.parent / "settings.json").read_text())
    rules = resolve_retention_map(settings["sources"])

    written: set[str] = set()
    for cfg in (*BAND_CONFIGS.values(), *RADAR_PRODUCT_CONFIGS.values()):
        written |= {cfg.s3_tiles_prefix, cfg.s3_cog_prefix}
    for cfg in WRF_PRODUCT_CONFIGS.values():
        written |= {cfg.s3_tiles_prefix, cfg.s3_cog_prefix, cfg.s3_geojson_prefix}
    for cfg in (ECMWF_TP_CONFIG, ECMWF_MSLP_CONFIG):
        written |= {cfg.tiles_prefix, cfg.cog_prefix, cfg.grib_prefix}
        if cfg.geojson_prefix:
            written.add(cfg.geojson_prefix)
    for cfg in GFS_PRODUCT_CONFIGS.values():
        written |= {
            cfg.tiles_prefix,
            cfg.cog_prefix,
            cfg.geojson_prefix,
            cfg.grib_prefix,
        }

    uncovered = sorted(w for w in written if not any(w.startswith(p) for p in rules))
    assert not uncovered, (
        f"these prefixes are written but match no lifecycle rule, so objects "
        f"under them would never expire: {uncovered}"
    )


@pytest.mark.parametrize("settings_name", ["settings.json", "settings-beta-1.json"])
def test_settings_source_keys_are_all_known(settings_name):
    """Every source block must be one the config actually reads.

    Config resolves each source with ``_sources.get(<key>, {})``, so a key that
    no longer matches yields an empty block and every setting under it silently
    reverts to its default: products off, retention 1 day, no station filter.
    That is how a whole deployment preset can go dark without an error, which is
    exactly what happened to settings-beta-1.json during the product rename.
    """
    import json
    from pathlib import Path

    from models.lifecycle_config import SOURCE_LIFECYCLE_PREFIXES

    settings = json.loads((Path(__file__).parent.parent / settings_name).read_text())
    unknown = sorted(set(settings["sources"]) - set(SOURCE_LIFECYCLE_PREFIXES))
    assert not unknown, (
        f"{settings_name} has source keys the config does not read, so their "
        f"settings are silently ignored: {unknown}"
    )


@pytest.mark.parametrize("settings_name", ["settings.json", "settings-beta-1.json"])
def test_settings_product_keys_are_all_known(settings_name):
    """Same for the product ids inside each source block."""
    import json
    from pathlib import Path

    from models.band_config import BAND_CONFIGS
    from models.gfs_config import GFS_PRODUCT_CONFIGS
    from models.radar_config import RADAR_PRODUCT_CONFIGS
    from models.wrf_config import WRF_PRODUCT_CONFIGS

    known = {
        "goes19-abi": {"c13", "c09", "c02"},
        "goes19-glm": {"fed", "toe", "mfa"},
        "radar-sinarame": set(RADAR_PRODUCT_CONFIGS),
        "wrf-arg4k": set(WRF_PRODUCT_CONFIGS),
        "ecmwf-ifs": {"precipitation", "mean_sea_level_pressure"},
        "gfs": set(GFS_PRODUCT_CONFIGS),
    }
    assert BAND_CONFIGS, "band configs must exist for this guard to mean anything"

    settings = json.loads((Path(__file__).parent.parent / settings_name).read_text())
    stray = {
        source: sorted(set(block.get("products", {})) - known.get(source, set()))
        for source, block in settings["sources"].items()
        if sorted(set(block.get("products", {})) - known.get(source, set()))
    }
    assert not stray, (
        f"{settings_name} lists products the code does not know, so toggling "
        f"them does nothing: {stray}"
    )
