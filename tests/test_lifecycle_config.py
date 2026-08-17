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
    got = resolve_retention_map({"wrf": {"retention_days": 3}})
    assert got["tiles/wrf"] == 3
    assert got["cog/wrf"] == 3
    assert got["geojson/wrf"] == 3


def test_object_form_overrides_one_kind_and_defaults_the_rest():
    got = resolve_retention_map(
        {"ecmwf": {"retention_days": {"default": 2, "grib": 1}}}
    )
    assert got["tiles/models/ecmwf"] == 2
    assert got["cog/models/ecmwf"] == 2
    assert got["geojson/models/ecmwf"] == 2
    assert got["grib/models/ecmwf"] == 1


def test_absent_source_falls_back_to_default():
    got = resolve_retention_map({})
    assert set(got) == _all_prefixes()  # every known prefix still covered
    assert all(days == DEFAULT_RETENTION_DAYS for days in got.values())


def test_object_form_without_default_uses_module_default_for_missing_kinds():
    got = resolve_retention_map({"wrf": {"retention_days": {"geojson": 4}}})
    assert got["geojson/wrf"] == 4
    assert got["tiles/wrf"] == DEFAULT_RETENTION_DAYS


def test_rejects_unknown_override_kind():
    with pytest.raises(ValueError, match="unknown keys"):
        resolve_retention_map({"wrf": {"retention_days": {"tilez": 2}}})


def test_rejects_zero_or_negative_days():
    with pytest.raises(ValueError, match=">= 1"):
        resolve_retention_map({"radar": {"retention_days": 0}})


def test_rejects_non_integer_days():
    with pytest.raises(ValueError, match=">= 1"):
        resolve_retention_map({"radar": {"retention_days": 1.5}})


def test_rejects_boolean_days():
    # bool is an int subclass; it must not be accepted as a day count.
    with pytest.raises(ValueError, match=">= 1"):
        resolve_retention_map({"radar": {"retention_days": True}})
