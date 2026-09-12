"""Every processor_id a work unit can carry must resolve in the registry.

`ProcessorRegistry.get()` raises `KeyError` at *runtime*, when the first work
unit arrives — not at import. So a product whose `processor_id` was never
registered, or was registered under a typo, passes Pylint, passes mypy, and
fails in production as three guaranteed retries plus a DLQ per unit.

The strings are coupled across three places with nothing checking them:

    models/*_config.py  ->  the id a product declares
    data_sources/*.py   ->  the id a source stamps on its work units
    worker/subprocess_processor.py::create_processor_registry  ->  the id bound

These tests close that loop. They matter most where one registration is shared
by more than one product — `gfs_500` and `gfs_250` are both rendered by
`GfsUpperLevelProcessor` and only `GFS_500_CONFIG.processor_id` is registered
explicitly, so giving 250 hPa its own id would silently break it.
"""

import pytest

from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG
from models.gfs_config import GFS_PRODUCT_CONFIGS


def _config_with_every_product_on():
    """A config with all families enabled, so no source is skipped.

    Products are read off the registries rather than listed, for the same
    reason the ids below are.
    """
    from unittest.mock import MagicMock  # pylint: disable=import-outside-toplevel

    from config import Config  # pylint: disable=import-outside-toplevel
    from models.input_source_config import (  # pylint: disable=import-outside-toplevel
        InputSourceConfig,
    )
    from models.radar_config import (  # pylint: disable=import-outside-toplevel
        RADAR_PRODUCT_CONFIGS,
        RadarStationFilter,
    )
    from models.gfs_config import (  # pylint: disable=import-outside-toplevel
        GfsAccessConfig,
    )
    from models.wrf_config import (  # pylint: disable=import-outside-toplevel
        WRF_PRODUCT_CONFIGS,
    )

    config = MagicMock(spec=Config)
    config.ENABLED_RADAR_PRODUCTS = {pid: True for pid in RADAR_PRODUCT_CONFIGS}
    config.ENABLED_WRF_PRODUCTS = {pid: True for pid in WRF_PRODUCT_CONFIGS}
    config.RADAR_STATION_FILTER = RadarStationFilter("all")
    config.ENABLE_ECMWF_PRECIPITATION = True
    config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE = True
    config.ECMWF_OPENDATA_SOURCES = ("ecmwf",)
    config.ENABLE_GFS_MSLP = True
    config.ENABLE_GFS_500 = True
    config.ENABLE_GFS_250 = True
    config.GFS_CYCLES_TO_MAINTAIN = 3
    config.GFS_MAX_STEPS_PER_TICK = 12
    config.GFS_AVAILABILITY_PROBE_FROM_HOURS = 3
    config.GFS_AVAILABILITY_PROBE_TO_HOURS = 8
    config.GFS_ACCESS = GfsAccessConfig(subset_endpoint="http://nomads.invalid/cgi")
    config.get_bounds.return_value = {
        "minx": -110.0,
        "miny": -60.0,
        "maxx": -30.0,
        "maxy": -15.0,
    }
    for name, mode in (
        ("RADAR_INPUT", "local"),
        ("GOES19_GLM_INPUT", "local"),
        ("WRF_INPUT", "local"),
        ("GOES19_INPUT", "local"),
        ("ECMWF_INPUT", "local"),
        ("GFS_INPUT", "local"),
    ):
        setattr(config, name, InputSourceConfig(mode=mode, input_dir="/tmp/x"))
    for knob in (
        "GOES_TARGET_IMAGES",
        "GOES_MAX_HOURS_BACK",
        "RADAR_TARGET_IMAGES",
        "WRF_TARGET_RUNS",
        "GLM_SAFETY_LAG_SECONDS",
        "GLM_TARGET_WINDOWS",
    ):
        setattr(config, knob, None)
    config.GLM_ACCUM_MINUTES = 10
    config.GLM_PRODUCE_EVERY_MINUTES = 10
    return config


def _registered_source_processor_ids() -> list[str]:
    """Every processor_id a real data source stamps on its work units.

    Read off the sources themselves rather than copied into a literal. The
    hand-maintained list this replaces went stale during a product rename and
    kept passing while 100% of ABI work units were unroutable: it asserted that
    six strings were registered, which they were, but none of them was the id
    any source actually emitted.
    """
    from factories import (  # pylint: disable=import-outside-toplevel
        create_data_source_registry,
    )

    from unittest.mock import (
        MagicMock,
        patch,
    )  # pylint: disable=import-outside-toplevel

    # The tile-bucket client is irrelevant here; only the ids matter.
    with patch("factories.create_s3_client", return_value=MagicMock()):
        sources = create_data_source_registry(_config_with_every_product_on())
    return sorted({s.processor_id for s in sources.get_all()})


@pytest.fixture(name="registry", scope="module")
def _registry():
    """The real registry the subprocess builds, heavy imports and all."""
    from worker.subprocess_processor import (  # pylint: disable=import-outside-toplevel
        create_processor_registry,
    )

    return create_processor_registry()


class TestGfsProducts:
    """The regression this file exists for."""

    @pytest.mark.parametrize(
        "product", GFS_PRODUCT_CONFIGS.values(), ids=lambda p: p.product_id
    )
    def test_every_gfs_product_resolves_to_a_processor(self, registry, product):
        assert registry.get(product.processor_id) is not None

    def test_the_two_upper_level_products_share_one_registration(self):
        """If these ever diverge, gfs_250 needs its own `registry.register`.

        This is not a style preference: `create_processor_registry` registers
        `GFS_500_CONFIG.processor_id` and nothing else for the upper levels, so
        250 hPa works purely because the two ids are equal.
        """
        assert GFS_PRODUCT_CONFIGS["250hpa"].processor_id == (
            GFS_PRODUCT_CONFIGS["500hpa"].processor_id
        )

    def test_mslp_does_not_share_the_upper_level_processor(self):
        assert GFS_PRODUCT_CONFIGS["mslp"].processor_id != (
            GFS_PRODUCT_CONFIGS["500hpa"].processor_id
        )


class TestEveryOtherProduct:
    @pytest.mark.parametrize(
        "product", [ECMWF_TP_CONFIG, ECMWF_MSLP_CONFIG], ids=lambda p: p.processor_id
    )
    def test_ecmwf_products_resolve(self, registry, product):
        assert registry.get(product.processor_id) is not None

    def test_every_source_processor_id_resolves(self, registry):
        """Close the loop: source.processor_id must be a registered key.

        This is the only check that spans data_sources/ and the registry, so a
        rename that moves one and not the other is invisible without it.
        """
        from models.ecmwf_config import (  # pylint: disable=import-outside-toplevel
            ECMWF_MSLP_CONFIG,
            ECMWF_TP_CONFIG,
        )
        from models.gfs_config import (  # pylint: disable=import-outside-toplevel
            GFS_INLINE_PROCESSOR_ID,
        )

        # Download/fan-out units are handled by run_worker's inline_processors
        # dict, never by the subprocess registry (see the contract test below).
        inline = {
            ECMWF_TP_CONFIG.inline_processor_id,
            ECMWF_MSLP_CONFIG.inline_processor_id,
            GFS_INLINE_PROCESSOR_ID,
        }

        unroutable = []
        for processor_id in _registered_source_processor_ids():
            if processor_id in inline:
                continue
            try:
                registry.get(processor_id)
            except KeyError:
                unroutable.append(processor_id)
        assert not unroutable, (
            f"these sources stamp a processor_id nothing is registered under, "
            f"so every one of their work units dead-letters: {unroutable}"
        )


class TestRegistryContract:
    def test_unknown_id_raises_with_the_available_ids(self, registry):
        """The KeyError is the only diagnostic a DLQ'd unit leaves behind."""
        with pytest.raises(KeyError) as caught:
            registry.get("gfs_850")
        assert "gfs_850" in str(caught.value)

    def test_stores_classes_not_instances(self, registry):
        """Lazy instantiation: building every processor per work unit is costly."""
        assert isinstance(registry.get("gfs_mslp"), type)

    def test_inline_processor_ids_are_not_in_the_subprocess_registry(self, registry):
        """Inline processors run in the worker process and are wired separately.

        A GFS/ECMWF *download* unit is handled by `worker.run_worker`'s
        `inline_processors` dict, never by this registry; finding one here would
        mean a fan-out unit could be routed into a subprocess with no RabbitMQ
        client.
        """
        from models.gfs_config import (  # pylint: disable=import-outside-toplevel
            GFS_INLINE_PROCESSOR_ID,
        )

        assert GFS_INLINE_PROCESSOR_ID not in registry.get_all_ids()
        assert ECMWF_TP_CONFIG.inline_processor_id not in registry.get_all_ids()
