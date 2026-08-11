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

# Sources that hardcode their processor_id rather than reading it off a config.
_HARDCODED_PROCESSOR_IDS = [
    "goes_band_13",
    "goes_band_9",
    "goes_band_2",
    "glm_fed",
    "radar",
    "wrf",
]


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
        assert GFS_PRODUCT_CONFIGS["250"].processor_id == (
            GFS_PRODUCT_CONFIGS["500"].processor_id
        )

    def test_mslp_does_not_share_the_upper_level_processor(self):
        assert GFS_PRODUCT_CONFIGS["mslp"].processor_id != (
            GFS_PRODUCT_CONFIGS["500"].processor_id
        )


class TestEveryOtherProduct:
    @pytest.mark.parametrize(
        "product", [ECMWF_TP_CONFIG, ECMWF_MSLP_CONFIG], ids=lambda p: p.processor_id
    )
    def test_ecmwf_products_resolve(self, registry, product):
        assert registry.get(product.processor_id) is not None

    @pytest.mark.parametrize("processor_id", _HARDCODED_PROCESSOR_IDS)
    def test_hardcoded_source_ids_resolve(self, registry, processor_id):
        assert registry.get(processor_id) is not None


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
