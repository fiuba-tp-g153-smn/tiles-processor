"""Tests for the producer's light-vs-normal work-queue routing policy."""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest

from models.work_unit import WorkUnit
from producer.queue_router import QueueRouter

NORMAL = "tiles_work_queue"
LIGHT = "tiles_light_queue"

# Every WRF product currently shipped (settings.json light_queue.wrf).
ALL_WRF_PRODUCTS = frozenset(
    {
        "Colmax",
        "Rafagas",
        "Campo900hPa",
        "Precipitacion1h",
        "MUCAPE",
        "AguaPrecipitable",
        "JetCapasBajas",
        "CortanteNivelesBajos",
        "CAPE_BRN",
        "Granizo",
    }
)


def _router(*, all_radar_light: bool = True, wrf=ALL_WRF_PRODUCTS) -> QueueRouter:
    return QueueRouter(
        normal_queue=NORMAL,
        light_queue=LIGHT,
        all_radar_light=all_radar_light,
        light_wrf_products=frozenset(wrf),
    )


def _unit(data_source_id: str, processor_id: str) -> WorkUnit:
    return WorkUnit.create(
        image_id="img-1",
        source_uri="uri",
        data_source_id=data_source_id,
        processor_id=processor_id,
        output_prefix="tiles/x",
        bounds={"minx": 0.0, "miny": 0.0, "maxx": 1.0, "maxy": 1.0},
        band_id=data_source_id,
    )


@pytest.mark.parametrize(
    "data_source_id",
    ["radar_DBZH", "radar_VRAD", "radar_ZDR", "radar_RHOHV", "radar_KDP"],
)
def test_radar_units_go_to_light_queue(data_source_id):
    router = _router()
    assert router.route(_unit(data_source_id, "radar")) == LIGHT


@pytest.mark.parametrize("product_id", sorted(ALL_WRF_PRODUCTS))
def test_configured_wrf_units_go_to_light_queue(product_id):
    router = _router()
    assert router.route(_unit(f"wrf_{product_id}", "wrf")) == LIGHT


@pytest.mark.parametrize(
    "data_source_id,processor_id",
    [
        ("goes19_abi_band_2", "goes_band_2"),
        ("goes19_abi_band_13", "goes_band_13"),
        ("glm_folder", "glm_fed"),
        ("ecmwf_tp_producer", "ecmwf_tp_processor"),
    ],
)
def test_heavy_units_go_to_normal_queue(data_source_id, processor_id):
    router = _router()
    assert router.route(_unit(data_source_id, processor_id)) == NORMAL


def test_wrf_product_absent_from_config_goes_to_normal_queue():
    """Config-driven membership: a WRF product not in the set stays normal."""
    router = _router(wrf={"Colmax"})  # only Colmax is light
    assert router.route(_unit("wrf_Colmax", "wrf")) == LIGHT
    assert router.route(_unit("wrf_Granizo", "wrf")) == NORMAL


def test_radar_stays_normal_when_all_radar_light_disabled():
    router = _router(all_radar_light=False)
    assert router.route(_unit("radar_DBZH", "radar")) == NORMAL
