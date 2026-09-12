import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from services.job_descriptor import describe_job


def test_goes_band_13_label_and_timestamp():
    desc = describe_job("goes19_abi_c13", "20260521320209", "goes19_abi_c13")
    assert desc.job_type == "goes19_abi_c13"
    assert "GOES-19 ABI" in desc.product_label
    assert "Cloud Tops" in desc.product_label
    assert desc.image_timestamp == "20260521320209"


def test_radar_rma12_dbzh_label_and_timestamp():
    desc = describe_job("radar_sinarame_dbzh", "RMA12_dbzh_20260114T170328Z", "radar")
    assert desc.job_type == "radar_sinarame_dbzh"
    assert "RMA12" in desc.product_label
    assert "dbzh" in desc.product_label
    assert "Horizontal Reflectivity" in desc.product_label
    assert desc.image_timestamp == "20260114T170328Z"


def test_radar_product_with_underscore_keeps_timestamp():
    # DBZH_450KM has an underscore in the product id, so the timestamp must be
    # read off the end of the image_id rather than by field position.
    desc = describe_job(
        "radar_sinarame_dbzh-450km", "RMA1_dbzh-450km_20260114T170328Z", "radar"
    )
    assert "RMA1" in desc.product_label
    assert "dbzh-450km" in desc.product_label
    assert "450 km" in desc.product_label
    assert desc.image_timestamp == "20260114T170328Z"


def test_wrf_colmax_label_strips_product_prefix():
    desc = describe_job(
        "wrf_arg4k_colmax", "colmax_20260114_00UTC_F006", "wrf_arg4k_colmax"
    )
    assert desc.job_type == "wrf_arg4k_colmax"
    assert "WRF colmax" in desc.product_label
    assert desc.image_timestamp == "20260114_00UTC_F006"


def test_goes19_glm_label():
    desc = describe_job("goes19_glm", "20260521320209", "goes19_glm_fed")
    assert desc.job_type == "goes19_glm"
    assert "GLM" in desc.product_label
    assert desc.image_timestamp == "20260521320209"


def test_ecmwf_label():
    desc = describe_job("ecmwf_ifs_tp_period", "tp_20260114_12UTC_006", "ecmwf_tp")
    assert desc.job_type == "ecmwf_ifs_tp_period"
    assert "ECMWF" in desc.product_label


def test_unknown_source_falls_back_to_raw_values():
    desc = describe_job("something_new", "abc123", "")
    assert desc.job_type == "something_new"
    assert desc.product_label == "something_new"
    assert desc.image_timestamp == "abc123"


def test_malformed_radar_image_id_does_not_raise():
    # image_id without the expected 3 underscore-separated parts
    desc = describe_job("radar_sinarame_dbzh", "weird", "radar")
    assert desc.job_type == "radar_sinarame_dbzh"
    assert isinstance(desc.product_label, str)
