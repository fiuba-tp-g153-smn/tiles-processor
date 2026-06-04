import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from services.job_descriptor import describe_job


def test_goes_band_13_label_and_timestamp():
    desc = describe_job("goes19_abi_band_13", "20260521320209", "band_13")
    assert desc.job_type == "goes19_abi_band_13"
    assert "GOES ABI" in desc.product_label
    assert "Cloud Tops" in desc.product_label
    assert desc.image_timestamp == "20260521320209"


def test_radar_rma12_dbzh_label_and_timestamp():
    desc = describe_job("radar_DBZH", "RMA12_DBZH_20260114T170328Z", "radar")
    assert desc.job_type == "radar_DBZH"
    assert "RMA12" in desc.product_label
    assert "DBZH" in desc.product_label
    assert "Horizontal Reflectivity" in desc.product_label
    assert desc.image_timestamp == "20260114T170328Z"


def test_wrf_colmax_label_strips_product_prefix():
    desc = describe_job("wrf_Colmax", "Colmax_20260114_00UTC_F006", "wrf_Colmax")
    assert desc.job_type == "wrf_Colmax"
    assert "WRF Colmax" in desc.product_label
    assert desc.image_timestamp == "20260114_00UTC_F006"


def test_glm_folder_label():
    desc = describe_job("glm_folder", "20260521320209", "glm_folder_fed")
    assert desc.job_type == "glm_folder"
    assert "GLM" in desc.product_label
    assert desc.image_timestamp == "20260521320209"


def test_ecmwf_label():
    desc = describe_job("ecmwf_tp_period", "tp_20260114_12UTC_006", "ecmwf_tp")
    assert desc.job_type == "ecmwf_tp_period"
    assert "ECMWF" in desc.product_label


def test_unknown_source_falls_back_to_raw_values():
    desc = describe_job("something_new", "abc123", "")
    assert desc.job_type == "something_new"
    assert desc.product_label == "something_new"
    assert desc.image_timestamp == "abc123"


def test_malformed_radar_image_id_does_not_raise():
    # image_id without the expected 3 underscore-separated parts
    desc = describe_job("radar_DBZH", "weird", "radar")
    assert desc.job_type == "radar_DBZH"
    assert isinstance(desc.product_label, str)
