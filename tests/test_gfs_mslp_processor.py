"""Tests for the GFS sea-level-pressure processor (port of `slpb.gs`)."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from models.gfs_config import GFS_MSLP_CONFIG, HIGHLIGHTED_THICKNESS_M
from models.work_unit import WorkUnit

FIXTURE = Path(__file__).parent / "fixtures" / "gfs_sample_f003.grib2"
BOUNDS = {"minx": -70.0, "miny": -40.0, "maxx": -60.0, "maxy": -30.0}
CYCLE_ISO = "2026-08-08T00:00:00+00:00"
CYCLE_TS = "20260808T0000Z"
IMAGE_ID = f"{CYCLE_TS}_f003"


@pytest.fixture(name="processor")
def _processor(tmp_path, monkeypatch):
    """A processor with S3 stubbed and scratch space under tmp_path."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    from config import Config  # pylint: disable=import-outside-toplevel

    with patch("processors.gfs_mslp_processor.create_s3_client") as factory:
        factory.return_value = AsyncMock(upload_file=AsyncMock(return_value=True))
        from processors.gfs_mslp_processor import (  # pylint: disable=import-outside-toplevel
            GfsMslpProcessor,
        )

        yield GfsMslpProcessor(Config())


def _work_unit() -> WorkUnit:
    return WorkUnit.create(
        image_id=IMAGE_ID,
        source_uri=json.dumps(
            {
                "grib_path": "grib/models/gfs/x.grib2",
                "cycle": CYCLE_ISO,
                "step_hours": 3,
                "product_id": "mslp",
            }
        ),
        data_source_id="gfs_step",
        processor_id=GFS_MSLP_CONFIG.processor_id,
        output_prefix=f"{GFS_MSLP_CONFIG.tiles_prefix}/{CYCLE_TS}",
        bounds=BOUNDS,
        band_id=GFS_MSLP_CONFIG.band_id,
    )


def _uploaded_keys(processor) -> list[str]:
    return [c.args[0] for c in processor._s3_client.upload_file.call_args_list]


class TestLoad:
    def test_pressure_is_converted_to_hectopascals(self, processor):
        pressure, _ = processor._load(FIXTURE, BOUNDS)
        assert 870.0 < float(pressure.min()) < float(pressure.max()) < 1090.0
        assert pressure.attrs["units"] == "hPa"

    def test_thickness_is_the_500_minus_1000_difference(self, processor):
        _, thickness = processor._load(FIXTURE, BOUNDS)
        assert 4800.0 < float(thickness.min()) < float(thickness.max()) < 6000.0
        assert thickness.attrs["units"] == "gpm"

    def test_fields_share_a_grid(self, processor):
        pressure, thickness = processor._load(FIXTURE, BOUNDS)
        assert pressure.shape == thickness.shape


class TestOutputs:
    def test_writes_a_cog_and_both_overlays(self, processor, tmp_path):
        pressure, thickness = processor._load(FIXTURE, BOUNDS)
        outputs = processor._generate_outputs(pressure, thickness, tmp_path, IMAGE_ID)
        assert set(outputs) == {"cog", "isobars", "thickness"}
        assert all(path.exists() for path in outputs.values())

    def test_isobars_are_every_three_hectopascals(self, processor, tmp_path):
        """`slpb.gs`: `set cint 3`."""
        features = _features(processor, tmp_path, "isobars")
        values = sorted({f["properties"]["pressure_hpa"] for f in features})
        assert values, "expected at least one isobar"
        assert all(v % 3 == 0 for v in values)

    def test_thickness_contours_are_every_sixty_gpm(self, processor, tmp_path):
        """`slpb.gs`: `set cint 60`."""
        features = _features(processor, tmp_path, "thickness")
        values = sorted({f["properties"]["thickness_gpm"] for f in features})
        assert values
        assert all(v % 60 == 0 for v in values)

    def test_air_mass_markers_are_flagged(self, processor, tmp_path):
        """The four contours `slpb.gs` redraws in colour."""
        features = _features(processor, tmp_path, "thickness")
        highlighted = {
            f["properties"]["thickness_gpm"]
            for f in features
            if f["properties"]["highlight"]
        }
        assert highlighted
        assert highlighted <= set(HIGHLIGHTED_THICKNESS_M)

    def test_every_contour_carries_a_highlight_flag(self, processor, tmp_path):
        """The frontend branches on it, so it must never be absent."""
        features = _features(processor, tmp_path, "thickness")
        assert all("highlight" in f["properties"] for f in features)

    def test_ordinary_contours_are_not_flagged(self, processor, tmp_path):
        features = _features(processor, tmp_path, "thickness")
        plain = [
            f
            for f in features
            if f["properties"]["thickness_gpm"] not in HIGHLIGHTED_THICKNESS_M
        ]
        assert plain
        assert not any(f["properties"]["highlight"] for f in plain)

    def test_geometry_uses_signed_longitudes(self, processor, tmp_path):
        """A 0-360 leak here would put the overlay half a world away."""
        features = _features(processor, tmp_path, "isobars")
        lons = [c[0] for f in features for c in f["geometry"]["coordinates"]]
        assert max(lons) <= 0.0
        assert min(lons) >= -180.0


class TestPipeline:
    @pytest.mark.asyncio
    async def test_uploads_cog_and_both_overlays(self, processor):
        await processor.process(str(FIXTURE), _work_unit())
        keys = _uploaded_keys(processor)
        assert len(keys) == 3

    @pytest.mark.asyncio
    async def test_cog_key_matches_what_the_downloader_checks(self, processor):
        """The fan-out dedups on this exact key; a mismatch re-renders for ever."""
        await processor.process(str(FIXTURE), _work_unit())
        expected = f"{GFS_MSLP_CONFIG.cog_prefix}/{CYCLE_TS}/{IMAGE_ID}.tif"
        assert expected in _uploaded_keys(processor)

    @pytest.mark.asyncio
    async def test_overlays_are_namespaced_by_cycle(self, processor):
        await processor.process(str(FIXTURE), _work_unit())
        geojson_keys = [k for k in _uploaded_keys(processor) if k.endswith(".json")]
        assert len(geojson_keys) == 2
        for key in geojson_keys:
            assert key.startswith(f"{GFS_MSLP_CONFIG.geojson_prefix}/{CYCLE_TS}/")

    @pytest.mark.asyncio
    async def test_missing_grib_fails_loudly(self, processor):
        with pytest.raises(FileNotFoundError):
            await processor.process("/nonexistent/x.grib2", _work_unit())

    @pytest.mark.asyncio
    async def test_intermediate_files_are_cleaned_up(self, processor, tmp_path):
        await processor.process(str(FIXTURE), _work_unit())
        leftovers = list((tmp_path / "tmp").rglob("*.tif")) + list(
            (tmp_path / "tmp").rglob("*.json")
        )
        assert leftovers == []

    @pytest.mark.asyncio
    async def test_records_stage_timings(self, processor):
        await processor.process(str(FIXTURE), _work_unit())
        assert {"load", "cog", "geojson", "upload"} <= set(processor._stage_timings)


def _features(processor, tmp_path: Path, kind: str) -> list[dict]:
    """Run the output stage and return the parsed features of one overlay."""
    pressure, thickness = processor._load(FIXTURE, BOUNDS)
    outputs = processor._generate_outputs(pressure, thickness, tmp_path, IMAGE_ID)
    return json.loads(outputs[kind].read_text())["features"]
