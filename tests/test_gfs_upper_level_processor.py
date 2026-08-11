"""Tests for the GFS upper-level processor (ports of `tgv500b.gs` / `250b.gs`)."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from models.gfs_config import (
    GEOPOTENTIAL_STEP_M,
    GFS_250_CONFIG,
    GFS_500_CONFIG,
    ISOTHERM_STEP_C,
)
from models.gfs_palettes import (
    WIND_250_COLORS,
    WIND_250_THRESHOLDS,
    WIND_500_COLORS,
    WIND_500_THRESHOLDS,
    wind_palette,
)
from models.units import MS_TO_KNOTS
from models.work_unit import WorkUnit

FIXTURE = Path(__file__).parent / "fixtures" / "gfs_sample_f003.grib2"
BOUNDS = {"minx": -70.0, "miny": -40.0, "maxx": -60.0, "maxy": -30.0}
CYCLE_ISO = "2026-08-08T00:00:00+00:00"
CYCLE_TS = "20260808T0000Z"
IMAGE_ID = f"{CYCLE_TS}_f003"


@pytest.fixture(name="processor")
def _processor(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    from config import Config  # pylint: disable=import-outside-toplevel

    with patch("processors.gfs_upper_level_processor.create_s3_client") as factory:
        factory.return_value = AsyncMock(
            upload_file=AsyncMock(return_value=True),
            upload_directory=AsyncMock(return_value=1),
        )
        from processors.gfs_upper_level_processor import (  # pylint: disable=import-outside-toplevel
            GfsUpperLevelProcessor,
        )

        yield GfsUpperLevelProcessor(Config())


def _work_unit(product) -> WorkUnit:
    return WorkUnit.create(
        image_id=IMAGE_ID,
        source_uri=json.dumps(
            {
                "grib_path": "grib/models/gfs/x.grib2",
                "cycle": CYCLE_ISO,
                "step_hours": 3,
                "product_id": product.product_id,
            }
        ),
        data_source_id="gfs_step",
        processor_id=product.processor_id,
        output_prefix=f"{product.tiles_prefix}/{CYCLE_TS}",
        bounds=BOUNDS,
        band_id=product.band_id,
    )


class TestPalettes:
    """Ported verbatim from jaecol / define_colors.gs."""

    def test_500_uses_twenty_knot_steps(self):
        steps = {b - a for a, b in zip(WIND_500_THRESHOLDS, WIND_500_THRESHOLDS[1:])}
        assert steps == {20.0}

    def test_250_uses_ten_knot_steps(self):
        steps = {b - a for a, b in zip(WIND_250_THRESHOLDS, WIND_250_THRESHOLDS[1:])}
        assert steps == {10.0}

    @pytest.mark.parametrize(
        "thresholds,colors",
        [
            (WIND_500_THRESHOLDS, WIND_500_COLORS),
            (WIND_250_THRESHOLDS, WIND_250_COLORS),
        ],
    )
    def test_every_band_has_a_colour(self, thresholds, colors):
        assert len(thresholds) == len(colors)

    @pytest.mark.parametrize("thresholds", [WIND_500_THRESHOLDS, WIND_250_THRESHOLDS])
    def test_shading_starts_at_eighty_knots(self, thresholds):
        assert thresholds[0] == 80.0

    @pytest.mark.parametrize("colors", [WIND_500_COLORS, WIND_250_COLORS])
    def test_colours_are_valid_hex(self, colors):
        for color in colors:
            assert len(color) == 7 and color.startswith("#")
            int(color[1:], 16)

    def test_lookup_by_product(self):
        assert wind_palette("500") == (WIND_500_THRESHOLDS, WIND_500_COLORS)
        assert wind_palette("250") == (WIND_250_THRESHOLDS, WIND_250_COLORS)

    def test_unknown_product_raises(self):
        with pytest.raises(ValueError, match="No wind palette"):
            wind_palette("850")


class TestLoad:
    def test_wind_speed_is_in_knots(self, processor):
        fields = processor._load(FIXTURE, GFS_250_CONFIG, BOUNDS)
        assert fields["speed"].attrs["units"] == "kt"
        assert 0.0 <= float(fields["speed"].min())
        assert float(fields["speed"].max()) < 400.0

    def test_uses_the_single_correct_conversion_constant(self, processor):
        import numpy as np  # pylint: disable=import-outside-toplevel

        from services.gfs_field import (
            load_field,
        )  # pylint: disable=import-outside-toplevel

        fields = processor._load(FIXTURE, GFS_250_CONFIG, BOUNDS)
        u = load_field(FIXTURE, "u", BOUNDS, level_hpa=250)
        v = load_field(FIXTURE, "v", BOUNDS, level_hpa=250)
        expected = float(np.hypot(u.values, v.values).max() * MS_TO_KNOTS)
        assert float(fields["speed"].max()) == pytest.approx(expected, rel=1e-5)

    def test_500_loads_temperature_and_wind_components(self, processor):
        fields = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)
        assert {"speed", "height", "temperature", "u", "v"} == set(fields)

    def test_250_loads_only_what_it_renders(self, processor):
        """No isotherms or barbs at 250 hPa, so those fields are never read."""
        fields = processor._load(FIXTURE, GFS_250_CONFIG, BOUNDS)
        assert set(fields) == {"speed", "height"}

    def test_temperature_is_converted_to_celsius(self, processor):
        fields = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)
        assert -80.0 < float(fields["temperature"].mean()) < 20.0

    def test_levels_are_distinct_between_products(self, processor):
        """A level mix-up would be invisible without this."""
        height_500 = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)["height"]
        height_250 = processor._load(FIXTURE, GFS_250_CONFIG, BOUNDS)["height"]
        assert float(height_250.mean()) > float(height_500.mean())


class TestTileUpsampling:
    """GFS is 0.25 deg (~22 km): roughly one tile pixel at zoom 2.

    Shading that natively and cutting tiles to zoom 7 would replicate each cell
    across ~23x23 pixels, so the colour bands render as hard staircases. The
    bilinear upsample is what makes zoom 7 carry real detail — the same reason
    `EcmwfTotalPrecipitationProcessor` upsamples its precipitation thresholds.
    """

    def test_upsamples_to_the_configured_resolution(self, processor):
        fields = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)
        upsampled = processor._upsample(fields["speed"], GFS_500_CONFIG)
        step = abs(float(upsampled["x"][1] - upsampled["x"][0]))
        assert step == pytest.approx(
            processor.config.GFS_TILE_SMOOTHING_RESOLUTION_DEG, rel=0.05
        )

    def test_produces_a_much_finer_grid(self, processor):
        fields = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)
        native = fields["speed"]
        upsampled = processor._upsample(native, GFS_500_CONFIG)
        assert upsampled.size > native.size * 100

    def test_bilinear_never_invents_values_outside_the_native_range(self, processor):
        """A convex combination of neighbours cannot fabricate a stronger jet."""
        fields = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)
        native = fields["speed"]
        upsampled = processor._upsample(native, GFS_500_CONFIG)
        assert float(upsampled.min()) >= float(native.min()) - 0.01
        assert float(upsampled.max()) <= float(native.max()) + 0.01

    def test_can_be_disabled(self, processor, monkeypatch):
        monkeypatch.setattr(processor.config, "GFS_TILE_SMOOTHING_RESOLUTION_DEG", 0.0)
        fields = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)
        upsampled = processor._upsample(fields["speed"], GFS_500_CONFIG)
        assert upsampled.shape == fields["speed"].shape

    def test_the_cog_keeps_the_native_grid(self, processor, tmp_path):
        """Only tiles are smoothed; the COG must carry real model values.

        Read back off disk, not inferred: a COG silently written at 0.01 deg
        would hand downstream consumers 625x interpolated data as if it were
        model output.
        """
        import rioxarray  # pylint: disable=import-outside-toplevel

        fields = processor._load(FIXTURE, GFS_250_CONFIG, BOUNDS)
        native_shape = fields["speed"].shape
        cog_path, rgba_path, _ = processor._generate_outputs(
            fields, GFS_250_CONFIG, tmp_path, IMAGE_ID
        )
        cog = rioxarray.open_rasterio(cog_path)
        assert cog.shape[-2:] == native_shape

        rgba = rioxarray.open_rasterio(rgba_path)
        assert rgba.shape[-1] > native_shape[-1] * 10


class TestOverlays:
    def test_500_emits_heights_isotherms_and_barbs(self, processor, tmp_path):
        fields = processor._load(FIXTURE, GFS_500_CONFIG, BOUNDS)
        overlays = processor._build_overlays(fields, GFS_500_CONFIG, tmp_path, IMAGE_ID)
        assert set(overlays) == {"heights", "isotherms", "barbs"}

    def test_250_emits_only_heights(self, processor, tmp_path):
        fields = processor._load(FIXTURE, GFS_250_CONFIG, BOUNDS)
        overlays = processor._build_overlays(fields, GFS_250_CONFIG, tmp_path, IMAGE_ID)
        assert set(overlays) == {"heights"}

    def test_height_contours_are_every_sixty_gpm(self, processor, tmp_path):
        features = _overlay_features(processor, tmp_path, GFS_500_CONFIG, "heights")
        values = sorted({f["properties"]["height_gpm"] for f in features})
        assert values
        assert all(v % GEOPOTENTIAL_STEP_M == 0 for v in values)

    def test_isotherms_are_every_five_degrees(self, processor, tmp_path):
        features = _overlay_features(processor, tmp_path, GFS_500_CONFIG, "isotherms")
        values = sorted({f["properties"]["temp_c"] for f in features})
        assert values
        assert all(v % ISOTHERM_STEP_C == 0 for v in values)

    def test_barbs_are_one_file_per_tile(self, processor, tmp_path):
        """Same layout WRF uses, so data-service serves both the same way.

        A single document keyed by z/x/y would ship every barb in the domain
        on each request regardless of viewport.
        """
        overlays = _overlays(processor, tmp_path, GFS_500_CONFIG)
        root = overlays["barbs"]
        assert root.is_dir()
        tiles = sorted(root.rglob("*.json"))
        assert tiles

    def test_barb_tiles_are_laid_out_as_z_x_y(self, processor, tmp_path):
        overlays = _overlays(processor, tmp_path, GFS_500_CONFIG)
        root = overlays["barbs"]
        for tile in root.rglob("*.json"):
            zoom, tile_x = tile.parent.parent.name, tile.parent.name
            assert zoom.isdigit() and tile_x.isdigit() and tile.stem.isdigit()

    def test_each_barb_tile_is_a_feature_collection(self, processor, tmp_path):
        """The frontend consumes these directly, so they must be valid GeoJSON."""
        overlays = _overlays(processor, tmp_path, GFS_500_CONFIG)
        tile = next(iter(overlays["barbs"].rglob("*.json")))
        document = json.loads(tile.read_text())
        assert document["type"] == "FeatureCollection"
        assert document["features"]

    def test_barbs_carry_speed_in_knots(self, processor, tmp_path):
        overlays = _overlays(processor, tmp_path, GFS_500_CONFIG)
        tile = next(iter(overlays["barbs"].rglob("*.json")))
        properties = json.loads(tile.read_text())["features"][0]["properties"]
        assert "speed_kt" in properties
        assert "dir_deg" in properties

    def test_barb_zoom_levels_match_the_shared_strides(self, processor, tmp_path):
        """extract_barbs_tiled caps at z8; deeper zooms overzoom on the client."""
        from services.contouring import (  # pylint: disable=import-outside-toplevel
            BARB_ZOOM_STRIDES,
        )

        overlays = _overlays(processor, tmp_path, GFS_500_CONFIG)
        zooms = {int(p.name) for p in overlays["barbs"].iterdir() if p.is_dir()}
        assert zooms <= set(BARB_ZOOM_STRIDES)

    def test_overlay_geometry_uses_signed_longitudes(self, processor, tmp_path):
        features = _overlay_features(processor, tmp_path, GFS_500_CONFIG, "heights")
        lons = [c[0] for f in features for c in f["geometry"]["coordinates"]]
        assert lons and max(lons) <= 0.0


class TestPipeline:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("product", [GFS_500_CONFIG, GFS_250_CONFIG])
    async def test_uploads_cog_and_single_file_overlays(self, processor, product):
        """Barbs are excluded here: they upload as a directory, not a file."""
        await processor.process(str(FIXTURE), _work_unit(product))
        keys = [c.args[0] for c in processor._s3_client.upload_file.call_args_list]
        file_overlays = 2 if product is GFS_500_CONFIG else 1  # heights (+isotherms)
        assert len(keys) == 1 + file_overlays

    @pytest.mark.asyncio
    async def test_500_uploads_tiles_and_barbs_as_directories(self, processor):
        """Two directory uploads: the tile pyramid and the per-tile barbs."""
        await processor.process(str(FIXTURE), _work_unit(GFS_500_CONFIG))
        prefixes = [
            c.args[1] for c in processor._s3_client.upload_directory.call_args_list
        ]
        assert f"{GFS_500_CONFIG.tiles_prefix}/{CYCLE_TS}/{IMAGE_ID}" in prefixes
        assert (
            f"{GFS_500_CONFIG.geojson_prefix}/{CYCLE_TS}/{IMAGE_ID}_barbs" in prefixes
        )

    @pytest.mark.asyncio
    async def test_250_uploads_no_barbs(self, processor):
        """250 hPa has no barbs, so only the tile pyramid goes up as a directory."""
        await processor.process(str(FIXTURE), _work_unit(GFS_250_CONFIG))
        prefixes = [
            c.args[1] for c in processor._s3_client.upload_directory.call_args_list
        ]
        assert not any("barbs" in p for p in prefixes)

    @pytest.mark.asyncio
    async def test_cog_key_matches_what_the_downloader_checks(self, processor):
        """The fan-out dedups on this key; a mismatch re-renders for ever."""
        await processor.process(str(FIXTURE), _work_unit(GFS_250_CONFIG))
        keys = [c.args[0] for c in processor._s3_client.upload_file.call_args_list]
        assert f"{GFS_250_CONFIG.cog_prefix}/{CYCLE_TS}/{IMAGE_ID}.tif" in keys

    @pytest.mark.asyncio
    async def test_products_write_to_separate_prefixes(self, processor):
        """500 and 250 share a processor, so a leak here is a real risk."""
        await processor.process(str(FIXTURE), _work_unit(GFS_500_CONFIG))
        keys = [c.args[0] for c in processor._s3_client.upload_file.call_args_list]
        assert all("500hpa" in k for k in keys)
        assert not any("250hpa" in k for k in keys)

    @pytest.mark.asyncio
    async def test_tiles_are_namespaced_by_cycle_and_step(self, processor):
        await processor.process(str(FIXTURE), _work_unit(GFS_500_CONFIG))
        prefix = processor._s3_client.upload_directory.call_args.args[1]
        assert prefix == f"{GFS_500_CONFIG.tiles_prefix}/{CYCLE_TS}/{IMAGE_ID}"

    @pytest.mark.asyncio
    async def test_missing_grib_fails_loudly(self, processor):
        with pytest.raises(FileNotFoundError):
            await processor.process("/nonexistent/x.grib2", _work_unit(GFS_500_CONFIG))

    @pytest.mark.asyncio
    async def test_unknown_band_id_is_rejected(self, processor):
        unit = _work_unit(GFS_500_CONFIG)
        unit.band_id = "gfs_850"
        with pytest.raises(ValueError, match="Unknown GFS band_id"):
            await processor.process(str(FIXTURE), unit)

    @pytest.mark.asyncio
    async def test_records_stage_timings(self, processor):
        await processor.process(str(FIXTURE), _work_unit(GFS_500_CONFIG))
        assert {"load", "cog", "geotiff", "geojson", "tiling", "upload"} <= set(
            processor._stage_timings
        )


def _overlays(processor, tmp_path: Path, product) -> dict:
    """Build every overlay for one product and return the path map."""
    fields = processor._load(FIXTURE, product, BOUNDS)
    return processor._build_overlays(fields, product, tmp_path, IMAGE_ID)


def _overlay_features(processor, tmp_path: Path, product, kind: str) -> list[dict]:
    fields = processor._load(FIXTURE, product, BOUNDS)
    overlays = processor._build_overlays(fields, product, tmp_path, IMAGE_ID)
    return json.loads(overlays[kind].read_text())["features"]
