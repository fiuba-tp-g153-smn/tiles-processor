"""Tests for the GFS inline downloader: caching and per-product fan-out."""

import json
from datetime import UTC, datetime

import pytest

from models.gfs_config import (
    GFS_250_CONFIG,
    GFS_500_CONFIG,
    GFS_GRIB_PREFIX,
    GFS_MSLP_CONFIG,
    GFS_STEP_DATA_SOURCE_ID,
    forecast_steps,
)
from models.work_unit import WorkUnit
from worker.gfs_grib_downloader import GfsGribDownloader

CYCLE = datetime(2026, 8, 8, 6, tzinfo=UTC)
CYCLE_TS = "20260808T0600Z"
IMAGE_ID = f"{CYCLE_TS}_f003"
BOUNDS = {"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0}
ALL_PRODUCTS = [GFS_MSLP_CONFIG, GFS_500_CONFIG, GFS_250_CONFIG]


class FakeS3:
    """Records uploads and replays a canned set of existing keys."""

    def __init__(self, existing: set[str] | None = None, head_fails: bool = False):
        self.existing = existing or set()
        self.head_fails = head_fails
        self.uploaded: list[str] = []

    async def head_exists(self, key: str) -> bool:
        if self.head_fails:
            raise RuntimeError("S3 unavailable")
        return key in self.existing

    async def upload_file(self, key: str, file_path) -> bool:
        self.uploaded.append(key)
        return True


class FakeMq:
    """Captures published work units."""

    def __init__(self):
        self.published: list[WorkUnit] = []

    def publish(self, work_unit: WorkUnit) -> None:
        self.published.append(work_unit)


def _step_unit(step: int = 3) -> WorkUnit:
    return WorkUnit.create(
        image_id=f"{CYCLE_TS}_f{step:03d}",
        source_uri=json.dumps({"cycle": CYCLE.isoformat(), "step_hours": step}),
        data_source_id="gfs_producer",
        processor_id="gfs_grib_download",
        output_prefix=f"{GFS_GRIB_PREFIX}/{CYCLE_TS}",
        bounds=BOUNDS,
        band_id=GFS_MSLP_CONFIG.band_id,
    )


def _downloader(s3, products=None) -> GfsGribDownloader:
    return GfsGribDownloader(
        products=products or ALL_PRODUCTS, s3_client=s3, bounds=BOUNDS
    )


GRIB_KEY = f"{GFS_GRIB_PREFIX}/{CYCLE_TS}/{IMAGE_ID}.grib2"


class TestGribCaching:
    @pytest.mark.asyncio
    async def test_uploads_the_grib_under_the_cycle_prefix(self, tmp_path):
        s3 = FakeS3()
        await _downloader(s3).process(str(tmp_path / "x.grib2"), _step_unit(), FakeMq())
        assert s3.uploaded == [GRIB_KEY]

    @pytest.mark.asyncio
    async def test_key_is_the_one_the_producer_dedups_on(self, tmp_path):
        """The producer LISTs this prefix and parses `_fNNN`; keep them in step."""
        s3 = FakeS3()
        await _downloader(s3).process(str(tmp_path / "x.grib2"), _step_unit(), FakeMq())
        key = s3.uploaded[0]
        assert key.startswith(f"{GFS_GRIB_PREFIX}/{CYCLE_TS}/")
        assert key.endswith("_f003.grib2")

    @pytest.mark.asyncio
    async def test_skips_upload_when_already_cached(self, tmp_path):
        s3 = FakeS3(existing={GRIB_KEY})
        await _downloader(s3).process(str(tmp_path / "x.grib2"), _step_unit(), FakeMq())
        assert s3.uploaded == []

    @pytest.mark.asyncio
    async def test_uploads_anyway_when_the_head_fails(self, tmp_path):
        """An overwrite is idempotent, so a failed check must not skip the PUT."""
        s3 = FakeS3(head_fails=True)
        await _downloader(s3).process(str(tmp_path / "x.grib2"), _step_unit(), FakeMq())
        assert s3.uploaded == [GRIB_KEY]

    @pytest.mark.asyncio
    async def test_a_failed_upload_raises(self, tmp_path):
        class RefusingS3(FakeS3):
            async def upload_file(self, key, file_path):
                return False

        with pytest.raises(RuntimeError, match="Failed to upload GFS GRIB"):
            await _downloader(RefusingS3()).process(
                str(tmp_path / "x.grib2"), _step_unit(), FakeMq()
            )


class TestProductFanOut:
    @pytest.mark.asyncio
    async def test_one_download_enqueues_every_enabled_product(self, tmp_path):
        """The whole point of the single-producer design."""
        mq = FakeMq()
        await _downloader(FakeS3()).process(str(tmp_path / "x.grib2"), _step_unit(), mq)
        assert {u.band_id for u in mq.published} == {
            "gfs_mslp",
            "gfs_500",
            "gfs_250",
        }

    @pytest.mark.asyncio
    async def test_only_enabled_products_are_enqueued(self, tmp_path):
        mq = FakeMq()
        await _downloader(FakeS3(), products=[GFS_MSLP_CONFIG]).process(
            str(tmp_path / "x.grib2"), _step_unit(), mq
        )
        assert [u.band_id for u in mq.published] == ["gfs_mslp"]

    @pytest.mark.asyncio
    async def test_units_point_at_the_cached_grib(self, tmp_path):
        mq = FakeMq()
        await _downloader(FakeS3()).process(str(tmp_path / "x.grib2"), _step_unit(), mq)
        for unit in mq.published:
            assert json.loads(unit.source_uri)["grib_path"] == GRIB_KEY

    @pytest.mark.asyncio
    async def test_units_carry_cycle_step_and_product(self, tmp_path):
        mq = FakeMq()
        await _downloader(FakeS3()).process(
            str(tmp_path / "x.grib2"), _step_unit(step=9), mq
        )
        meta = json.loads(mq.published[0].source_uri)
        assert meta["cycle"] == CYCLE.isoformat()
        assert meta["step_hours"] == 9
        assert meta["product_id"] == GFS_MSLP_CONFIG.product_id

    @pytest.mark.asyncio
    async def test_units_route_to_the_step_source_and_product_processor(self, tmp_path):
        mq = FakeMq()
        await _downloader(FakeS3()).process(str(tmp_path / "x.grib2"), _step_unit(), mq)
        by_band = {u.band_id: u for u in mq.published}
        assert by_band["gfs_mslp"].data_source_id == GFS_STEP_DATA_SOURCE_ID
        assert by_band["gfs_mslp"].processor_id == GFS_MSLP_CONFIG.processor_id
        assert by_band["gfs_500"].processor_id == GFS_500_CONFIG.processor_id
        assert by_band["gfs_250"].processor_id == GFS_500_CONFIG.processor_id

    @pytest.mark.asyncio
    async def test_output_prefix_is_namespaced_by_cycle(self, tmp_path):
        mq = FakeMq()
        await _downloader(FakeS3()).process(str(tmp_path / "x.grib2"), _step_unit(), mq)
        by_band = {u.band_id: u for u in mq.published}
        assert by_band["gfs_500"].output_prefix == (
            f"{GFS_500_CONFIG.tiles_prefix}/{CYCLE_TS}"
        )


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_skips_products_already_rendered(self, tmp_path):
        done = f"{GFS_500_CONFIG.cog_prefix}/{CYCLE_TS}/{IMAGE_ID}.tif"
        mq = FakeMq()
        await _downloader(FakeS3(existing={done})).process(
            str(tmp_path / "x.grib2"), _step_unit(), mq
        )
        assert {u.band_id for u in mq.published} == {"gfs_mslp", "gfs_250"}

    @pytest.mark.asyncio
    async def test_enqueues_nothing_when_every_product_is_done(self, tmp_path):
        existing = {f"{p.cog_prefix}/{CYCLE_TS}/{IMAGE_ID}.tif" for p in ALL_PRODUCTS}
        mq = FakeMq()
        await _downloader(FakeS3(existing=existing)).process(
            str(tmp_path / "x.grib2"), _step_unit(), mq
        )
        assert mq.published == []

    @pytest.mark.asyncio
    async def test_head_failure_re_enqueues_everything(self, tmp_path):
        """Re-rendering wastes work; skipping would silently drop a product."""
        mq = FakeMq()
        await _downloader(FakeS3(head_fails=True)).process(
            str(tmp_path / "x.grib2"), _step_unit(), mq
        )
        assert len(mq.published) == 3


class TestMetrics:
    @pytest.mark.asyncio
    async def test_reports_upload_and_list_stages(self, tmp_path):
        class Collector:
            def __init__(self):
                self.stages = None

            def set_stage_timings(self, stages):
                self.stages = stages

        collector = Collector()
        await _downloader(FakeS3()).process(
            str(tmp_path / "x.grib2"), _step_unit(), FakeMq(), collector
        )
        assert set(collector.stages) == {"upload", "list"}

    @pytest.mark.asyncio
    async def test_upload_time_is_zero_when_the_grib_was_cached(self, tmp_path):
        class Collector:
            stages: dict | None = None

            def set_stage_timings(self, stages):
                self.stages = stages

        collector = Collector()
        await _downloader(FakeS3(existing={GRIB_KEY})).process(
            str(tmp_path / "x.grib2"), _step_unit(), FakeMq(), collector
        )
        assert collector.stages["upload"] == 0.0


class TestCadenceCoverage:
    @pytest.mark.asyncio
    async def test_every_forecast_step_fans_out(self, tmp_path):
        """33 steps x 3 products = 99 rendering units per fully ingested cycle."""
        total = 0
        for step in forecast_steps():
            mq = FakeMq()
            await _downloader(FakeS3()).process(
                str(tmp_path / "x.grib2"), _step_unit(step=step), mq
            )
            total += len(mq.published)
        assert total == 33 * 3
