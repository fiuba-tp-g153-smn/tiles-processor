"""Tests for the GFS producer data source: eligibility, dedup and per-tick cap."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from data_sources.base import DiscoveryConfig
from data_sources.gfs_producer_source import GfsProducerDataSource
from models.gfs_config import (
    GFS_GRIB_PREFIX,
    GFS_INLINE_PROCESSOR_ID,
    GFS_PRODUCER_DATA_SOURCE_ID,
    forecast_steps,
)

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
ALL_STEPS = forecast_steps()


class FakeS3:
    """Records LIST calls and replays a canned set of cached keys."""

    def __init__(self, cached_keys: list[str] | None = None, fail: bool = False):
        self._cached = cached_keys or []
        self._fail = fail
        self.list_calls: list[str] = []

    async def list_files(self, folder_path: str, file_pattern: str) -> list[str]:
        self.list_calls.append(folder_path)
        if self._fail:
            raise RuntimeError("S3 unavailable")
        return [
            k for k in self._cached if k.startswith(folder_path) and file_pattern in k
        ]


class FakeFetcher:
    """Stands in for the fetcher, recording what it was asked to fetch."""

    def __init__(self):
        self.calls: list[tuple[datetime, int]] = []

    async def fetch(self, cycle, step_hours, dest):
        self.calls.append((cycle, step_hours))
        return dest.with_suffix(".grib2")


def _source(s3=None, fetcher=None, **overrides) -> GfsProducerDataSource:
    kwargs = {
        "cycles_to_maintain": 3,
        "max_steps_per_tick": 12,
        "probe_from_hours": 3,
        "probe_to_hours": 8,
    }
    kwargs.update(overrides)
    return GfsProducerDataSource(
        fetcher=fetcher or FakeFetcher(), s3_client=s3 or FakeS3(), **kwargs
    )


def _discovery(now: datetime = NOW, in_progress=None) -> DiscoveryConfig:
    return DiscoveryConfig(
        current_time=now,
        existing_tilesets=set(),
        in_progress_images=in_progress or set(),
        bounds={"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
    )


def _cached_key(cycle: datetime, step: int) -> str:
    return (
        f"{GFS_GRIB_PREFIX}/{cycle:%Y%m%dT%H%MZ}/{cycle:%Y%m%dT%H%MZ}_f{step:03d}.grib2"
    )


class TestIdentity:
    def test_source_and_processor_ids(self):
        source = _source()
        assert source.source_id == GFS_PRODUCER_DATA_SOURCE_ID
        assert source.processor_id == GFS_INLINE_PROCESSOR_ID


class TestCandidateCycles:
    def test_returns_the_n_most_recent_cycles_newest_first(self):
        cycles = _source()._candidate_cycles(NOW)
        assert cycles == [
            datetime(2026, 8, 8, 12, tzinfo=UTC),
            datetime(2026, 8, 8, 6, tzinfo=UTC),
            datetime(2026, 8, 8, 0, tzinfo=UTC),
        ]

    def test_honours_cycles_to_maintain(self):
        assert len(_source(cycles_to_maintain=1)._candidate_cycles(NOW)) == 1

    def test_walks_back_across_midnight(self):
        cycles = _source()._candidate_cycles(datetime(2026, 8, 8, 1, tzinfo=UTC))
        assert cycles[0] == datetime(2026, 8, 8, 0, tzinfo=UTC)
        assert cycles[-1] == datetime(2026, 8, 7, 12, tzinfo=UTC)

    def test_snaps_a_mid_cycle_clock_down_to_the_last_cycle(self):
        cycles = _source()._candidate_cycles(datetime(2026, 8, 8, 14, 37, tzinfo=UTC))
        assert cycles[0] == datetime(2026, 8, 8, 12, tzinfo=UTC)


class TestEligibilityWindow:
    """Cycles that cannot exist yet, or never showed up, must not be emitted."""

    @pytest.mark.asyncio
    async def test_skips_a_cycle_younger_than_the_window(self):
        # 12Z at 14:00Z is 2h old — below probe_from of 3h.
        source = _source(cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 14, tzinfo=UTC))
        )
        assert emitted == []

    @pytest.mark.asyncio
    async def test_emits_once_the_cycle_is_old_enough(self):
        source = _source(cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 16, tzinfo=UTC))
        )
        assert len(emitted) == 12

    @pytest.mark.asyncio
    async def test_abandons_old_cycles_that_never_produced_anything(self):
        """A run NCEP skipped must not be re-attempted on every tick forever.

        At 23:00Z the candidates are 18Z (5h old, inside the window), 12Z (11h)
        and 06Z (17h). With nothing cached, only 18Z is worth attempting — the
        older two are abandoned rather than retried for ever.
        """
        source = _source(max_steps_per_tick=1000, cycles_to_maintain=3)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 23, tzinfo=UTC))
        )
        cycles = {json.loads(i.source_uri)["cycle"] for i in emitted}
        assert cycles == {datetime(2026, 8, 8, 18, tzinfo=UTC).isoformat()}

    @pytest.mark.asyncio
    async def test_the_newest_candidate_is_never_abandoned_by_age(self):
        """The newest candidate ages 0-6h, so it can never pass probe_to (8h).

        Past +6h a fresh cycle takes over as newest, which is why the abandon
        rule only ever applies to the second and third candidates.
        """
        source = _source(max_steps_per_tick=1, cycles_to_maintain=1)
        for offset in (3, 4, 5):
            now = datetime(2026, 8, 8, 6, tzinfo=UTC) + timedelta(hours=offset)
            assert await source.discover_images(_discovery(now=now)), now

    @pytest.mark.asyncio
    async def test_keeps_filling_an_old_cycle_that_has_cached_steps(self):
        """The run demonstrably exists, so age must not stop the fill."""
        cycle = datetime(2026, 8, 8, 12, tzinfo=UTC)
        s3 = FakeS3([_cached_key(cycle, 0)])
        source = _source(s3=s3, cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 23, tzinfo=UTC))
        )
        assert len(emitted) == 12


class TestDedup:
    @pytest.mark.asyncio
    async def test_skips_steps_already_cached(self):
        cycle = datetime(2026, 8, 8, 6, tzinfo=UTC)
        s3 = FakeS3([_cached_key(cycle, s) for s in (0, 3, 6)])
        source = _source(s3=s3, cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 11, tzinfo=UTC))
        )
        steps = [json.loads(i.source_uri)["step_hours"] for i in emitted]
        assert 0 not in steps and 3 not in steps and 6 not in steps
        assert steps[0] == 9

    @pytest.mark.asyncio
    async def test_emits_nothing_when_the_cycle_is_complete(self):
        cycle = datetime(2026, 8, 8, 6, tzinfo=UTC)
        s3 = FakeS3([_cached_key(cycle, s) for s in ALL_STEPS])
        source = _source(s3=s3, cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 11, tzinfo=UTC))
        )
        assert emitted == []

    @pytest.mark.asyncio
    async def test_uses_one_list_per_cycle_not_one_head_per_step(self):
        """33 HEADs per cycle per tick would be 28k pointless calls a day."""
        s3 = FakeS3()
        source = _source(s3=s3, cycles_to_maintain=3)
        await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 23, tzinfo=UTC))
        )
        assert len(s3.list_calls) <= 3

    @pytest.mark.asyncio
    async def test_skips_steps_already_in_progress(self):
        cycle = datetime(2026, 8, 8, 6, tzinfo=UTC)
        in_progress = {f"{cycle:%Y%m%dT%H%MZ}_f000", f"{cycle:%Y%m%dT%H%MZ}_f003"}
        source = _source(cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(
                now=datetime(2026, 8, 8, 11, tzinfo=UTC), in_progress=in_progress
            )
        )
        assert {i.image_id for i in emitted}.isdisjoint(in_progress)

    @pytest.mark.asyncio
    async def test_a_failed_listing_treats_everything_as_missing(self):
        """Fail-safe: the per-tick cap keeps that from becoming a burst."""
        source = _source(s3=FakeS3(fail=True), cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 11, tzinfo=UTC))
        )
        assert len(emitted) == 12


class TestPerTickCap:
    """The guard that bounds the cold-start burst regardless of concurrency."""

    @pytest.mark.asyncio
    async def test_caps_total_emissions_across_all_cycles(self):
        source = _source(max_steps_per_tick=12, cycles_to_maintain=3)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 23, tzinfo=UTC))
        )
        assert len(emitted) == 12

    @pytest.mark.asyncio
    async def test_cold_start_burst_is_one_cycle_not_three(self):
        """What the cap actually protects against, uncapped.

        The eligibility window already rules out the two older candidates on a
        cold start (nothing cached, past their window), so the worst case is one
        cycle's 33 steps — not the 3 x 33 = 99 a naive reading would suggest.
        """
        source = _source(max_steps_per_tick=1000, cycles_to_maintain=3)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 23, tzinfo=UTC))
        )
        assert len(emitted) == len(ALL_STEPS) == 33

    @pytest.mark.asyncio
    async def test_newest_cycle_is_filled_first(self):
        """With two cycles in flight, the budget goes to the newer one."""
        older = datetime(2026, 8, 8, 12, tzinfo=UTC)
        newer = datetime(2026, 8, 8, 18, tzinfo=UTC)
        # Both cycles have produced something, so neither is abandoned.
        s3 = FakeS3([_cached_key(older, 0), _cached_key(newer, 0)])
        source = _source(s3=s3, max_steps_per_tick=5, cycles_to_maintain=3)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 23, tzinfo=UTC))
        )
        cycles = {json.loads(i.source_uri)["cycle"] for i in emitted}
        assert cycles == {newer.isoformat()}

    @pytest.mark.asyncio
    async def test_cap_applies_after_dedup_so_the_fill_progresses(self):
        """Truncating before the dedup would spend the budget on cached steps.

        With the first 12 steps already cached and a cap of 12, a tick must
        still emit 12 *new* steps — not zero.
        """
        cycle = datetime(2026, 8, 8, 6, tzinfo=UTC)
        s3 = FakeS3([_cached_key(cycle, s) for s in ALL_STEPS[:12]])
        source = _source(s3=s3, max_steps_per_tick=12, cycles_to_maintain=1)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 11, tzinfo=UTC))
        )
        steps = [json.loads(i.source_uri)["step_hours"] for i in emitted]
        assert len(steps) == 12
        assert set(steps).isdisjoint(ALL_STEPS[:12])


class TestEmittedImageInfo:
    @pytest.mark.asyncio
    async def test_carries_cycle_and_step_for_the_fetcher(self):
        source = _source(max_steps_per_tick=1, cycles_to_maintain=1)
        info = (
            await source.discover_images(
                _discovery(now=datetime(2026, 8, 8, 11, tzinfo=UTC))
            )
        )[0]
        meta = json.loads(info.source_uri)
        assert meta["cycle"] == datetime(2026, 8, 8, 6, tzinfo=UTC).isoformat()
        assert meta["step_hours"] == 0

    @pytest.mark.asyncio
    async def test_image_ids_are_unique_per_cycle_and_step(self):
        source = _source(max_steps_per_tick=99, cycles_to_maintain=3)
        emitted = await source.discover_images(
            _discovery(now=datetime(2026, 8, 8, 23, tzinfo=UTC))
        )
        assert len({i.image_id for i in emitted}) == len(emitted)

    @pytest.mark.asyncio
    async def test_image_id_encodes_cycle_and_step(self):
        source = _source(max_steps_per_tick=1, cycles_to_maintain=1)
        info = (
            await source.discover_images(
                _discovery(now=datetime(2026, 8, 8, 11, tzinfo=UTC))
            )
        )[0]
        assert info.image_id == "20260808T0600Z_f000"


class TestDownload:
    @pytest.mark.asyncio
    async def test_delegates_to_the_fetcher(self, tmp_path):
        fetcher = FakeFetcher()
        source = _source(fetcher=fetcher)
        cycle = datetime(2026, 8, 8, 6, tzinfo=UTC)
        await source.download(
            json.dumps({"cycle": cycle.isoformat(), "step_hours": 9}),
            tmp_path / "unit",
        )
        assert fetcher.calls == [(cycle, 9)]
