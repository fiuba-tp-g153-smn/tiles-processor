"""Tests for GOES-19 GLM data source window completeness filtering."""

import sys
import os
from datetime import datetime, UTC, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest

from data_sources.goes19_glm import Goes19GlmDataSource
from data_sources.base import DiscoveryConfig
from models.band_config import BandConfig


@pytest.fixture
def band_config():
    """Create a mock band config for GLM."""
    config = MagicMock(spec=BandConfig)
    config.band_id = "glm_fed"
    config.s3_prefix = "glm-fed"
    return config


@pytest.fixture
def glm_source(band_config):
    """Create a GLM data source with mocked S3 client."""
    source = Goes19GlmDataSource(band_config)
    source._s3_client = AsyncMock()
    return source


def _make_glm_filename(file_time: datetime) -> str:
    """
    Generate a GLM L2-LCFA filename for a given time.

    Format: OR_GLM-L2-LCFA_G19_s<start>_e<end>_c<creation>.nc
    Where timestamps use format: YYYYdddHHMMSS (ddd = day of year)
    """
    day_of_year = file_time.timetuple().tm_yday
    start_str = f"{file_time.year}{day_of_year:03d}{file_time.hour:02d}{file_time.minute:02d}{file_time.second:02d}"
    end_time = file_time + timedelta(seconds=20)
    end_day_of_year = end_time.timetuple().tm_yday
    end_str = f"{end_time.year}{end_day_of_year:03d}{end_time.hour:02d}{end_time.minute:02d}{end_time.second:02d}"
    creation_str = end_str  # Simplification: creation time = end time

    return f"OR_GLM-L2-LCFA_G19_s{start_str}_e{end_str}_c{creation_str}.nc"


def _make_glm_s3_key(file_time: datetime) -> str:
    """Generate full S3 key path for GLM file."""
    year = file_time.year
    day_of_year = file_time.timetuple().tm_yday
    hour = file_time.hour
    filename = _make_glm_filename(file_time)
    return f"GLM-L2-LCFA/{year}/{day_of_year:03d}/{hour:02d}/{filename}"


class TestWindowCompletenessFilter:
    """Test suite for window completeness filtering logic."""

    @pytest.mark.asyncio
    async def test_incomplete_window_excluded(self, glm_source):
        """
        Test that a window started less than 10 minutes ago is excluded.

        Scenario: Current time is 12:08, window started at 12:00
        Expected: Window is excluded (only 8 minutes elapsed)
        """
        # Setup: Current time is 12:08 UTC
        current_time = datetime(2026, 2, 13, 12, 8, 0, tzinfo=UTC)

        # Create files in 12:00-12:10 window (but only up to 12:08)
        files = []
        window_start = datetime(2026, 2, 13, 12, 0, 0)
        for minute in range(0, 8):  # 12:00 to 12:08 (8 minutes worth)
            for second in [0, 20, 40]:
                file_time = window_start + timedelta(minutes=minute, seconds=second)
                files.append(_make_glm_s3_key(file_time))

        # Mock S3 to return these files
        glm_source._collect_candidates_from_hourly_paths = AsyncMock(return_value=files)

        # Run discovery
        config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=set(),
            in_progress_images=set(),
            bounds={"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        )

        result = await glm_source.discover_images(config)

        # Assert: No windows returned (the only window is incomplete)
        assert len(result) == 0, "Incomplete window should be filtered out"

    @pytest.mark.asyncio
    async def test_complete_window_included(self, glm_source):
        """
        Test that a window started 10+ minutes ago is included.

        Scenario: Current time is 12:11, window started at 12:00
        Expected: Window is included (11 minutes elapsed)
        """
        # Setup: Current time is 12:11 UTC
        current_time = datetime(2026, 2, 13, 12, 11, 0, tzinfo=UTC)

        # Create files in 12:00-12:10 window (full window)
        files = []
        window_start = datetime(2026, 2, 13, 12, 0, 0)
        for minute in range(0, 10):  # Full 10 minutes
            for second in [0, 20, 40]:
                file_time = window_start + timedelta(minutes=minute, seconds=second)
                files.append(_make_glm_s3_key(file_time))

        # Mock S3 to return these files
        glm_source._collect_candidates_from_hourly_paths = AsyncMock(return_value=files)

        # Run discovery
        config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=set(),
            in_progress_images=set(),
            bounds={"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        )

        result = await glm_source.discover_images(config)

        # Assert: One window returned
        assert len(result) == 1, "Complete window should be included"
        assert (
            result[0].image_id == "20260213120000"
        )  # Nuevo formato: YYYYMMDDHHMMSS

    @pytest.mark.asyncio
    async def test_exact_boundary_included(self, glm_source):
        """
        Test that a window at exactly 10 minutes elapsed is included.

        Scenario: Current time is 12:10:00, window started at 12:00:00
        Expected: Window is included (exactly 10 minutes elapsed, using <=)
        """
        # Setup: Current time is exactly 12:10 UTC
        current_time = datetime(2026, 2, 13, 12, 10, 0, tzinfo=UTC)

        # Create files in 12:00-12:10 window
        files = []
        window_start = datetime(2026, 2, 13, 12, 0, 0)
        for minute in range(0, 10):
            file_time = window_start + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Mock S3 to return these files
        glm_source._collect_candidates_from_hourly_paths = AsyncMock(return_value=files)

        # Run discovery
        config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=set(),
            in_progress_images=set(),
            bounds={"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        )

        result = await glm_source.discover_images(config)

        # Assert: Window is included at exact boundary
        assert len(result) == 1, "Window at exact 10-minute boundary should be included"

    @pytest.mark.asyncio
    async def test_multiple_windows_filtering(self, glm_source):
        """
        Test filtering with multiple windows at different ages.

        Scenario: Current time is 12:25
        Windows: 12:00-12:10 (complete), 12:10-12:20 (complete), 12:20-12:30 (incomplete)
        Expected: First two windows included, last one excluded
        """
        # Setup: Current time is 12:25 UTC
        current_time = datetime(2026, 2, 13, 12, 25, 0, tzinfo=UTC)

        # Create files across three windows
        files = []
        base_time = datetime(2026, 2, 13, 12, 0, 0)

        # Window 1: 12:00-12:10 (complete)
        for minute in range(0, 10):
            file_time = base_time + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Window 2: 12:10-12:20 (complete)
        for minute in range(10, 20):
            file_time = base_time + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Window 3: 12:20-12:30 (incomplete - only 5 minutes so far)
        for minute in range(20, 25):
            file_time = base_time + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Mock S3 to return these files
        glm_source._collect_candidates_from_hourly_paths = AsyncMock(return_value=files)

        # Run discovery
        config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=set(),
            in_progress_images=set(),
            bounds={"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        )

        result = await glm_source.discover_images(config)

        # Assert: Only first two windows returned (sorted descending by time)
        assert len(result) == 2, "Two complete windows should be included"
        # Results are sorted descending, so 12:10 window comes first
        assert result[0].image_id == "20260213121000"  # 12:10 window
        assert result[1].image_id == "20260213120000"  # 12:00 window

    @pytest.mark.asyncio
    async def test_empty_result_all_incomplete(self, glm_source):
        """
        Test that an empty list is returned when all windows are incomplete.

        Scenario: Current time is 12:05, only one window 12:00-12:10 exists
        Expected: Empty list (window is incomplete)
        """
        # Setup: Current time is 12:05 UTC
        current_time = datetime(2026, 2, 13, 12, 5, 0, tzinfo=UTC)

        # Create files in 12:00-12:10 window (but only 5 minutes worth)
        files = []
        window_start = datetime(2026, 2, 13, 12, 0, 0)
        for minute in range(0, 5):
            file_time = window_start + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Mock S3 to return these files
        glm_source._collect_candidates_from_hourly_paths = AsyncMock(return_value=files)

        # Run discovery
        config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=set(),
            in_progress_images=set(),
            bounds={"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        )

        result = await glm_source.discover_images(config)

        # Assert: Empty result
        assert len(result) == 0, "No complete windows should return empty list"

    @pytest.mark.asyncio
    async def test_timezone_handling(self, glm_source):
        """
        Test that UTC-aware current_time works correctly with naive window_start.

        This verifies the .replace(tzinfo=None) timezone stripping works properly.
        """
        # Setup: Current time is UTC-aware
        current_time = datetime(2026, 2, 13, 12, 15, 0, tzinfo=UTC)

        # Create files in 12:00-12:10 window
        files = []
        window_start = datetime(2026, 2, 13, 12, 0, 0)  # Naive datetime
        for minute in range(0, 10):
            file_time = window_start + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Mock S3 to return these files
        glm_source._collect_candidates_from_hourly_paths = AsyncMock(return_value=files)

        # Run discovery
        config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=set(),
            in_progress_images=set(),
            bounds={"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        )

        # Should not raise TypeError about comparing naive and aware datetimes
        result = await glm_source.discover_images(config)

        # Assert: Window is included (15 minutes elapsed)
        assert len(result) == 1, "Timezone handling should work correctly"

    @pytest.mark.asyncio
    async def test_existing_tilesets_still_filtered(self, glm_source):
        """
        Test that existing_tilesets filtering still works after completeness filter.

        Verify the completeness filter doesn't break the existing duplicate prevention.
        """
        # Setup: Current time is 12:20 (both windows complete)
        current_time = datetime(2026, 2, 13, 12, 20, 0, tzinfo=UTC)

        # Create files for two complete windows
        files = []
        base_time = datetime(2026, 2, 13, 12, 0, 0)

        for minute in range(0, 20):  # Two 10-minute windows
            file_time = base_time + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Mock S3 to return these files
        glm_source._collect_candidates_from_hourly_paths = AsyncMock(return_value=files)

        # Mark first window as already processed
        existing_tilesets = {"2026044120000"}  # 12:00 window

        # Run discovery
        config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=existing_tilesets,
            in_progress_images=set(),
            bounds={"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        )

        result = await glm_source.discover_images(config)

        # Assert: Only second window returned (first filtered by existing_tilesets)
        assert len(result) == 1, "existing_tilesets filter should still work"
        assert result[0].image_id == "20260213121000"  # 12:10 window only


class TestWindowGrouping:
    """Test suite for window grouping logic (not changed, but verify still works)."""

    def test_group_files_into_10min_windows(self, glm_source):
        """Test that files are correctly grouped into 10-minute windows."""
        # Create files spanning 12:00 to 12:25
        files = []
        base_time = datetime(2026, 2, 13, 12, 0, 0)

        for minute in range(0, 25):
            file_time = base_time + timedelta(minutes=minute)
            files.append(_make_glm_s3_key(file_time))

        # Group into windows
        windows = glm_source._group_into_windows(files)

        # Should have 3 windows: 12:00, 12:10, 12:20
        assert len(windows) == 3, "Should group into 3 ten-minute windows"

        # Check window start times
        window_starts = sorted([w[0] for w in windows])
        expected_starts = [
            datetime(2026, 2, 13, 12, 0, 0),
            datetime(2026, 2, 13, 12, 10, 0),
            datetime(2026, 2, 13, 12, 20, 0),
        ]
        assert window_starts == expected_starts, "Window start times should be correct"

        # Check file counts per window
        window_dict = {start: files for start, files in windows}
        assert len(window_dict[datetime(2026, 2, 13, 12, 0, 0)]) == 10  # 12:00-12:09
        assert len(window_dict[datetime(2026, 2, 13, 12, 10, 0)]) == 10  # 12:10-12:19
        assert len(window_dict[datetime(2026, 2, 13, 12, 20, 0)]) == 5  # 12:20-12:24
