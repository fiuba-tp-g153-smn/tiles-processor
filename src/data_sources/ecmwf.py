"""ECMWF weather model data source implementation."""

import asyncio
from ecmwf.opendata import Client
import logging
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Set, List

from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from models.band_config import ProductConfig

logger = logging.getLogger(__name__)


class EcmwfDataSource(DataSource):
    """
    Data source for ECMWF weather model forecasts using ecmwf-opendata.

    This data source:
    - Downloads GRIB files with total precipitation forecasts
    - Polls for new model runs (00Z and 12Z)
    - Handles 144-hour forecasts with 3-hour timesteps
    - Starts polling 7 hours after model run time
    """

    # Polling configuration
    POLLING_START_DELAY_HOURS = 7  # Start checking 7h after run time
    POLLING_INTERVAL_MINUTES = 10  # Check every 10 minutes
    MAX_POLLING_ATTEMPTS = 60  # Max attempts (10 hours of polling)
    LOOKBACK_HOURS = 48  # Search for runs in the last 48 hours

    # Model run times
    MODEL_RUN_HOURS = [0, 12]  # 00Z and 12Z

    # Forecast configuration (must match processor)
    FORECAST_HOURS = 144  # 6-day forecast
    INTERVAL_HOURS = 6  # 6-hour intervals

    def __init__(self, product_config: ProductConfig):
        """
        Initialize ECMWF data source.

        Args:
            product_config: Product configuration for total precipitation
        """
        self._product_config = product_config

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return self._product_config.product_id

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return f"ecmwf_{self._product_config.product_id}"

    @property
    def product_config(self) -> ProductConfig:
        """Get the product configuration."""
        return self._product_config

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new ECMWF model runs that need processing.

        Strategy:
        1. Start with the latest expected model run
        2. Check if it's already processed or in progress - skip if so
        3. Check if enough time has passed (>7h) for it to be available
        4. Check if data is available (single quick check, no extensive polling)
        5. If not found, search backwards through previous runs (up to 36h)
        6. Return the first available, unprocessed run

        This ensures we always make progress even if the latest run is delayed.
        """
        current_time = config.current_time
        # Log what we're checking against
        logger.info(
            f"[{self.source_id}] Discovery check - "
            f"existing_tilesets={sorted(config.existing_tilesets)}, "
            f"in_progress={sorted(config.in_progress_images)}"
        )
        # Generate candidate runs to check (latest first, going back 36 hours)
        candidate_runs = self._get_candidate_runs(current_time)

        for run_time in candidate_runs:
            run_id = self._format_run_id(run_time)

            # Skip if already processed or in progress
            if run_id in config.existing_tilesets or run_id in config.in_progress_images:
                logger.info(
                    f"[{self.source_id}] SKIPPING {run_id} - "
                    f"in_existing={run_id in config.existing_tilesets}, "
                    f"in_progress={run_id in config.in_progress_images}"
                )
                continue

            # Check if enough time has passed since the run
            time_since_run = current_time - run_time
            if time_since_run < timedelta(hours=self.POLLING_START_DELAY_HOURS):
                logger.debug(
                    f"[{self.source_id}] Run {run_id} too recent "
                    f"({time_since_run.total_seconds() / 3600:.1f}h old, need {self.POLLING_START_DELAY_HOURS}h)"
                )
                continue

            # Quick check for data availability (no extensive polling for older runs)
            # Only poll extensively for the most recent run
            is_latest = run_time == candidate_runs[0]
            if is_latest:
                # For latest run, poll extensively
                is_available = await self._poll_for_data_availability(run_time)
            else:
                # For older runs, just do a quick single check
                is_available = await asyncio.to_thread(
                    self._check_data_availability, run_time
                )

            if is_available:
                logger.info(f"[{self.source_id}] Found available run: {run_id}")

                # Return one ImageInfo per interval that isn't already processed/in progress
                image_infos = []
                for start_hour in range(0, self.FORECAST_HOURS, self.INTERVAL_HOURS):
                    end_hour = start_hour + self.INTERVAL_HOURS
                    interval_name = f"{start_hour:03d}-{end_hour:03d}h"
                    interval_id = f"{run_id}_{interval_name}"

                    # Skip if this specific interval is already processed or in progress
                    if interval_id in config.existing_tilesets or interval_id in config.in_progress_images:
                        logger.debug(f"[{self.source_id}] Interval {interval_id} already processed/in progress")
                        continue

                    image_infos.append(
                        ImageInfo(
                            image_id=interval_id,
                            source_uri=run_id,  # Still download once per run
                            data_source_id=self.source_id,
                            processor_id=self.processor_id,
                            output_prefix=self._product_config.s3_prefix,
                        )
                    )

                if image_infos:
                    logger.info(f"[{self.source_id}] Returning {len(image_infos)} intervals for {run_id}")
                    return image_infos
                else:
                    logger.info(f"[{self.source_id}] All intervals for {run_id} already processed")
                    continue
            else:
                logger.debug(f"[{self.source_id}] Run {run_id} not yet available")

        # No available runs found
        logger.info(
            f"[{self.source_id}] No available runs found in the last {self.LOOKBACK_HOURS}h"
        )
        return []

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download ECMWF GRIB file for a specific model run.

        Implements simple caching: if the file already exists at dest_path,
        it is reused instead of downloading again. This is useful because
        multiple intervals from the same run share the same GRIB file.

        Args:
            source_uri: Run identifier (e.g., "2026-02-05T00Z")
            dest_path: Local path to save the downloaded GRIB file

        Returns:
            Path to the downloaded file.
        """
        # Check if file already exists (cache for multiple intervals from same run)
        if dest_path.exists() and dest_path.stat().st_size > 0:
            logger.info(
                f"[{self.source_id}] Reusing cached GRIB file for {source_uri} at {dest_path}"
            )
            return dest_path

        # Parse run time from source_uri
        run_time = self._parse_run_id(source_uri)

        # Ensure parent directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Download using ecmwf-opendata
        # This is a synchronous blocking operation, run in thread pool
        await asyncio.to_thread(
            self._download_grib_file,
            run_time=run_time,
            dest_path=dest_path,
        )

        logger.info(f"[{self.source_id}] Downloaded {source_uri} to {dest_path}")
        return dest_path

    def _get_latest_expected_run(self, current_time: datetime) -> datetime:
        """
        Get the latest model run time we should expect to be available.

        Args:
            current_time: Current UTC time

        Returns:
            The latest model run datetime (00Z or 12Z)
        """
        # Start from today at 00:00 UTC
        today = current_time.replace(hour=0, minute=0, second=0, microsecond=0)

        # Find all possible run times for today and yesterday
        possible_runs = []
        for day_offset in [0, -1]:
            day = today + timedelta(days=day_offset)
            for hour in self.MODEL_RUN_HOURS:
                possible_runs.append(day.replace(hour=hour))

        # Find the latest run that's before current_time
        possible_runs.sort(reverse=True)
        for run_time in possible_runs:
            if run_time <= current_time:
                return run_time

        # Fallback to most recent (shouldn't happen in practice)
        return possible_runs[0]

    def _get_candidate_runs(self, current_time: datetime) -> List[datetime]:
        """
        Get list of candidate model runs to check (latest first).

        Returns all model runs in the last LOOKBACK_HOURS, sorted by most recent first.

        Args:
            current_time: Current UTC time

        Returns:
            List of datetime objects representing model runs (00Z and 12Z)
        """
        candidates = []
        cutoff_time = current_time - timedelta(hours=self.LOOKBACK_HOURS)

        # Generate all possible runs going back LOOKBACK_HOURS
        # Start from current time and go backwards
        check_date = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = cutoff_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

        while check_date >= end_date:
            for hour in self.MODEL_RUN_HOURS:
                run_time = check_date.replace(hour=hour)
                # Only include runs that are:
                # 1. In the past (before current_time)
                # 2. Within the lookback window
                if cutoff_time <= run_time <= current_time:
                    candidates.append(run_time)
            check_date -= timedelta(days=1)

        # Sort by most recent first
        candidates.sort(reverse=True)
        return candidates

    def _format_run_id(self, run_time: datetime) -> str:
        """Format run time as run ID (e.g., '2026-02-05T00Z')."""
        return run_time.strftime("%Y-%m-%dT%HZ")

    def _parse_run_id(self, run_id: str) -> datetime:
        """Parse run ID back to datetime."""
        return datetime.strptime(run_id, "%Y-%m-%dT%HZ").replace(tzinfo=UTC)

    async def _poll_for_data_availability(self, run_time: datetime) -> bool:
        """
        Poll ECMWF API to check if data for a specific run is available.

        Retries every POLLING_INTERVAL_MINUTES up to MAX_POLLING_ATTEMPTS.

        Args:
            run_time: The model run time to check

        Returns:
            True if data is available, False otherwise
        """
        run_id = self._format_run_id(run_time)

        for attempt in range(1, self.MAX_POLLING_ATTEMPTS + 1):
            logger.debug(
                f"[{self.source_id}] Polling attempt {attempt}/{self.MAX_POLLING_ATTEMPTS} "
                f"for {run_id}"
            )

            # Check if data is available (synchronous operation, run in thread pool)
            try:
                is_available = await asyncio.to_thread(
                    self._check_data_availability, run_time
                )

                if is_available:
                    logger.info(
                        f"[{self.source_id}] Data available for {run_id} "
                        f"(attempt {attempt})"
                    )
                    return True
            except Exception as e:
                logger.warning(
                    f"[{self.source_id}] Error checking availability for {run_id}: {e}"
                )

            # Wait before next attempt (unless it's the last one)
            if attempt < self.MAX_POLLING_ATTEMPTS:
                await asyncio.sleep(self.POLLING_INTERVAL_MINUTES * 60)

        logger.error(
            f"[{self.source_id}] Data for {run_id} not available after "
            f"{self.MAX_POLLING_ATTEMPTS} attempts"
        )
        return False

    def _check_data_availability(self, run_time: datetime) -> bool:
        """
        Check if ECMWF data is available for a specific run.

        This is a synchronous method that queries the ecmwf-opendata API.

        Args:
            run_time: The model run time to check

        Returns:
            True if data is available, False otherwise
        """
        import tempfile

        run_id = self._format_run_id(run_time)
        try:
            client = Client(source="aws")  # Changed from "aws" to "ecmwf"

            # Try to download a file to check availability
            # Use step=6 (first 6h forecast) as availability check
            logger.debug(
                f"[{self.source_id}] Checking availability for {run_id} "
                f"(date={run_time.strftime('%Y-%m-%d')}, time={run_time.hour})"
            )

            # Create a temporary file for the download test
            with tempfile.NamedTemporaryFile(delete=True, suffix=".grib") as tmp_file:
                client.retrieve(
                    time=run_time.hour,
                    date=run_time.strftime("%Y-%m-%d"),
                    step=6,  # Check for 6h step (first interval we need)
                    type="fc",  # Forecast
                    param="tp",  # Total precipitation
                    target=tmp_file.name,  # Download to temp file
                )

            # If we get here without exception, data exists
            logger.info(f"[{self.source_id}] Data available for {run_id}")
            return True

        except Exception as e:
            logger.debug(f"[{self.source_id}] Data availability check failed for {run_id}: {type(e).__name__}: {e}")
            return False

    def _download_grib_file(self, run_time: datetime, dest_path: Path) -> None:
        """
        Download GRIB file with all timesteps for a model run.

        This is a synchronous method using ecmwf-opendata.

        Args:
            run_time: The model run time
            dest_path: Where to save the GRIB file
        """
        client = Client(source="aws")

        # Define all steps for 6 days (144 hours) at 3-hour intervals
        # Steps: 3, 6, 9, 12, ..., 144
        steps = list(range(3, 145, 3))

        logger.info(
            f"[{self.source_id}] Downloading GRIB for {self._format_run_id(run_time)} "
            f"({len(steps)} timesteps)"
        )

        # Download all steps in one request (library handles concatenation)
        client.retrieve(
            time=run_time.hour,
            date=run_time.strftime("%Y-%m-%d"),
            step=steps,
            type="fc",  # Forecast
            param="tp",  # Total precipitation
            target=str(dest_path),
        )

        logger.info(
            f"[{self.source_id}] Download complete: {dest_path} "
            f"({dest_path.stat().st_size / 1024 / 1024:.1f} MB)"
        )
