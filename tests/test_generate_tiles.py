import asyncio
import sys
import os
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from services.generate_tiles import GenerateTilesService


class TestGenerateTilesService:
    """Tests for GenerateTilesService."""

    @pytest.fixture
    def tmp_output_dir(self, tmp_path):
        """Provide a temporary output directory."""
        return tmp_path / "tiles_output"

    @pytest.fixture
    def mock_geotiff_files(self, tmp_path):
        """Create mock GeoTIFF files."""
        geotiff_dir = tmp_path / "geotiffs"
        geotiff_dir.mkdir()
        files = []
        for i in range(3):
            f = geotiff_dir / f"image_{i}.tif"
            f.write_text("mock geotiff content")
            files.append(f)
        return files

    def test_init_sets_attributes(self, mock_geotiff_files, tmp_output_dir):
        """Test that __init__ correctly sets attributes."""
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)

        assert service._geotiff_files == mock_geotiff_files
        assert service._output_dir == tmp_output_dir
        assert service.MAX_CONCURRENT_TILES == 2
        assert service.GDAL_PROCESSES == 2

    @pytest.mark.asyncio
    async def test_run_creates_output_directory(
        self, mock_geotiff_files, tmp_output_dir
    ):
        """Test that run() creates the output directory."""
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)

        with patch.object(
            service, "_generate_tiles_with_limit", new_callable=MagicMock
        ) as mock_gen:
            mock_gen.return_value = asyncio.Future()
            mock_gen.return_value.set_result(None)

            await service.run()

        assert tmp_output_dir.exists()

    @pytest.mark.asyncio
    async def test_run_processes_all_files(self, mock_geotiff_files, tmp_output_dir):
        """Test that run() processes all GeoTIFF files."""
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)

        processed_files = []

        async def track_processing(geotiff_path):
            processed_files.append(geotiff_path)

        with patch.object(
            service, "_generate_tiles_with_limit", side_effect=track_processing
        ):
            await service.run()

        assert len(processed_files) == 3
        for f in mock_geotiff_files:
            assert f in processed_files

    @pytest.mark.asyncio
    async def test_respects_max_concurrent_tiles(self, tmp_path, tmp_output_dir):
        """Test that semaphore limits concurrent tile generation."""
        # Create 5 mock files
        geotiff_files = [tmp_path / f"file_{i}.tif" for i in range(5)]
        for f in geotiff_files:
            f.write_text("mock")

        service = GenerateTilesService(geotiff_files, tmp_output_dir)

        concurrent_count = 0
        max_concurrent = 0

        original_generate = service._generate_tiles

        def track_concurrent(path):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            # Simulate some work
            import time

            time.sleep(0.05)
            concurrent_count -= 1

        with patch.object(service, "_generate_tiles", side_effect=track_concurrent):
            await service.run()

        assert max_concurrent <= service.MAX_CONCURRENT_TILES

    def test_generate_tiles_command_construction(
        self, mock_geotiff_files, tmp_output_dir
    ):
        """Test that gdal2tiles command is constructed correctly."""
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)
        geotiff_path = mock_geotiff_files[0]

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="", stdout="")

            with patch("shutil.rmtree"):
                with patch.object(Path, "rename"):
                    service._generate_tiles(geotiff_path)

            # Verify command structure
            call_args = mock_run.call_args
            cmd = call_args[0][0]

            assert cmd[0] == "gdal2tiles.py"
            assert "-z" in cmd
            assert "3-7" in cmd
            assert "-w" in cmd
            assert "none" in cmd
            assert "--tiledriver=WEBP" in cmd
            assert f"--processes={service.GDAL_PROCESSES}" in cmd
            assert str(geotiff_path) in cmd

    def test_generate_tiles_atomic_rename(self, mock_geotiff_files, tmp_output_dir):
        """Test atomic rename from temp to final directory."""
        tmp_output_dir.mkdir(parents=True)
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)
        geotiff_path = mock_geotiff_files[0]

        rename_calls = []

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            with patch.object(Path, "rename") as mock_rename:
                mock_rename.side_effect = lambda dest: rename_calls.append(dest)

                with patch.object(Path, "mkdir"):
                    with patch.object(Path, "exists", return_value=False):
                        service._generate_tiles(geotiff_path)

            # Verify rename was called with final destination
            assert len(rename_calls) == 1
            expected_final = tmp_output_dir / f"{geotiff_path.stem}_tiles"
            assert rename_calls[0] == expected_final

    def test_generate_tiles_cleanup_on_failure(
        self, mock_geotiff_files, tmp_output_dir
    ):
        """Test that temp directory is cleaned up on failure."""
        tmp_output_dir.mkdir(parents=True)
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)
        geotiff_path = mock_geotiff_files[0]

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="gdal2tiles failed")

            with patch("shutil.rmtree") as mock_rmtree:
                with patch.object(Path, "mkdir"):
                    with patch.object(Path, "exists", return_value=True):
                        with pytest.raises(RuntimeError, match="gdal2tiles failed"):
                            service._generate_tiles(geotiff_path)

                # Verify cleanup was attempted
                mock_rmtree.assert_called()

    def test_generate_tiles_overwrites_existing(
        self, mock_geotiff_files, tmp_output_dir
    ):
        """Test that existing tiles directory is removed before rename."""
        tmp_output_dir.mkdir(parents=True)
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)
        geotiff_path = mock_geotiff_files[0]

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            with patch("shutil.rmtree") as mock_rmtree:
                with patch.object(Path, "mkdir"):
                    with patch.object(Path, "exists", return_value=True):
                        with patch.object(Path, "rename"):
                            service._generate_tiles(geotiff_path)

            # rmtree should be called to remove existing directory
            assert mock_rmtree.called
