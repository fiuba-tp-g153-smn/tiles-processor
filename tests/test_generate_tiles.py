import asyncio
import sys
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import pytest
from services.generate_tiles import GenerateTilesService
from services.processing_steps import run_gdal2tiles


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

        with patch("services.generate_tiles.run_gdal2tiles") as mock_gdal:
            mock_gdal.return_value = tmp_output_dir / "tiles"
            await service.run()

        assert tmp_output_dir.exists()

    @pytest.mark.asyncio
    async def test_run_processes_all_files(self, mock_geotiff_files, tmp_output_dir):
        """Test that run() processes all GeoTIFF files."""
        service = GenerateTilesService(mock_geotiff_files, tmp_output_dir)

        processed_paths = []

        def track_processing(geotiff_path, output_dir, **kwargs):
            processed_paths.append(geotiff_path)
            return output_dir / f"{geotiff_path.stem}_tiles"

        with patch(
            "services.generate_tiles.run_gdal2tiles", side_effect=track_processing
        ):
            await service.run()

        assert len(processed_paths) == 3
        for f in mock_geotiff_files:
            assert f in processed_paths

    @pytest.mark.asyncio
    async def test_respects_max_concurrent_tiles(self, tmp_path, tmp_output_dir):
        """Test that concurrency limits are respected during tile generation."""
        # Create 5 mock files
        geotiff_files = [tmp_path / f"file_{i}.tif" for i in range(5)]
        for f in geotiff_files:
            f.write_text("mock")

        service = GenerateTilesService(geotiff_files, tmp_output_dir)

        concurrent_count = 0
        max_concurrent = 0

        def track_concurrent(geotiff_path, output_dir, **kwargs):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            import time

            time.sleep(0.05)
            concurrent_count -= 1
            return output_dir / f"{geotiff_path.stem}_tiles"

        with patch(
            "services.generate_tiles.run_gdal2tiles", side_effect=track_concurrent
        ):
            await service.run()

        assert max_concurrent <= service.MAX_CONCURRENT_TILES


class TestRunGdal2Tiles:
    """Tests for the shared run_gdal2tiles function."""

    @pytest.fixture
    def tmp_output_dir(self, tmp_path):
        """Provide a temporary output directory."""
        output_dir = tmp_path / "tiles_output"
        output_dir.mkdir(parents=True)
        return output_dir

    @pytest.fixture
    def mock_geotiff(self, tmp_path):
        """Create a mock GeoTIFF file."""
        geotiff_dir = tmp_path / "geotiffs"
        geotiff_dir.mkdir()
        f = geotiff_dir / "image_0.tif"
        f.write_text("mock geotiff content")
        return f

    def test_command_construction(self, mock_geotiff, tmp_output_dir):
        """Test that gdal2tiles command is constructed correctly."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="", stdout="")

            with patch("pathlib.Path.rename"):
                run_gdal2tiles(mock_geotiff, tmp_output_dir)

            # Verify command structure
            call_args = mock_run.call_args
            cmd = call_args[0][0]

            assert cmd[0] == "gdal2tiles.py"
            assert "-z" in cmd
            assert "3-7" in cmd
            assert "-w" in cmd
            assert "none" in cmd
            assert "--tiledriver=WEBP" in cmd
            assert "--processes=2" in cmd
            assert str(mock_geotiff) in cmd

    def test_atomic_rename(self, mock_geotiff, tmp_output_dir):
        """Test atomic rename from temp to final directory."""
        rename_calls = []

        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            with patch.object(Path, "rename") as mock_rename:
                mock_rename.side_effect = lambda dest: rename_calls.append(dest)

                with patch.object(Path, "mkdir"):
                    with patch.object(Path, "exists", return_value=False):
                        run_gdal2tiles(mock_geotiff, tmp_output_dir)

            # Verify rename was called with final destination
            assert len(rename_calls) == 1
            expected_final = tmp_output_dir / f"{mock_geotiff.stem}_tiles"
            assert rename_calls[0] == expected_final

    def test_cleanup_on_failure(self, mock_geotiff, tmp_output_dir):
        """Test that temp directory is cleaned up on failure."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="gdal2tiles failed")

            with patch("services.processing_steps.shutil.rmtree") as mock_rmtree:
                with patch.object(Path, "mkdir"):
                    with patch.object(Path, "exists", return_value=True):
                        with pytest.raises(RuntimeError, match="gdal2tiles failed"):
                            run_gdal2tiles(mock_geotiff, tmp_output_dir)

                # Verify cleanup was attempted
                mock_rmtree.assert_called()

    def test_overwrites_existing(self, mock_geotiff, tmp_output_dir):
        """Test that existing tiles directory is removed before rename."""
        with patch("services.processing_steps.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            with patch("services.processing_steps.shutil.rmtree") as mock_rmtree:
                with patch.object(Path, "mkdir"):
                    with patch.object(Path, "exists", return_value=True):
                        with patch.object(Path, "rename"):
                            run_gdal2tiles(mock_geotiff, tmp_output_dir)

            # rmtree should be called to remove existing directory
            assert mock_rmtree.called
