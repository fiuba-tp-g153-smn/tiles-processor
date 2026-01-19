import asyncio
import os
import sys
import time
import shutil
import random
import csv
from pathlib import Path
from typing import Optional

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from clients.s3_client import S3Client
from config import Config


class BenchmarkingS3Client(S3Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.download_timings = []  # List of (filename, ms)

    async def download_file(
        self,
        s3_client,
        relative_file_path: str,
        retries: int = 3,
        local_cache_dir: Optional[Path] = None,
    ):
        start = time.perf_counter()
        result = await super().download_file(
            s3_client, relative_file_path, retries, local_cache_dir
        )
        end = time.perf_counter()
        duration_ms = (end - start) * 1000
        self.download_timings.append((relative_file_path, duration_ms))
        return result


async def run_test(scenario_name, num_files, file_size_mb, config):
    print(f"\n[{scenario_name}] Initializing...")
    print(f"Files: {num_files}")
    print(f"Size per file: {file_size_mb:.2f} MB")
    print(f"Total Size: {num_files * file_size_mb:.2f} MB")

    test_dir = Path(f"benchmark_{scenario_name}_data")
    download_dir = Path(f"benchmark_{scenario_name}_download")
    s3_prefix = f"benchmark_{scenario_name}"

    # Use the subclass
    client = BenchmarkingS3Client.create_with_credentials(
        bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
        endpoint=config.S3_TILES_DATA_ENDPOINT,
        access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
        secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
        secure=config.S3_TILES_DATA_SECURE,
    )

    await client.ensure_bucket_exists()

    try:
        # Cleanup local
        def harmless_cleanup(path: Path):
            if path.exists():
                try:
                    shutil.rmtree(path, ignore_errors=True)
                except Exception as e:
                    print(f"Warning: Failed to cleanup {path}: {e}")

        harmless_cleanup(test_dir)
        harmless_cleanup(download_dir)

        test_dir.mkdir(parents=True, exist_ok=True)
        download_dir.mkdir(parents=True, exist_ok=True)

        # GENERATE
        print(f"[{scenario_name}] Generating data...")
        block_size = 1024 * 1024  # 1MB
        trash_data = os.urandom(min(int(file_size_mb * 1024 * 1024), block_size))

        if file_size_mb < 1:
            trash_data = trash_data[: int(file_size_mb * 1024 * 1024)]

        for i in range(num_files):
            p = test_dir / f"test_{i:05d}.bin"
            with open(p, "wb") as f:
                remaining_bytes = int(file_size_mb * 1024 * 1024)
                while remaining_bytes > 0:
                    write_size = min(remaining_bytes, len(trash_data))
                    f.write(trash_data[:write_size])
                    remaining_bytes -= write_size

        # UPLOAD
        print(f"[{scenario_name}] Uploading...")
        start_time = time.time()
        count = await client.upload_directory(test_dir, s3_prefix)
        end_time = time.time()
        upload_duration = end_time - start_time

        print(f"[{scenario_name}] Uploaded {count} files in {upload_duration:.2f}s")
        if upload_duration > 0:
            print(
                f"[{scenario_name}] Upload Speed: {(num_files * file_size_mb) / upload_duration:.2f} MB/s"
            )

        # DOWNLOAD
        print(f"[{scenario_name}] Downloading...")
        start_time = time.time()
        await client.download_folder(f"{s3_prefix}/", local_cache_dir=download_dir)
        end_time = time.time()
        download_duration = end_time - start_time

        print(f"[{scenario_name}] Downloaded in {download_duration:.2f}s")
        if download_duration > 0:
            print(
                f"[{scenario_name}] Download Speed: {(num_files * file_size_mb) / download_duration:.2f} MB/s"
            )

        # SAVE TIMINGS CSV
        if scenario_name == "many_small":
            csv_filename = "benchmark_many_small_timings.csv"
            print(f"[{scenario_name}] Writing timings to {csv_filename}...")
            with open(csv_filename, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["filename", "duration_ms"])
                writer.writerows(client.download_timings)

        # CLEANUP S3
        print(f"[{scenario_name}] Cleaning S3...")
        await client.delete_prefix(s3_prefix)

    finally:
        harmless_cleanup(test_dir)
        harmless_cleanup(download_dir)


async def benchmark():
    config = Config()

    # 1. 5 files of 500MB
    # await run_test("heavy", 5, 500, config)

    # 2. 10000 files of 100KB (0.1 MB)
    await run_test("many_small", 10000, 0.1, config)


if __name__ == "__main__":
    asyncio.run(benchmark())
