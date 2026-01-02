import asyncio
import logging
import sys
import time

from jobs.process_band_13_job import ProcessBand13Job

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logging.Formatter.converter = time.gmtime

AVAILABLE_JOBS = {
    "process_band_13": ProcessBand13Job,
}


async def main():
    if len(sys.argv) < 2:
        logging.error("Usage: python3 ./main.py <job_name>")
        return

    job_name = sys.argv[1]
    job_class = AVAILABLE_JOBS.get(job_name)
    if not job_class:
        logging.error(f"Job '{job_name}' not found.")
        return

    job_instance = job_class()
    await job_instance.run()


if __name__ == "__main__":
    asyncio.run(main())
