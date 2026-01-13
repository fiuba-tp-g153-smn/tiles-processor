# Tiles Processor

This project is a Python-based scheduler application designed to process GOES-19 satellite imagery. It automates the retrieval of ABI-L1b-RadF products (Full Disk Radiance) from an S3 bucket, processes specific spectral bands to compute brightness temperatures, and generates GeoTIFFs and map tiles for visualization.

## Features

- **Automated Scheduling**: Uses `APScheduler` to run processing jobs at defined intervals (cron).
- **Data Products**:
    - **Band 13 (Clean IR Window)**: Processes Channel 13 (10.33 µm) for Cloud Top monitoring.
    - **Band 9 (Mid-Level Water Vapor)**: Processes Channel 9 (6.93 µm) for Water Vapor analysis.
- **Processing Pipeline**:
    1.  Downloads raw NetCDF/H5 files from S3.
    2.  Geferences the satellite data.
    3.  Computes brightness temperatures.
    4.  Generates colorized GeoTIFFs.
    5.  Produces XYZ tiles for web mapping.
- **Radar Tools**: Includes standalone scripts (`radar_to_tiles.py`) for processing radar data (H5) into tiles.

## Commands

The project uses a `Makefile` to simplify common operations:

- `make up`: Starts the application in development mode using Docker Compose.
- `make down`: Stops and removes the application containers.
- `make prod`: Starts the application in production mode.
- `make test`: Runs the test suite using `pytest` with coverage reporting.

## Environment Variables

Configuration is managed via environment variables. Copy `.env.example` to `.env` and adjust as needed:

| Variable | Description | Example |
|bound|---|---|
| `LOG_LEVEL` | Logging verbosity level. | `INFO` |
| `BAND_13_SCHEDULE_CRON` | Cron schedule for the Band 13 processing job. | `*/10 * * * *` |
| `BAND_9_SCHEDULE_CRON` | Cron schedule for the Band 9 processing job. | `*/10 * * * *` |
| `TZ` | Timezone for the scheduler. | `America/Argentina/Buenos_Aires` |
| `TMP_DIR_HOST` | Local path for temporary processing files. | `./.tmp` |
| `TMP_DIR_CONTAINER` | Container path for temporary processing files. | `/app/.tmp` |

## Radar Processing

The repository also contains tools for processing radar data:

- `radar_to_tiles.py`: Converts H5 radar files to map tiles.
- `explore_radar.py`: A script to inspect H5 radar files structure and metadata.
