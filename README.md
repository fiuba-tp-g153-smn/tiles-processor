# Tiles Processor

This project is a Python-based scheduler application designed to process GOES-19 satellite imagery. It automates the retrieval of ABI-L1b-RadF products (Full Disk Radiance) from an S3 bucket, processes specific spectral bands to compute brightness temperatures, and generates GeoTIFFs and map tiles for visualization.

## Features


- **Satellite Data Processing**: Automatically downloads and processes GOES-19 satellite imagery.
    - **Band 13 (Clean IR Window)**: Processes Channel 13 (10.33 µm) for Cloud Top monitoring.
    - **Band 9 (Mid-Level Water Vapor)**: Processes Channel 9 (6.93 µm) for Water Vapor analysis.
- **Job Management**:
    - **Queuing System**: Jobs are triggered by CRON schedules but are added to a processing queue. A background worker processes jobs sequentially to prevent resource overload.
    - **Feature Toggles**: Specific job types (Band 13, Band 9) can be enabled or disabled via configuration.
- **Safety Limits**: Prevents job execution if the temporary directory size exceeds 10GB (`MAX_TMP_DIR_SIZE_BYTES`) to avoid disk overflow.
- **Processing Pipeline**:
    1.  Downloads raw NetCDF/H5 files from S3.
    2.  Geferences the satellite data.
    3.  Computes brightness temperatures.
    4.  Generates colorized GeoTIFFs.
    5.  Produces XYZ tiles for web mapping.
- **Dockerized**: Fully containerized environment for easy deployment.
- **Scheduler**: Uses `APScheduler` for precise job scheduling (cron-based).

## Commands

| Command | Description |
| :--- | :--- |
| `make up` | Start the application in detached mode. |
| `make down` | Stop the application. |
| `make prod` | Build and start the application in production mode. |
| `make test` | Run unit tests. |

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LOG_LEVEL` | Logging verbosity (DEBUG, INFO, WARNING, ERROR). | `INFO` |
| `TZ` | Timezone for the scheduler. | `UTC` |
| `BAND_13_SCHEDULE_CRON` | Cron expression for Band 13 job. | Required |
| `BAND_9_SCHEDULE_CRON` | Cron expression for Band 9 job. | Required |
| `ENABLE_BAND_13` | Enable/Disable Band 13 processing (`true`/`false`). | `true` |
| `ENABLE_BAND_9` | Enable/Disable Band 9 processing (`true`/`false`). | `true` |
| `TMP_DIR_HOST` | Local path for temporary files (host). | `./.tmp` |
| `TMP_DIR_CONTAINER` | Container path for temporary files. | `/app/.tmp` |

## Radar Processing

The repository also contains tools for processing radar data:

- `radar_to_tiles.py`: Converts H5 radar files to map tiles.
- `explore_radar.py`: A script to inspect H5 radar files structure and metadata.
