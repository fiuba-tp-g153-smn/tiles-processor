# Tiles Processor

<img src="https://uptime.mapasmn.com/api/badge/9/status?style=flat-square" /> <img src="https://uptime.mapasmn.com/api/badge/9/uptime?style=flat-square" /> <img src="https://uptime.mapasmn.com/api/badge/9/ping?style=flat-square" />

This project is a Python-based scheduler application designed to process GOES-19 satellite imagery. It automates the retrieval of ABI-L1b-RadF products (Full Disk Radiance) from an S3 bucket, processes specific spectral bands to compute brightness temperatures, and generates GeoTIFFs and map tiles for visualization.

## Features

- **Satellite Data Processing**: Automatically downloads and processes GOES-19 satellite imagery.
    - **Band 13 (Clean IR Window)**: Processes Channel 13 (10.33 µm) for Cloud Top monitoring.
    - **Band 9 (Mid-Level Water Vapor)**: Processes Channel 9 (6.93 µm) for Water Vapor analysis.
- **Job Management**:
    - **Queuing System**: Jobs are triggered by CRON schedules but are added to a processing queue. A background worker processes jobs sequentially to prevent resource overload.
    - **Feature Toggles**: Specific job types (Band 13, Band 9) can be enabled or disabled via configuration.
    - **Smart Execution**:
        - **Immediate First Run**: New deployments trigger a separate one-off execution immediately, then follow the recurring schedule.
        - **Persistence**: Job state is saved to SQLite, ensuring the schedule survives application restarts.
- **Optimized Processing**:
    - **Smart Caching**: Checks local disk before downloading from S3.
    - **Skip Logic**: Skips the entire processing pipeline if the final tiles already exist for a file.
    - **Auto-Cleanup**: Automatically deletes raw `.nc` and intermediate `.tif` files, retaining only the last 26 images to minimize disk usage while maintaining an effective cache.
- **Safety Limits**: Prevents job execution if the temporary directory size exceeds 10GB (`MAX_TMP_DIR_SIZE_BYTES`) to avoid disk overflow.
- **Dockerized**: Fully containerized environment for easy deployment.
- **Scheduler**: Uses `APScheduler` for precise job scheduling (cron-based).

## Processing Pipeline

Each job (Band 13 and Band 9) follows this processing pipeline:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              JOB EXECUTION FLOW                              │
└─────────────────────────────────────────────────────────────────────────────┘

1. DOWNLOAD (S3Client)
   │  Downloads 24 images from NOAA's noaa-goes19 bucket
   │  Pattern: ABI-L1b-RadF/{YYYY}/{DDD}/{HH}/...C13_G19... (or C09_G19)
   │  Goes back up to 5 hours to find 24 files (4 hours of data)
   │  [OPTIMIZATION] Checks local cache & existing tiles first
   │  Returns: Dict[filename, bytes] (skips files where tiles exist)
   │
   ▼
2. GEOREFERENCE (SetupGOESGeorreferencingService)
   │  - Opens NetCDF files from bytes (h5netcdf engine)
   │  - Extracts GOES satellite projection metadata
   │  - Applies coordinate transformation (x,y → geostationary coords)
   │  - Sets CRS from goes_imager_projection attributes
   │  Returns: Dict[filename, xr.Dataset]
   │
   ▼
3. BRIGHTNESS TEMPERATURE (ComputeBrightnessTemperaturesService)
   │  - Extracts radiance data ("Rad" variable)
   │  - Reads Planck constants (fk1, fk2, bc1, bc2) from file
   │  - Applies Planck equation: T = (fk2 / ln((fk1/L) + 1) - bc1) / bc2
   │  - Filters non-physical values (keeps 150K-350K only)
   │  Returns: Dict[filename, xr.DataArray]  (temperature in Kelvin)
   │
   ▼
4. GEOTIFF GENERATION (GenerateGeoTIFFFilesService)
   │  - Reprojects to EPSG:4326 (lat/lon)
   │  - Clips to configured bounds (default: Argentina region)
   │  - Normalizes temperature to color palette (0-255)
   │  - Creates RGBA GeoTIFF with transparency for NaN
   │  Output: .tmp/band_{N}/geotiff/{original_filename}.tif
   │
   ▼
5. TILE GENERATION (GenerateTilesService)
      - Runs gdal2tiles.py for each GeoTIFF
      - Generates XYZ tiles (zoom 3-7, WEBP format, Leaflet-compatible)
      Output: .tmp/band_{N}/tiles/{original_filename}_tiles/{z}/{x}/{y}.webp
```

### Band Specifications

| Aspect | Band 13 (Cloud Tops) | Band 9 (Water Vapor) |
|--------|---------------------|----------------------|
| Wavelength | 10.33 µm (Clean IR Window) | 6.93 µm (Mid-Level WV) |
| Purpose | Cloud top temperature | Atmospheric moisture |
| Color Palette | Gray → Red | Maroon → Blue (SMN style) |
| Temp Range | 183.15K - 323.15K (-90°C to +50°C) | 220K - 260K (-53°C to -13°C) |
| Output Dir | `.tmp/band_13/` | `.tmp/band_9/` |

### Recommended Execution Frequency

GOES-19 publishes Full Disk images **every 10 minutes**. Each job downloads 24 images (4 hours of data).

| Schedule | CRON | Rationale |
|----------|------|-----------|
| **Every 30 min** (recommended) | `*/30 * * * *` | Good balance - fresh data, reasonable load |
| Every 10 min | `*/10 * * * *` | Real-time updates, but high resource usage |
| Every hour | `0 * * * *` | Lower resource usage, 1-hour delay acceptable |

### File Management & Retention

GOES-19 files have unique names based on timestamp:
```
OR_ABI-L1b-RadF-M6C13_G19_s20250141230210_e20250141239518_c20250141239557.nc
                         └── s20250141230210 = start time (2025, day 014, 12:30:21.0 UTC)
```

**Optimization Strategy**:
- **Smart Skip**: If tiles exist in `.tmp/band_{N}/tiles/{filename}_tiles`, the system **skips** downloading and processing that file entirely.
- **Retention Policy**:
    - The system keeps the **newest 26 files** (approx 4.3 hours) in `raw/` and `geotiff/`.
    - Older files are automatically deleted after every run to save disk space.
    - This retention window ensures the "last 24 images" cache remains effective for subsequent runs.
- **Intermediate Cleanup**: Raw `.nc` and intermediate `.tif` files are transient for files outside the retention window.

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
| `BAND_13_SCHEDULE_CRON` | Cron expression for Band 13 job (validated at startup). | Required |
| `BAND_9_SCHEDULE_CRON` | Cron expression for Band 9 job (validated at startup). | Required |
| `ENABLE_BAND_13` | Enable/Disable Band 13 processing (`true`/`false`). | `true` |
| `ENABLE_BAND_9` | Enable/Disable Band 9 processing (`true`/`false`). | `true` |
| `TMP_DIR_HOST` | Local path for temporary files (host). | `./.tmp` |
| `TMP_DIR_CONTAINER` | Container path for temporary files. | `/app/.tmp` |
| `BOUNDS_MINX` | West longitude for clipping (EPSG:4326). | `-90.0` |
| `BOUNDS_MINY` | South latitude for clipping (EPSG:4326). | `-60.0` |
| `BOUNDS_MAXX` | East longitude for clipping (EPSG:4326). | `-30.0` |
| `BOUNDS_MAXY` | North latitude for clipping (EPSG:4326). | `-15.0` |

## Radar Processing

The repository also contains tools for processing radar data:

- `radar_to_tiles.py`: Converts H5 radar files to map tiles.
- `explore_radar.py`: A script to inspect H5 radar files structure and metadata.
