# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## AI Collaboration Rules

When assisting with this repository, **always follow these rules**:

1. **Before writing any code**, describe your proposed approach and **wait for explicit approval**.
   - If requirements are ambiguous or underspecified, **ask clarifying questions first**.

2. If a task requires changes to **more than 3 files**, **stop** and break the work into
   smaller, clearly defined tasks before proceeding.

3. **After writing code**, explicitly list:
   - What could break as a result of the change
   - Which tests should be added or updated to cover those risks

## Build / Test / Lint

```bash
make up                         # Start dev environment (Docker Compose with bind mounts)
make down                       # Stop all services
make test                       # Run tests with coverage
make prod                       # Production build and start
make clean                      # Remove Docker volumes

pytest tests/test_config.py -v  # Run a single test file
pytest tests/ -k "test_health"  # Run tests matching a pattern

black src/ --check
pylint src --ignore-patterns="test_.*?py"
```

If you are running bare commands you need to use the virtual environment so run it like `source .venv/bin/activate && cmd`.

Pre-commit hooks run Black and Pylint automatically.

## Architecture Overview

Distributed satellite imagery processing system using a **producer-worker pattern**.

```
NOAA S3 (GOES-19) → Producer → RabbitMQ → Workers (1–5) → MinIO/SeaweedFS (tiles)
```

1. **Producer** (`src/producer/`): Cron-scheduled. Discovers new images on NOAA S3, checks MinIO for already-processed tiles, publishes `WorkUnit` messages to RabbitMQ.
2. **Workers** (`src/worker/`): Consume work units (prefetch=1, manual ack). Each unit runs the full pipeline: download → georeference → brightness temp / reflectance → GeoTIFF → gdal2tiles → upload → cleanup.
3. **Subprocess isolation** (`src/worker/subprocess_processor.py`): Heavy per-image processing runs in a subprocess so memory is fully reclaimed after each image.
4. **Entry point**: `src/main.py producer|worker`

### Key Components

- **Processors** (`src/processors/`): `GoesProcessor` (base, template-method pattern) → subclassed by `Band2Processor`, `Band13Processor`, `Band9Processor`. `GlmFedProcessor` aggregates lightning flashes. All registered in `ProcessorRegistry`.
- **Data Sources** (`src/data_sources/`): `DataSourceRegistry` with pluggable implementations for GOES-19 ABI, GLM, and Radar.
- **Services** (`src/services/processing_steps.py`): Pure functions for georeferencing, brightness temperature, colorization, tile generation, RGBA composition.
- **Clients** (`src/clients/`): Async S3 (aioboto3 + semaphore), RabbitMQ (pika, connection pooling), SQLite progress tracker.
- **Config**: env vars via `src/config.py`; feature flags and geographic bounds via `settings.json`.

## Critical Gotchas

### Band 2 Memory (21696×21696 pixels)

Band 2 Full Disk is 21696×21696 (≈470 M pixels). CF-decoding int16 → float64 via xarray's default `mask_and_scale=True` produces a **3.76 GB** array. The fix:

1. Load raw int16 with `mask_and_scale=False` (~940 MB)
2. Coarsen 4× to 5424×5424
3. *Then* apply `scale_factor` / `add_offset` on the small array (~235 MB float64)

Peak memory after fix: ~1.2 GB.

### Template Method Pattern in `GoesProcessor`

`Band2Processor` overrides `_apply_georeferencing`, `_compute_brightness_temperature`, and `_generate_geotiff`. The pipeline in `_run_science_pipeline` **must** call `self._apply_georeferencing()` (not the module-level function directly). Inlining the call bypasses the override and Band 2 receives the CF-decoded 3.76 GB array → OOM.

### rioxarray Reproject Resolution

For `rio.reproject("EPSG:4326")` on geostationary data, leave `resolution=None`. Passing an explicit `resolution=0.02` on 5424×5424 input increased output from ~5400² to ~8100²—worse, not better.

## Code Style

- Early returns to avoid nesting; keep functions <20 lines; one class per file.
- Prefix event handlers with `handle_`, use verb-noun naming.
- Depend on abstractions (ABC/Protocol), not concrete classes; constructor injection only.
- Use `frozen=True`, `slots=True` dataclasses for data containers.
- Fail fast: validate inputs early, raise domain-specific exceptions, no bare `except`.
