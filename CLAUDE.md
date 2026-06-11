# CLAUDE.md

## Collaboration Protocol

1. **Before coding**: Describe approach → wait for approval. Ask clarifying questions if requirements are ambiguous.
2. **>3 file changes**: Stop. Break into smaller tasks first.
3. **After coding**: List what could break and which tests need adding/updating.

## Commands

```bash
make up / make down / make test / make prod / make clean

pytest tests/test_config.py -v          # single file
pytest tests/ -k "test_health"          # pattern match

black src/ --check
pylint src --ignore-patterns="test_.*?py"
```

Bare commands require `source .venv/bin/activate && cmd`. Pre-commit hooks run Black + Pylint.

## Architecture

Producer-worker satellite imagery pipeline:

```
NOAA S3 (GOES-19 ABI)  ─┐
Local CG_GLM-L2-GLMF    ├→ Producer → RabbitMQ → Workers (1–5) → S3/SeaweedFS (tiles)
Local SINARAME radar    ─┘
```

| Component | Location | Role |
|---|---|---|
| **Producer** | `src/producer/` | Cron-scheduled. Discovers new images from each data source, deduplicates via SeaweedFS, publishes `WorkUnit` to RabbitMQ. |
| **Workers** | `src/worker/` | Consume work units (prefetch=1, manual ack). Pipeline: download → georeference → science → GeoTIFF → gdal2tiles → upload → cleanup. |
| **Subprocess isolation** | `src/worker/subprocess_processor.py` | Heavy processing in subprocess for full memory reclamation per image. |
| **Processors** | `src/processors/` | `GoesProcessor` (template-method) → `Band2Processor`, `Band13Processor`, `Band9Processor`. `GlmFedProcessor` aggregates pre-gridded GLM windows via `glmtools` and emits FED/TOE/MFA tiles in one run. All via `ProcessorRegistry`. |
| **Data Sources** | `src/data_sources/` | `DataSourceRegistry` with pluggable impls: `Goes19AbiDataSource` (NOAA S3 by default), `GlmFolderDataSource` (CG_GLM-L2-GLMF), `RadarDataSource` (SINARAME H5), `WrfDataSource` (WRF-ARG4K FIELD2D), ECMWF. GOES/GLM/radar/WRF read via per-source `*FileRepository` (Local or S3 impl, same folder layout) selected by `<source>_input_mode` in settings.json; S3 credentials via `<SOURCE>_S3_ACCESS_KEY`/`_SECRET_KEY` env vars (unset = anonymous). |
| **Services** | `src/services/processing_steps.py`, `glm_aggregation.py` | Pure functions: georeferencing, brightness temp, colorization, tiling, RGBA; GLM window aggregation + GEOS→EPSG:4326 reprojection. |
| **Clients** | `src/clients/` | Async S3 (aioboto3 + semaphore), RabbitMQ (pika, connection pooling), SQLite progress tracker. |
| **Config** | `src/config.py`, `settings.json` | Env vars, feature flags, geographic bounds. |
| **Entry point** | `src/main.py producer\|worker` | |

## Critical Gotchas

### Band 2 Memory (21696×21696 — ~470M pixels)

`mask_and_scale=True` (xarray default) decodes int16 → float64 = **3.76 GB**. Required approach:
1. Load raw int16 with `mask_and_scale=False` (~940 MB)
2. Coarsen 4× → 5424×5424
3. Then apply `scale_factor`/`add_offset` on the small array (~235 MB)

Peak after fix: ~1.2 GB.

### Template Method — Do Not Bypass

`Band2Processor` overrides `_apply_georeferencing`, `_compute_brightness_temperature`, `_generate_geotiff`. The pipeline in `_run_science_pipeline` **must** call `self._apply_georeferencing()` — never the module-level function. Inlining bypasses the override → Band 2 gets the 3.76 GB array → OOM.

### rioxarray Reproject

For `rio.reproject("EPSG:4326")` on geostationary data, leave `resolution=None`. Explicit `resolution=0.02` on 5424² input inflates output to ~8100².

## Code Style

- Early returns; functions <20 lines; one class per file.
- `handle_` prefix for event handlers; verb-noun naming.
- Immutable by default: `frozen=True`, `slots=True` dataclasses.
- Fail fast: validate inputs early, domain-specific exceptions, no bare `except`.
- Minimal changes: only modify code related to the task.

## Design Principles

- **Dependency Injection (DI) via constructor**: Pass deps through `__init__`, no globals, no service locator.
- **Abstractions**: Depend on ABC (shared impl) and not Protocol (structural typing). Keep interfaces small (ISP).
- **SOLID**: SRP, open/closed, Liskov substitution, interface segregation, dependency inversion.
- **Composition over inheritance**.
- **Typed registries**: `Generic[T]`, validate on registration, decorator or explicit registration, scoped not global.
- **Error handling**: Custom exception hierarchies, context managers for cleanup, catch specific exceptions.
- **Testing**: Test interfaces not implementations, DI for easy mocking, mock external services, Protocol for test doubles.

## Resource Management

### Memory
- Stream large files (generators / async iteration), never load full satellite images when avoidable.
- Context managers (`with`/`async with`) for all file/connection cleanup.
- Bounded buffers: `asyncio.Queue(maxsize=N)`.
- Chunk-process large datasets. Use `memory_profiler` for suspected leaks.

### Concurrency
- `asyncio` for I/O-bound; `multiprocessing.Pool` for CPU-bound; `concurrent.futures.ThreadPoolExecutor` for blocking I/O.
- `asyncio.Semaphore(N)` to bound concurrent ops — no unbounded task creation.
- Never use blocking I/O in async functions (use `asyncio.to_thread`).
- Connection pooling for HTTP and RabbitMQ channels.

### Infrastructure
- Docker: `mem_limit`, `cpus`, `--memory-swap=0`. Monitor with `docker stats`.
- RabbitMQ: prefetch=1, manual ack, message TTL, dead-letter exchange.
- S3: multipart uploads >5MB, aioboto3 async, exponential backoff retries, stream to disk.
- Monitoring: structured logging with timing, track queue depth / processing time / error rates, `time.perf_counter()`.

## Anti-Patterns

- ❌ God objects, circular deps, global mutable state, tight framework coupling
- ❌ Mixing business logic with infrastructure
- ❌ Catching `Exception` without re-raise, ignoring queue backpressure
- ❌ Not cleaning up resources in error paths
