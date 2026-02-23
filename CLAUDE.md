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

## Build/Test/Lint Commands

```bash
# Start development environment (Docker Compose with hot reload)
make up

# Stop all services
make down

# Run tests with coverage
make test

# Run a single test file
pytest tests/test_config.py -v

# Run tests matching pattern
pytest tests/ -k "test_health" -v

# Production build and start
make prod

# Clean Docker volumes
make clean
```

## Code Quality

Pre-commit hooks run Black (formatter) and Pylint (linter). Pylint ignores test files.

```bash
black src/ --check
pylint src --ignore-patterns="test_.*?py"
```

## Architecture Overview

This is a **distributed satellite imagery processing system** using a producer-worker pattern with RabbitMQ.

### Processing Flow

```
NOAA S3 (GOES-19) → Producer (discovery) → RabbitMQ → Workers (1-5) → Seaweedfs (tiles)
```

1. **Producer** (`src/producer/`): Runs on cron schedule, discovers new satellite images from NOAA S3, checks Seaweedfs for already-processed images, publishes work units to RabbitMQ
2. **Workers** (`src/worker/`): Consume work units, execute processing pipeline (download → georeference → brightness temp → GeoTIFF → tiles → upload → cleanup)
3. **RabbitMQ**: Work queue with dead letter queue for failed messages, manual ack, prefetch=1
4. **Seaweedfs**: S3-compatible storage for generated XYZ map tiles

### Key Components

- **Data Sources** (`src/data_sources/`): Registry pattern for pluggable sources (GOES-19, Radar). Add new sources by implementing base class and registering.
- **Processors** (`src/processors/`): Registry pattern for image processors (Band 13 Cloud Tops, Band 9 Water Vapor, Radar). Each processor handles a specific data type's pipeline.
- **Work Unit** (`src/models/work_unit.py`): Dataclass representing a processing task, serialized to JSON for queue transmission.
- **S3 Client** (`src/clients/s3_client.py`): Async operations using aioboto3 with semaphore-based concurrency control.
- **Health Server** (`src/health_server.py`): HTTP server on port 8080 with `/health` and `/ready` endpoints for container health checks.

### Entry Point

`src/main.py` accepts `producer` or `worker` as argument to start the respective service.

## Configuration

- **Environment**: Required vars defined in `src/config.py`, template in `.env.example`
- **Feature toggles**: `settings.json` controls enabled bands, geographic bounds, timezone
- **Docker Compose**: `docker-compose-dev.yaml` (dev with volumes), `docker-compose.yaml` (prod)

## Development Guidelines

### Code Style & Organization

- **Early returns** to avoid nested conditions
- **Descriptive names**: prefix handlers with `handle_`, use verb-noun for functions
- **Functional style**: prefer immutable approaches when not verbose
- **Minimal changes**: only modify code related to the task
- Keep functions <20 lines
- One class per file unless tightly coupled (e.g., exceptions with their class)

### Architecture & Design Patterns

#### Dependency Injection & Decoupling

- **Constructor injection**: Pass dependencies via `__init__`, avoid globals
- **Interface dependencies**: Depend on abstractions (ABC/Protocol), not concrete classes
- **Factory pattern**: Use factories for complex object creation
- **Avoid service locator**: Don't use global registries to fetch dependencies at runtime
- **Composition over inheritance**: Prefer has-a relationships over is-a

#### Interface Design

- **Use ABC for inheritance-based interfaces**: When you need shared implementation
- **Use Protocol for structural typing**: When you only care about behavior
- **Keep interfaces small**: Interface Segregation Principle (ISP)
- **Define contracts explicitly**: Document preconditions, postconditions, invariants
- Use `dataclasses` with `slots=True` for data containers

#### Class Design Principles (SOLID)

- **Single Responsibility Principle (SRP)**: One class, one reason to change
- **Open/Closed Principle**: Open for extension, closed for modification
- **Liskov Substitution**: Subtypes must be substitutable for base types
- **Interface Segregation**: Many small interfaces over one large interface
- **Dependency Inversion**: Depend on abstractions, not concretions
- **Immutable by default**: Use `frozen=True` dataclasses, avoid setters

#### Registry Pattern Implementation

- Use typed registries with `Generic[T]` for type safety
- Validate on registration to fail fast
- Register via decorators or explicit calls
- Keep registry references scoped, not global

#### Error Handling & Validation

- **Fail fast**: Validate inputs early, raise exceptions immediately
- **Custom exceptions**: Create domain-specific exception hierarchies
- **Typed errors**: Use exception classes to communicate error types
- **Context managers**: Use for cleanup in error paths
- **Avoid bare except**: Catch specific exceptions or use `Exception` with re-raise

#### Testing Considerations

- **Test interfaces, not implementation**: Tests should work with any implementation
- **Use dependency injection**: Makes mocking/stubbing easy
- **Mock external services**: Don't test S3, RabbitMQ in unit tests
- **Use Protocol for test doubles**: Create lightweight test implementations

### Resource Management Best Practices

#### Memory Optimization

- **Stream large files**: Use generators and async iteration instead of loading full files
- **Context managers**: Always use `with` or `async with` for file/connection cleanup
- **Explicit cleanup**: Call `.close()` on resources when context managers aren't available
- **Bounded buffers**: Use `asyncio.Queue(maxsize=N)` to prevent unbounded memory growth
- **Dataclass slots**: Add `slots=True` to dataclasses to reduce memory overhead (40-50%)
- **Weak references**: Use `weakref` for caches that shouldn't prevent garbage collection
- **Chunk processing**: Process large datasets in fixed-size chunks
- **Memory profiling**: Use `memory_profiler` decorator for suspected memory leaks

#### CPU Optimization

- **Async I/O**: Use `asyncio` for I/O-bound operations (network, disk)
- **Process pools**: Use `multiprocessing.Pool` for CPU-bound tasks (image processing)
- **Thread pools**: Use `concurrent.futures.ThreadPoolExecutor` for I/O with blocking APIs
- **Semaphores**: Limit concurrent operations with `asyncio.Semaphore(max_concurrent)`
- **Connection pooling**: Reuse HTTP connections via session objects
- **Batch operations**: Group small operations to reduce overhead
- **Lazy evaluation**: Defer expensive computations until needed
- **Caching**: Use `functools.lru_cache` for pure functions with repeated inputs

#### Docker & Resource Limits

- Set memory limits in `docker-compose.yaml`: `mem_limit: 2g`
- Set CPU limits: `cpus: 2.0`
- Use `--memory-swap=0` to prevent swap thrashing
- Configure worker prefetch to match available memory
- Monitor with `docker stats` for runtime resource usage

#### RabbitMQ Configuration

- **Prefetch count**: Set to 1 for workers to prevent overwhelming single workers
- **Manual ack**: Always use manual acknowledgment for reliable processing
- **TTL on messages**: Set message TTL to prevent infinite queue growth
- **Dead letter exchange**: Route failed messages for later analysis
- **Connection pooling**: Reuse channels within a connection

#### S3/Seaweedfs Best Practices

- **Multipart uploads**: Use for files >5MB to improve reliability
- **Async client**: Use aioboto3 for non-blocking S3 operations
- **Exponential backoff**: Retry failed operations with increasing delays
- **Streaming**: Stream large objects directly to disk, don't buffer in memory
- **Lifecycle policies**: Configure automatic deletion of old tiles

#### Performance Monitoring

- **Structured logging**: Use structured logs with timing data (`logger.info("msg", extra={...})`)
- **Metrics**: Track queue depth, processing time, error rates
- **APM tools**: Consider Prometheus + Grafana for production
- **Profile critical paths**: Use `cProfile` or `py-spy` for CPU profiling
- **Time operations**: Use `time.perf_counter()` for accurate measurements

#### Anti-Patterns to Avoid

**Architecture:**

- ❌ God objects that do everything
- ❌ Circular dependencies between modules
- ❌ Global mutable state
- ❌ Tight coupling to frameworks/libraries
- ❌ Service locator pattern (use DI instead)
- ❌ Mixing business logic with infrastructure concerns

**Resources:**

- ❌ Loading entire satellite images into memory (when possible)
- ❌ Unbounded async task creation (use semaphores)
- ❌ Blocking I/O in async functions (use `asyncio.to_thread`)
- ❌ Global state for caches (use instance attributes with lifecycle)
- ❌ Catching `Exception` without re-raising or proper handling
- ❌ Ignoring backpressure signals from queues
- ❌ Not cleaning up resources in error paths
