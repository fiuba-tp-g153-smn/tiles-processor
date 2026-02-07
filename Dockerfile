################################
# Stage 1: Builder
################################
FROM ghcr.io/osgeo/gdal:ubuntu-small-latest-amd64 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN poetry config virtualenvs.in-project true

WORKDIR /app

# Copy dependency files first (cache-friendly layer)
COPY pyproject.toml poetry.lock README.md ./

# Install production dependencies into .venv
RUN (poetry check --lock || poetry lock) && poetry install --without dev --no-root --no-ansi

################################
# Stage 2: Runtime
################################
FROM ghcr.io/osgeo/gdal:ubuntu-small-latest-amd64 AS runner

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv ./.venv

# Copy project source code
COPY src/ ./src/

# Use the venv for all subsequent commands
ENV PATH="/app/.venv/bin:$PATH"

# Use python implementation of protobuf instead of binary
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# Set PYTHONPATH
ENV PYTHONPATH=/app/src

# Build arguments for environment variables
ARG LOG_LEVEL
ARG DATA_DIR
ENV LOG_LEVEL=$LOG_LEVEL
ENV DATA_DIR=$DATA_DIR

# Options: process_band_13, process_band_9 or scheduler
CMD ["python3", "src/main.py", "process_band_13"]

# Health check
HEALTHCHECK --interval=5s --timeout=5s --retries=3 CMD python3 -c 'import urllib.request; urllib.request.urlopen("http://localhost:8080/health")'
