FROM ghcr.io/osgeo/gdal:ubuntu-small-latest-amd64

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Use python implementation of protobuf instead of binary
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# Update and install python pip and basic tools if missing
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN poetry config virtualenvs.create false

# Set work directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml poetry.lock README.md ./

# Remove the EXTERNALLY-MANAGED marker to allow pip/poetry to install system-wide
RUN rm -f /usr/lib/python3.12/EXTERNALLY-MANAGED

# Re-generate lock file if it is outdated, then install all dependencies (except dev/test deps)
# "--without dev": keep container smaller by skipping development deps
# "--no-root": don't install this project as a package itself, we run code mounted in /app
RUN (poetry check --lock || poetry lock --no-update) && poetry install --without dev --no-root --no-ansi

# Copy project source code
COPY src/ ./src/

# Set PYTHONPATH
ENV PYTHONPATH=/app/src

# Build arguments for environment variables
ARG LOG_LEVEL
ARG DATA_DIR_CONTAINER

ENV LOG_LEVEL=$LOG_LEVEL
ENV DATA_DIR_CONTAINER=$DATA_DIR_CONTAINER

# Options: process_band_13, process_band_9 or scheduler
CMD ["python3", "src/main.py", "process_band_13"]

# Health check
HEALTHCHECK --interval=2s --timeout=10s --retries=3 CMD python3 src/healthcheck.py
