FROM ghcr.io/osgeo/gdal:ubuntu-small-latest-amd64

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

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

# Set work directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Configure poetry to not create a virtual environment (install in system)
RUN poetry config virtualenvs.create false

# Remove the EXTERNALLY-MANAGED marker to allow pip/poetry to install system-wide
RUN rm -f /usr/lib/python3.12/EXTERNALLY-MANAGED

# Install dependencies
RUN poetry install --no-root --no-interaction --no-ansi

# Copy project source code
COPY src/ ./src/
COPY README.md ./

# Set PYTHONPATH
ENV PYTHONPATH=/app/src

# Default command
CMD ["python3", "src/main.py", "process_band_9"]
