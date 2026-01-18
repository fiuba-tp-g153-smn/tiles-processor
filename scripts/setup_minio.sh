#!/bin/bash

# Load environment variables from .env if it exists
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Set defaults if not present in env
PORT=${S3_TILES_DATA_PORT}
MINIO_ROOT_USER=${MINIO_ROOT_USER}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}
BUCKET_NAME=${S3_TILES_DATA_BUCKET_NAME}

S3_TILES_DATA_TILES_PROCESSOR_USER=${S3_TILES_DATA_TILES_PROCESSOR_USER}
S3_TILES_DATA_TILES_PROCESSOR_PASSWORD=${S3_TILES_DATA_TILES_PROCESSOR_PASSWORD}

S3_TILES_DATA_DATA_SERVICE_USER=${S3_TILES_DATA_DATA_SERVICE_USER}
S3_TILES_DATA_DATA_SERVICE_PASSWORD=${S3_TILES_DATA_DATA_SERVICE_PASSWORD}

echo "Setting up MinIO bucket and users at localhost:$PORT"

# Get absolute path to init script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Run mc command using docker
# We use --network host to access localhost:$PORT where MinIO is running mapped
docker run --rm --network host \
  -v "$SCRIPT_DIR/init_minio.sh:/setup.sh" \
  --entrypoint /bin/sh \
  -e MINIO_ENDPOINT="http://localhost:$PORT" \
  -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
  -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
  -e BUCKET_NAME="$BUCKET_NAME" \
  -e S3_TILES_DATA_TILES_PROCESSOR_USER="$S3_TILES_DATA_TILES_PROCESSOR_USER" \
  -e S3_TILES_DATA_TILES_PROCESSOR_PASSWORD="$S3_TILES_DATA_TILES_PROCESSOR_PASSWORD" \
  -e S3_TILES_DATA_DATA_SERVICE_USER="$S3_TILES_DATA_DATA_SERVICE_USER" \
  -e S3_TILES_DATA_DATA_SERVICE_PASSWORD="$S3_TILES_DATA_DATA_SERVICE_PASSWORD" \
  minio/mc:latest -c "/setup.sh"
