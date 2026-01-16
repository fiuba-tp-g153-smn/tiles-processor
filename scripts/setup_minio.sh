#!/bin/bash

# Load environment variables from .env if it exists
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Set defaults if not present in env
PORT=${S3_TILES_DATA_PORT}
ACCESS_KEY=${S3_TILES_DATA_ACCESS_KEY}
SECRET_KEY=${S3_TILES_DATA_SECRET_KEY}
BUCKET_NAME=${S3_TILES_DATA_BUCKET_NAME}

echo "Setting up MinIO bucket: $BUCKET_NAME at localhost:$PORT"

# Run mc command using docker
# We use --network host to access localhost:$PORT where MinIO is running mapped
docker run --rm --network host --entrypoint /bin/sh minio/mc:latest -c "
mc alias set myminio http://localhost:$PORT $ACCESS_KEY $SECRET_KEY;
mc mb myminio/$BUCKET_NAME --ignore-existing;
mc anonymous set download myminio/$BUCKET_NAME;
echo 'Bucket $BUCKET_NAME created and configured';
"
