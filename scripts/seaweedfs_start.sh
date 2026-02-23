#!/bin/sh
# =================================================================================================
# SeaweedFS Startup Script
# =================================================================================================
# Generates /etc/seaweedfs/s3.json from environment variables, starts weed server in the
# background, waits for the master to be ready (unauthenticated port 9333), creates the bucket,
# signals readiness via /tmp/seaweedfs_ready, then waits on the server process.
#
# Env vars required:
#   S3_ROOT_USER / S3_ROOT_PASSWORD                         — Admin credentials
#   S3_TILES_DATA_BUCKET_NAME                               — Bucket to create
#   S3_TILES_DATA_TILES_PROCESSOR_USER / _PASSWORD          — Read-Write user
#   S3_TILES_DATA_DATA_SERVICE_USER / _PASSWORD             — Read-Only user

set -e

mkdir -p /etc/seaweedfs

echo "Generating /etc/seaweedfs/s3.json..."

cat > /etc/seaweedfs/s3.json << EOF
{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        {
          "accessKey": "${S3_ROOT_USER}",
          "secretKey": "${S3_ROOT_PASSWORD}"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "tiles-processor",
      "credentials": [
        {
          "accessKey": "${S3_TILES_DATA_TILES_PROCESSOR_USER}",
          "secretKey": "${S3_TILES_DATA_TILES_PROCESSOR_PASSWORD}"
        }
      ],
      "actions": [
        "Read:${S3_TILES_DATA_BUCKET_NAME}",
        "Write:${S3_TILES_DATA_BUCKET_NAME}",
        "List:${S3_TILES_DATA_BUCKET_NAME}",
        "Tagging:${S3_TILES_DATA_BUCKET_NAME}"
      ]
    },
    {
      "name": "data-service",
      "credentials": [
        {
          "accessKey": "${S3_TILES_DATA_DATA_SERVICE_USER}",
          "secretKey": "${S3_TILES_DATA_DATA_SERVICE_PASSWORD}"
        }
      ],
      "actions": [
        "Read:${S3_TILES_DATA_BUCKET_NAME}",
        "List:${S3_TILES_DATA_BUCKET_NAME}"
      ]
    }
  ]
}
EOF

echo "Starting SeaweedFS (master + volume + filer + S3 gateway on :8333)..."
weed server \
  -dir=/data \
  -s3 \
  -s3.port=8333 \
  -s3.config=/etc/seaweedfs/s3.json &
WEED_PID=$!

# Forward SIGTERM/INT to weed so it can flush and close cleanly.
# (SIGKILL cannot be trapped — the kernel kills immediately.)
trap 'echo "Shutting down SeaweedFS..."; kill -TERM "$WEED_PID" 2>/dev/null' TERM INT

echo "Waiting for SeaweedFS master..."
until wget -qO /dev/null http://seaweedfs:9333/cluster/status 2>/dev/null; do
    sleep 1
done

echo "Creating bucket ${S3_TILES_DATA_BUCKET_NAME}..."
echo "s3.bucket.create -name ${S3_TILES_DATA_BUCKET_NAME}" \
    | weed shell -master=localhost:9333 || true

touch /tmp/seaweedfs_ready
echo "SeaweedFS ready."

wait $WEED_PID
