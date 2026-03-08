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

# Fail fast if any required env var is missing, before generating config files.
: "${S3_ROOT_USER:?S3_ROOT_USER is required}"
: "${S3_ROOT_PASSWORD:?S3_ROOT_PASSWORD is required}"
: "${S3_TILES_DATA_BUCKET_NAME:?S3_TILES_DATA_BUCKET_NAME is required}"
: "${S3_TILES_DATA_TILES_PROCESSOR_USER:?S3_TILES_DATA_TILES_PROCESSOR_USER is required}"
: "${S3_TILES_DATA_TILES_PROCESSOR_PASSWORD:?S3_TILES_DATA_TILES_PROCESSOR_PASSWORD is required}"
: "${S3_TILES_DATA_DATA_SERVICE_USER:?S3_TILES_DATA_DATA_SERVICE_USER is required}"
: "${S3_TILES_DATA_DATA_SERVICE_PASSWORD:?S3_TILES_DATA_DATA_SERVICE_PASSWORD is required}"

mkdir -p /etc/seaweedfs

echo "Generating /etc/seaweedfs/s3.json..."

sed \
  -e "s|__ROOT_USER__|${S3_ROOT_USER}|g" \
  -e "s|__ROOT_PASSWORD__|${S3_ROOT_PASSWORD}|g" \
  -e "s|__BUCKET__|${S3_TILES_DATA_BUCKET_NAME}|g" \
  -e "s|__RW_USER__|${S3_TILES_DATA_TILES_PROCESSOR_USER}|g" \
  -e "s|__RW_PASSWORD__|${S3_TILES_DATA_TILES_PROCESSOR_PASSWORD}|g" \
  -e "s|__RO_USER__|${S3_TILES_DATA_DATA_SERVICE_USER}|g" \
  -e "s|__RO_PASSWORD__|${S3_TILES_DATA_DATA_SERVICE_PASSWORD}|g" \
  << 'EOF' > /etc/seaweedfs/s3.json
{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        {
          "accessKey": "__ROOT_USER__",
          "secretKey": "__ROOT_PASSWORD__"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "tiles-processor",
      "credentials": [
        {
          "accessKey": "__RW_USER__",
          "secretKey": "__RW_PASSWORD__"
        }
      ],
      "actions": [
        "Read:__BUCKET__",
        "Write:__BUCKET__",
        "List:__BUCKET__",
        "Tagging:__BUCKET__"
      ]
    },
    {
      "name": "data-service",
      "credentials": [
        {
          "accessKey": "__RO_USER__",
          "secretKey": "__RO_PASSWORD__"
        }
      ],
      "actions": [
        "Read:__BUCKET__",
        "List:__BUCKET__"
      ]
    }
  ]
}
EOF

METRICS_FLAG=""
if [ -n "${SEAWEEDFS_METRICS_ADDRESS:-}" ]; then
  METRICS_FLAG="-master.metrics.address=${SEAWEEDFS_METRICS_ADDRESS}"
fi

echo "Starting SeaweedFS (master + volume + filer + S3 gateway on :8333)..."
weed server \
  -dir=/data \
  -volume.index=leveldb \
  -volume.max=10 \
  -filer \
  -master.garbageThreshold=0.1 \
  -master.maxParallelVacuumPerServer=4 \
  -master.defaultReplication=000 \
  -master.volumePreallocate=false \
  -master.volumeSizeLimitMB=5000 \
  -s3 \
  -s3.port=8333 \
  -s3.allowEmptyFolder=false \
  -s3.config=/etc/seaweedfs/s3.json \
  $METRICS_FLAG &
WEED_PID=$!

# Forward SIGTERM/INT to weed so it can flush and close cleanly.
# (SIGKILL cannot be trapped — the kernel kills immediately.)
trap 'echo "Shutting down SeaweedFS..."; kill -TERM "$WEED_PID" 2>/dev/null; wait "$WEED_PID" 2>/dev/null; exit 0' TERM INT

echo "Waiting for SeaweedFS master..."
MAX_RETRIES=30
RETRIES=0
until wget -qO /dev/null http://seaweedfs:9333/cluster/status 2>/dev/null; do
    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
        echo "SeaweedFS master did not become ready in time. Aborting."
        kill -TERM "$WEED_PID" 2>/dev/null
        exit 1
    fi
    sleep 1
done

# Check if the bucket already exists before creating it, to avoid a noisy
# error on container restarts when the /data volume is persisted.
echo "Checking bucket ${S3_TILES_DATA_BUCKET_NAME}..."
BUCKET_EXISTS=$(echo "s3.bucket.list" \
    | weed shell -master=localhost:9333 2>/dev/null \
    | grep -c "${S3_TILES_DATA_BUCKET_NAME}" || true)

if [ "$BUCKET_EXISTS" -eq 0 ]; then
    echo "Creating bucket ${S3_TILES_DATA_BUCKET_NAME}..."
    echo "s3.bucket.create -name ${S3_TILES_DATA_BUCKET_NAME}" \
        | weed shell -master=localhost:9333
else
    echo "Bucket ${S3_TILES_DATA_BUCKET_NAME} already exists, skipping."
fi

touch /tmp/seaweedfs_ready
echo "SeaweedFS ready."

wait $WEED_PID
