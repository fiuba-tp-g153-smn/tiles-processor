#!/bin/sh
# =================================================================================================
# SeaweedFS Startup Script
# =================================================================================================
# Generates /etc/seaweedfs/s3.json from environment variables, starts weed server in the
# background (stdout/stderr piped through a grep filter to suppress benign
# "Volume N becomes (un)?crowded" spam — see the filter block below), waits for the master to be
# ready (unauthenticated port 9333), creates the buckets, signals readiness via
# /tmp/seaweedfs_ready, then launches the admin scheduler and maintenance worker and waits on
# all four subprocesses.
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
: "${S3_INTERSECTION_DATA_BUCKET_NAME:?S3_INTERSECTION_DATA_BUCKET_NAME is required}"
: "${S3_INTERSECTION_DATA_ALERTS_SERVICE_USER:?S3_INTERSECTION_DATA_ALERTS_SERVICE_USER is required}"
: "${S3_INTERSECTION_DATA_ALERTS_SERVICE_PASSWORD:?S3_INTERSECTION_DATA_ALERTS_SERVICE_PASSWORD is required}"
: "${S3_BASEMAP_BUCKET_NAME:?S3_BASEMAP_BUCKET_NAME is required}"

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
  -e "s|__INTERSECTION_BUCKET__|${S3_INTERSECTION_DATA_BUCKET_NAME}|g" \
  -e "s|__ALERTS_SERVICE_USER__|${S3_INTERSECTION_DATA_ALERTS_SERVICE_USER}|g" \
  -e "s|__ALERTS_SERVICE_PASSWORD__|${S3_INTERSECTION_DATA_ALERTS_SERVICE_PASSWORD}|g" \
  -e "s|__BASEMAP_BUCKET__|${S3_BASEMAP_BUCKET_NAME}|g" \
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
        "Admin",
        "Read:__BUCKET__",
        "Write:__BUCKET__",
        "List:__BUCKET__",
        "Tagging:__BUCKET__",
        "Read:__BASEMAP_BUCKET__",
        "Write:__BASEMAP_BUCKET__",
        "List:__BASEMAP_BUCKET__",
        "Tagging:__BASEMAP_BUCKET__"
      ]
    },
    {
      "name": "alerts-service",
      "credentials": [
        {
          "accessKey": "__ALERTS_SERVICE_USER__",
          "secretKey": "__ALERTS_SERVICE_PASSWORD__"
        }
      ],
      "actions": [
        "Read:__INTERSECTION_BUCKET__",
        "Write:__INTERSECTION_BUCKET__",
        "List:__INTERSECTION_BUCKET__",
        "Tagging:__INTERSECTION_BUCKET__"
      ]
    }
  ]
}
EOF

METRICS_FLAG=""
if [ -n "${SEAWEEDFS_METRICS_ADDRESS:-}" ] \
  && [ -n "${PROMETHEUS_PUSHGATEWAY_HTTP_PROTO:-}" ] \
  && [ -n "${PROMETHEUS_PUSHGATEWAY_USER:-}" ] \
  && [ -n "${PROMETHEUS_PUSHGATEWAY_PASS:-}" ]; then
  METRICS_FLAG="-master.metrics.address=${PROMETHEUS_PUSHGATEWAY_HTTP_PROTO}://${PROMETHEUS_PUSHGATEWAY_USER}:${PROMETHEUS_PUSHGATEWAY_PASS}@${SEAWEEDFS_METRICS_ADDRESS}"
  echo "Metrics enabled: pushing to ${PROMETHEUS_PUSHGATEWAY_HTTP_PROTO}://${SEAWEEDFS_METRICS_ADDRESS}"
else
  echo "Metrics disabled: SEAWEEDFS_METRICS_ADDRESS or Pushgateway credentials not fully set."
fi

# Drop benign "volume_layout.go … becomes (un)?crowded" spam from weed server logs (glog.V(0),
# no gate; pending-delta bursts cross threshold even with volumes ~30 % full). awk+fflush
# (busybox grep lacks --line-buffered); named FIFO (direct pipe breaks $!); admin/worker unfiltered.
WEED_LOG_PIPE=/tmp/weed-server.log
rm -f "$WEED_LOG_PIPE"
mkfifo "$WEED_LOG_PIPE"
awk '!/volume_layout\.go:[0-9]+ Volume [0-9]+ becomes (un)?crowded$/ { print; fflush() }' \
  < "$WEED_LOG_PIPE" &
WEED_LOG_FILTER_PID=$!

echo "Starting SeaweedFS (master + volume + filer + S3 gateway)..."
weed server \
  -dir=/data \
  -master \
  -master.garbageThreshold=0.01 \
  -master.defaultReplication=000 \
  -master.volumePreallocate=false \
  -master.volumeSizeLimitMB=256 \
  -master.metrics.intervalSeconds=10 \
  -volume \
  -volume.index=leveldb \
  -volume.max=900 \
  -filer \
  -s3 \
  -s3.port=8333 \
  -s3.allowEmptyFolder=false \
  -s3.config=/etc/seaweedfs/s3.json \
  $METRICS_FLAG \
  > "$WEED_LOG_PIPE" 2>&1 &
WEED_PID=$!

# Forward SIGTERM/INT to the server and wait for it to exit cleanly.
# (SIGKILL cannot be trapped — the kernel kills immediately.)
# The log filter normally exits on its own once weed closes the FIFO (EOF),
# but we backstop it with an explicit kill and tear down the FIFO so repeat
# invocations (e.g. container restarts on the same tmpfs) start fresh.
# This trap is replaced below once admin and worker are started.
trap '
  echo "Shutting down SeaweedFS..."
  kill -TERM "$WEED_PID" 2>/dev/null
  wait "$WEED_PID" 2>/dev/null
  kill -TERM "$WEED_LOG_FILTER_PID" 2>/dev/null
  wait "$WEED_LOG_FILTER_PID" 2>/dev/null
  rm -f "$WEED_LOG_PIPE"
  exit 0
' TERM INT

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

echo "Waiting for SeaweedFS filer..."
RETRIES=0
until wget -qO /dev/null http://seaweedfs:8888/ 2>/dev/null; do
    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
        echo "SeaweedFS filer did not become ready in time. Aborting."
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

echo "Checking bucket ${S3_INTERSECTION_DATA_BUCKET_NAME}..."
INTERSECTION_BUCKET_EXISTS=$(echo "s3.bucket.list" \
    | weed shell -master=localhost:9333 2>/dev/null \
    | grep -c "${S3_INTERSECTION_DATA_BUCKET_NAME}" || true)

if [ "$INTERSECTION_BUCKET_EXISTS" -eq 0 ]; then
    echo "Creating bucket ${S3_INTERSECTION_DATA_BUCKET_NAME}..."
    echo "s3.bucket.create -name ${S3_INTERSECTION_DATA_BUCKET_NAME}" \
        | weed shell -master=localhost:9333
else
    echo "Bucket ${S3_INTERSECTION_DATA_BUCKET_NAME} already exists, skipping."
fi

echo "Checking bucket ${S3_BASEMAP_BUCKET_NAME}..."
BASEMAP_BUCKET_EXISTS=$(echo "s3.bucket.list" \
    | weed shell -master=localhost:9333 2>/dev/null \
    | grep -c "${S3_BASEMAP_BUCKET_NAME}" || true)

if [ "$BASEMAP_BUCKET_EXISTS" -eq 0 ]; then
    echo "Creating bucket ${S3_BASEMAP_BUCKET_NAME}..."
    echo "s3.bucket.create -name ${S3_BASEMAP_BUCKET_NAME}" \
        | weed shell -master=localhost:9333
else
    echo "Bucket ${S3_BASEMAP_BUCKET_NAME} already exists, skipping."
fi

touch /tmp/seaweedfs_ready
echo "SeaweedFS ready."

# Seed the admin's plugin-job-type config so the Erasure Coding detector starts
# disabled. EC placement needs >=4 disks/racks; on this single-node deployment
# detection can never succeed and the scheduler otherwise floods logs with
# "Failed to plan EC destinations" every ~60s.
#
# The scheduler reads {dataDir}/plugin/job_types/{jobType}/config.pb as a
# plugin_pb.PersistedJobTypeConfig and skips detection when
# AdminRuntime.Enabled is false (proto3 default). The 18-byte blob below
# encodes { job_type: "erasure_coding", admin_runtime: {} }.
ADMIN_DATA_DIR=/data/admin-data
EC_CONFIG_DIR="$ADMIN_DATA_DIR/plugin/job_types/erasure_coding"
EC_CONFIG_FILE="$EC_CONFIG_DIR/config.pb"
if [ ! -f "$EC_CONFIG_FILE" ]; then
    echo "Seeding EC task config (disabled) at $EC_CONFIG_FILE..."
    mkdir -p "$EC_CONFIG_DIR"
    printf '\x0a\x0eerasure_coding\x2a\x00' > "$EC_CONFIG_FILE"
fi

echo "Starting SeaweedFS admin scheduler..."
weed admin \
  -master=localhost:9333 \
  -dataDir="$ADMIN_DATA_DIR" \
  -adminUser="${S3_ROOT_USER}" \
  -adminPassword="${S3_ROOT_PASSWORD}" \
  -readOnlyUser="${S3_TILES_DATA_DATA_SERVICE_USER}" \
  -readOnlyPassword="${S3_TILES_DATA_DATA_SERVICE_PASSWORD}" &
ADMIN_PID=$!

echo "Starting SeaweedFS maintenance worker..."
mkdir -p /data/worker-data
weed worker \
  -admin=localhost:23646 \
  -workingDir=/data/worker-data \
  -metricsPort=2112 &
WORKER_PID=$!

# Shutdown order matters for data integrity: reap admin+worker first so their master-client
# sessions don't stall weed server's 2× ~10 s graceful-stop (filer gRPC + volume heartbeat drain)
# past docker's SIGKILL deadline; awk filter reaped last so final shutdown logs still reach docker.
trap '
  echo "Shutting down SeaweedFS..."
  kill -TERM "$ADMIN_PID"  2>/dev/null
  kill -TERM "$WORKER_PID" 2>/dev/null
  wait "$ADMIN_PID"  2>/dev/null
  wait "$WORKER_PID" 2>/dev/null
  kill -TERM "$WEED_PID"   2>/dev/null
  wait "$WEED_PID"   2>/dev/null
  kill -TERM "$WEED_LOG_FILTER_PID" 2>/dev/null
  wait "$WEED_LOG_FILTER_PID" 2>/dev/null
  rm -f "$WEED_LOG_PIPE"
  exit 0
' TERM INT

# Include the log filter in the final wait so a crash of the filter (which
# would cause `weed server` to block on the FIFO) exits the script and lets
# docker restart the container via `restart: unless-stopped`.
wait $WEED_PID $ADMIN_PID $WORKER_PID $WEED_LOG_FILTER_PID
