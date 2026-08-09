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

# =================================================================================================
# Storage/maintenance tunables (all env-overridable)
# =================================================================================================

# Hard ceiling on volume slots. At 100 % the master cannot assign: every PutObject fails with
# "InternalError" while GETs keep working. Shared with the pressure warning so they can't drift.
SEAWEEDFS_VOLUME_MAX="${SEAWEEDFS_VOLUME_MAX:-900}"

# Filer metadata-log retention. The log grows ~5.8 GB/day here, so footprint ≈ days * 5.8 GB.
SEAWEEDFS_METALOG_RETENTION_DAYS="${SEAWEEDFS_METALOG_RETENTION_DAYS:-2}"

# Maintenance cadence, and how stale an abandoned multipart upload may get before it's reaped.
SEAWEEDFS_MAINTENANCE_INTERVAL_SECONDS="${SEAWEEDFS_MAINTENANCE_INTERVAL_SECONDS:-3600}"
SEAWEEDFS_MULTIPART_MAX_AGE="${SEAWEEDFS_MULTIPART_MAX_AGE:-24h}"

# Delay before the first pass: long enough for volumes to register with the master, short enough
# that a container booted against a full cluster frees slots in minutes.
SEAWEEDFS_MAINTENANCE_STARTUP_DELAY_SECONDS="${SEAWEEDFS_MAINTENANCE_STARTUP_DELAY_SECONDS:-300}"

# Vacuum threshold. Do NOT lower without pausing writers: every volume over the threshold blocks up
# to 30 s draining, so 0.01 projected ~15 h to reclaim ~1.3 GB. 0.3 skips tiles-data (0.9 % garbage)
# and still catches freshly purged metadata-log volumes (~100 % garbage).
SEAWEEDFS_VACUUM_GARBAGE_THRESHOLD="${SEAWEEDFS_VACUUM_GARBAGE_THRESHOLD:-0.3}"

# Warn at this percentage of volume.max.
SEAWEEDFS_VOLUME_SLOT_WARN_PERCENT="${SEAWEEDFS_VOLUME_SLOT_WARN_PERCENT:-85}"

# Buckets (space-separated) using the lifecycle TTL fast path. "" = all worker-driven.
SEAWEEDFS_LIFECYCLE_FASTPATH_BUCKETS="${SEAWEEDFS_LIFECYCLE_FASTPATH_BUCKETS:-${S3_TILES_DATA_BUCKET_NAME}}"

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
  -volume.max="${SEAWEEDFS_VOLUME_MAX}" \
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

# The filer metadata change log (/topics/.system/log/...) is assigned with an empty collection
# name, so it hides in the unnamed collection that collection.list prints as "" and the admin UI
# calls "default". Isolating it makes it visible there and droppable in one shot via
# `collection.delete -collection=metalog`, instead of purge -> async chunk delete -> vacuum ->
# deleteEmpty across several passes.
#
# Bucket data is unaffected: fs.configure refuses a -collection under /buckets/.
# -ttl here is silently ignored (the log's assign path never sets Ttl) — retention comes from
# fs.log.purge in the maintenance loop below.
echo "Isolating filer metadata log into the 'metalog' collection..."
echo "fs.configure -locationPrefix=/topics/ -collection=metalog -apply" \
    | weed shell -master=localhost:9333 2>&1 \
    || echo "WARNING: could not apply the /topics/ collection rule; metadata log stays in the default collection."

# S3 lifecycle TTL fast path (upstream default: off).
#
# Off: the s3_lifecycle worker walks the bucket daily and DELETEs each expired object — ~20 M filer
# ops per sweep here, which is exactly what inflates the metadata log, and the space only comes back
# after vacuum + deleteEmpty.
# On: PutObject stamps the matching Expiration.Days rule as a volume TTL, and the volume server
# drops the whole .dat once lastModified + ttl passes. No scan, no tombstones, slot returns at once.
# Durations still come solely from the bucket's lifecycle rules (TILE_LIFECYCLE_RETENTION_DAYS).
#
# Caveats:
#   * The TTL is stamped at write time and cannot be changed after — a rule edit only affects
#     later writes.
#   * New writes only; objects already on disk carry no TTL.
#   * Real retention is ttl + volume fill-time, which is why volumeSizeLimitMB=256 matters.
#   * Silently declines tag-filtered rules and versioned buckets (neither applies here).
#
# The flag is a bucket attribute: persistent, idempotent, and order-independent w.r.t. the rules.
for _fastpath_bucket in $SEAWEEDFS_LIFECYCLE_FASTPATH_BUCKETS; do
    echo "Enabling the lifecycle TTL fast path on ${_fastpath_bucket}..."
    echo "s3.bucket.lifecycle.fastpath -name ${_fastpath_bucket} -enable" \
        | weed shell -master=localhost:9333 2>&1 \
        || echo "WARNING: could not enable the lifecycle TTL fast path on ${_fastpath_bucket};" \
                "expiration falls back to the worker's per-object scan."
done

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

# =================================================================================================
# SeaweedFS maintenance loop
# =================================================================================================
# Nothing else prunes the filer metadata change log. The master's built-in maintenance script
# (which includes fs.log.purge) only loads from a master.toml — this deploy is pure CLI flags — and
# is skipped outright while an admin server is connected, which it always is since we start
# `weed admin` above.
#
# Left alone the log reached 70.7 GB / 296 volumes, which together with tiles-data hit
# volume.max exactly and took all writes down. It is sized by filer OPERATION count, not bytes
# stored, so tile churn and lifecycle expiries drive it however little data is retained.
run_seaweedfs_maintenance() {
    # Order matters. fs.log.purge removes filer entries but their chunks are deleted asynchronously,
    # so each vacuum reclaims the PREVIOUS pass's garbage — this converges over runs, not in one
    # shot. deleteEmpty is last and is the only step that returns a volume SLOT; vacuum just
    # shrinks the .dat in place.
    weed shell -master=localhost:9333 <<SHELL 2>&1
lock
fs.log.purge -daysAgo ${SEAWEEDFS_METALOG_RETENTION_DAYS}
s3.clean.uploads -timeAgo ${SEAWEEDFS_MULTIPART_MAX_AGE}
volume.vacuum -garbageThreshold=${SEAWEEDFS_VACUUM_GARBAGE_THRESHOLD}
volume.deleteEmpty -quietFor=1h -apply
unlock
SHELL
}

# Sum volumeCount across collections so the volume.max ceiling surfaces as a warning rather than
# as a silent write outage.
warn_on_volume_slot_pressure() {
    used=$(echo "collection.list" \
        | weed shell -master=localhost:9333 2>/dev/null \
        | sed -n 's/.*volumeCount:\([0-9][0-9]*\).*/\1/p' \
        | awk '{ total += $1 } END { print total + 0 }')

    # An unreachable master yields 0 here; nothing to report, and the next pass retries.
    [ -n "$used" ] && [ "$used" -gt 0 ] || return 0

    percent=$(( used * 100 / SEAWEEDFS_VOLUME_MAX ))
    echo "SeaweedFS volume slots in use: ${used}/${SEAWEEDFS_VOLUME_MAX} (${percent}%)"
    if [ "$percent" -ge "$SEAWEEDFS_VOLUME_SLOT_WARN_PERCENT" ]; then
        echo "WARNING: ${percent}% of ${SEAWEEDFS_VOLUME_MAX} volume slots used; at 100% ALL writes" \
             "fail with InternalError while reads keep working. Check 'collection.list': if metalog" \
             "dominates lower SEAWEEDFS_METALOG_RETENTION_DAYS, if tiles-data does the data has" \
             "outgrown volume.max * volumeSizeLimitMB."
    fi
}

echo "Starting SeaweedFS maintenance loop (first pass in ${SEAWEEDFS_MAINTENANCE_STARTUP_DELAY_SECONDS}s, then every ${SEAWEEDFS_MAINTENANCE_INTERVAL_SECONDS}s; metadata-log retention ${SEAWEEDFS_METALOG_RETENTION_DAYS}d)..."
(
  # Settle first: a pass against a half-formed topology is noise at best. Then sweep at the end
  # of each iteration, so a container booted against a wedged cluster frees slots after the
  # startup delay instead of after a full interval.
  sleep "$SEAWEEDFS_MAINTENANCE_STARTUP_DELAY_SECONDS"
  while true; do
      echo "Running SeaweedFS maintenance pass..."
      run_seaweedfs_maintenance \
          || echo "WARNING: SeaweedFS maintenance pass failed; retrying next interval."
      warn_on_volume_slot_pressure || true
      sleep "$SEAWEEDFS_MAINTENANCE_INTERVAL_SECONDS"
  done
) &
MAINTENANCE_PID=$!

# Shutdown order matters for data integrity: reap the maintenance loop first (it holds the shell
# `lock` during a pass and would otherwise keep the master busy), then admin+worker so their
# master-client sessions don't stall weed server's 2× ~10 s graceful-stop (filer gRPC + volume
# heartbeat drain) past docker's SIGKILL deadline; awk filter reaped last so final shutdown logs
# still reach docker.
trap '
  echo "Shutting down SeaweedFS..."
  kill -TERM "$MAINTENANCE_PID" 2>/dev/null
  wait "$MAINTENANCE_PID" 2>/dev/null
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
#
# MAINTENANCE_PID is deliberately NOT waited on: it is a `while true` loop that never exits, so
# listing it would block forever and defeat the crash-restart above. The trap reaps it.
wait $WEED_PID $ADMIN_PID $WORKER_PID $WEED_LOG_FILTER_PID
