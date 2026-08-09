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
# Storage/monitor tunables (all env-overridable)
# =================================================================================================
# Housekeeping itself is owned by SeaweedFS's own plugin tasks — see the monitor loop below.

# Hard ceiling on volume slots. At 100 % the master cannot assign: every PutObject fails with
# "InternalError" while GETs keep working. Shared with the pressure warning so they can't drift.
SEAWEEDFS_VOLUME_MAX="${SEAWEEDFS_VOLUME_MAX:-900}"

# Monitor cadence, and the delay before the first check — long enough for volumes to register with
# the master, short enough that a container booted against a full cluster reports it in minutes.
SEAWEEDFS_MONITOR_INTERVAL_SECONDS="${SEAWEEDFS_MONITOR_INTERVAL_SECONDS:-3600}"
SEAWEEDFS_MONITOR_STARTUP_DELAY_SECONDS="${SEAWEEDFS_MONITOR_STARTUP_DELAY_SECONDS:-300}"

# Warn at this percentage of volume.max.
SEAWEEDFS_VOLUME_SLOT_WARN_PERCENT="${SEAWEEDFS_VOLUME_SLOT_WARN_PERCENT:-85}"

# Warn when a plugin job type stops recording runs. admin_script ticks every 17 min when healthy,
# so hours of silence means the task has stalled — its known failure mode, and one it reports
# nowhere else. On stall we force a run through the admin API, which dispatches directly and so
# does not depend on the job-assignment path that wedges.
SEAWEEDFS_JOB_STALE_WARN_HOURS="${SEAWEEDFS_JOB_STALE_WARN_HOURS:-6}"
SEAWEEDFS_MONITORED_JOB_TYPES="${SEAWEEDFS_MONITORED_JOB_TYPES:-admin_script}"
SEAWEEDFS_ADMIN_SCRIPT_FORCE_RUN_ON_STALL="${SEAWEEDFS_ADMIN_SCRIPT_FORCE_RUN_ON_STALL:-true}"

# The admin_script job config, pushed on EVERY boot (see configure_admin_script_job below), so this
# file is the source of truth instead of whatever landed in /data years ago. Retention lives here
# because it is part of the pushed script.
SEAWEEDFS_ADMIN_URL="${SEAWEEDFS_ADMIN_URL:-http://localhost:23646}"
SEAWEEDFS_ADMIN_SCRIPT_INTERVAL_MINUTES="${SEAWEEDFS_ADMIN_SCRIPT_INTERVAL_MINUTES:-17}"

# Filer metadata-log retention. The log grows ~5.8 GB/day here, so footprint ≈ days * 5.8 GB.
# Upstream's default is 7d (~40 GB, ~160 volumes of volume.max) — too long for this cluster.
SEAWEEDFS_METALOG_RETENTION_DAYS="${SEAWEEDFS_METALOG_RETENTION_DAYS:-2}"

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
# fs.log.purge in the admin_script plugin task, configured in the admin UI on :23646.
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
# admin_script job provisioning
# =================================================================================================
# The plugin stack seeds a job config from the worker's descriptor ONLY when none exists
# (SaveJobTypeConfigIfNotExists), and DescriptorVersion is written but read nowhere — so nothing
# ever migrates it. A config written years ago is used verbatim forever. That is how this cluster
# ran for months on a pre-4.32 default carrying `ec.balance -apply` (which can never succeed on a
# single node with 0 EC shard slots, and marked every run "error", hiding real failures) and
# `fs.log.purge -daysAgo=7` (~40 GB of metadata log at this throughput).
#
# So we push the config on every boot instead. Same contract SeaweedFS itself documents for
# admin.toml — "applied at every startup, overriding values". admin.toml cannot be used here:
# its pluginConfigSections covers only vacuum, volume_balance and erasure_coding, not admin_script.
#
# NOTE: this makes the admin UI non-authoritative for admin_script — edits made there are reverted
# on the next restart. Change the values in this file, not in the UI.
ADMIN_COOKIE_JAR=/tmp/seaweedfs-admin-cookies

# Log in to the admin server, storing the session cookie. Two steps, because HandleLogin validates
# a CSRF token bound to a pre-existing session: GET /login mints the token, saves it in the session
# cookie and renders it as <input name="csrf_token">, and only then will the POST be accepted. A
# bare POST is answered with 303 -> /login?error=Invalid CSRF token and sets no auth cookie — which
# looks like success to `curl -f`, since 3xx is not an error. (Writes under /api need no CSRF, only
# the cookie; the token is a login-form requirement.)
#
# --data-urlencode keeps credentials with URL/shell metacharacters intact. Never echo the body.
admin_login() {
    rm -f "$ADMIN_COOKIE_JAR"
    attempt=0
    while [ "$attempt" -lt 30 ]; do
        attempt=$((attempt + 1))

        csrf=$(curl -sf -c "$ADMIN_COOKIE_JAR" -b "$ADMIN_COOKIE_JAR" \
                    "${SEAWEEDFS_ADMIN_URL}/login" \
               | sed -n 's/.*name="csrf_token" value="\([^"]*\)".*/\1/p' | head -1)

        if [ -n "$csrf" ]; then
            curl -sf -o /dev/null \
                -c "$ADMIN_COOKIE_JAR" -b "$ADMIN_COOKIE_JAR" \
                --data-urlencode "username=${S3_ROOT_USER}" \
                --data-urlencode "password=${S3_ROOT_PASSWORD}" \
                --data-urlencode "csrf_token=${csrf}" \
                "${SEAWEEDFS_ADMIN_URL}/login" || true

            # Both success and failure redirect with 303, so confirm the session actually works
            # rather than trusting the POST's exit status.
            if curl -sf -o /dev/null -b "$ADMIN_COOKIE_JAR" \
                    "${SEAWEEDFS_ADMIN_URL}/api/plugin/job-types/admin_script/config"; then
                chmod 600 "$ADMIN_COOKIE_JAR" 2>/dev/null || true
                return 0
            fi
        fi
        sleep 2
    done
    echo "WARNING: could not log in to the SeaweedFS admin server at ${SEAWEEDFS_ADMIN_URL} after" \
         "${attempt} attempts; check S3_ROOT_USER / S3_ROOT_PASSWORD."
    return 1
}

# Push the admin_script config. protojson encodes int64 as a STRING ("17"), matching what the admin
# writes to config.json. adminRuntime.enabled MUST be sent explicitly: the server backfills absent
# fields from the descriptor, but `enabled` is not in that backfill list, so omitting it leaves the
# proto3 zero value and the task ends up silently DISABLED.
configure_admin_script_job() {
    admin_login || return 1

    script="fs.log.purge -daysAgo=${SEAWEEDFS_METALOG_RETENTION_DAYS}\nvolume.deleteEmpty -quietFor=24h -apply\nvolume.fix.replication -apply\ns3.clean.uploads -timeAgo=24h"

    payload=$(printf '{"adminConfigValues":{"script":{"stringValue":"%s"},"run_interval_minutes":{"int64Value":"%s"}},"adminRuntime":{"enabled":true,"detectionIntervalMinutes":%s}}' \
        "$script" \
        "$SEAWEEDFS_ADMIN_SCRIPT_INTERVAL_MINUTES" \
        "$SEAWEEDFS_ADMIN_SCRIPT_INTERVAL_MINUTES")

    if curl -sf -o /dev/null -X PUT \
            -b "$ADMIN_COOKIE_JAR" \
            -H 'Content-Type: application/json' \
            --data "$payload" \
            "${SEAWEEDFS_ADMIN_URL}/api/plugin/job-types/admin_script/config"; then
        echo "Configured the admin_script job (purge ${SEAWEEDFS_METALOG_RETENTION_DAYS}d," \
             "every ${SEAWEEDFS_ADMIN_SCRIPT_INTERVAL_MINUTES}m)."
        return 0
    fi

    echo "WARNING: could not push the admin_script job config; SeaweedFS keeps whatever is already" \
         "persisted in ${ADMIN_DATA_DIR}. If none exists, housekeeping is NOT running."
    return 1
}

# =================================================================================================
# SeaweedFS monitor loop
# =================================================================================================
# Housekeeping is NOT done here. The `admin_script` plugin task already runs fs.log.purge,
# volume.deleteEmpty, volume.fix.replication and s3.clean.uploads on a 17-min tick, and vacuum is
# its own plugin task (hence "DisableVacuum (by plugin worker)" in the logs). Doing that work here
# too would only contend for the same cluster lock the admin server needs.
#
# What the plugin stack does NOT do is notice when it stops. admin_script stalls silently — a job
# that never gets an executor blocks the task, and with globalExecutionConcurrency=1 nothing else
# runs until a restart. Its own history shows 29-day and 20-day gaps with no log line and no run
# record. During one of those the filer metadata change log reached 70.7 GB / 296 volumes, which
# together with tiles-data hit volume.max exactly and failed every write while reads kept working.
#
# So this loop only observes: it takes no lock and mutates nothing. Everything below must stay
# read-only.

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
             "dominates, shorten fs.log.purge -daysAgo in the admin_script config on :23646; if" \
             "tiles-data does, the data has outgrown volume.max * volumeSizeLimitMB."
    fi
}

# Force a run through the admin API. RunPluginJobTypeAPI runs detection and dispatches in-request,
# so it does not go through the scheduler's job-assignment path — which is exactly the part that
# wedges. This is the recovery lever, not just a nudge.
force_job_run() {
    job_type=$1
    admin_login || return 1

    response=$(curl -sf -X POST \
        -b "$ADMIN_COOKIE_JAR" \
        -H 'Content-Type: application/json' \
        --data '{}' \
        "${SEAWEEDFS_ADMIN_URL}/api/plugin/job-types/${job_type}/run") || {
        echo "WARNING: forced run of ${job_type} failed; a container restart is the fallback."
        return 1
    }

    # No jq in this image, so pull the counters out with sed.
    field() { echo "$response" | sed -n "s/.*\"$1\":[[:space:]]*\([0-9][0-9]*\).*/\1/p"; }
    skipped=$(field skipped_active_count)

    echo "Forced run of ${job_type}: success=$(field success_count) error=$(field error_count)" \
         "skipped_active=${skipped:-0}"

    # Proposals are filtered against still-active jobs, so a non-zero count here means a previous
    # job is stuck holding the single execution slot. Only a restart clears that.
    if [ -n "$skipped" ] && [ "$skipped" -gt 0 ]; then
        echo "WARNING: ${job_type} has ${skipped} job(s) still marked active, blocking dispatch." \
             "Restart the container to clear them."
    fi
}

# Warn when a plugin job type stops recording runs. runs.json is rewritten on every completed run,
# so its mtime is the same signal as the last_updated_time field inside it — without needing a JSON
# parser in busybox sh.
check_stalled_jobs() {
    now=$(date +%s)
    for job_type in $SEAWEEDFS_MONITORED_JOB_TYPES; do
        runs="$ADMIN_DATA_DIR/plugin/job_types/${job_type}/runs.json"

        if [ ! -f "$runs" ]; then
            echo "WARNING: ${job_type} has no run history at ${runs}; the plugin task is not" \
                 "recording runs. Check its config in the admin UI on :23646."
            continue
        fi

        age_hours=$(( (now - $(stat -c %Y "$runs")) / 3600 ))
        [ "$age_hours" -lt "$SEAWEEDFS_JOB_STALE_WARN_HOURS" ] && continue

        echo "WARNING: ${job_type} has recorded no run for ${age_hours}h. This task stalls silently" \
             "when a job is never assigned an executor. Housekeeping (log purge, deleteEmpty," \
             "multipart cleanup) is NOT running until it resumes."

        [ "$SEAWEEDFS_ADMIN_SCRIPT_FORCE_RUN_ON_STALL" = "true" ] || continue
        force_job_run "$job_type" || true
    done
}

echo "Starting SeaweedFS monitor loop (first check in ${SEAWEEDFS_MONITOR_STARTUP_DELAY_SECONDS}s, then every ${SEAWEEDFS_MONITOR_INTERVAL_SECONDS}s)..."
(
  # Push the job config first, in this subshell rather than inline: admin_login retries for ~60 s
  # while the admin server comes up and the worker registers its descriptor, and doing that inline
  # would delay installing the shutdown trap below by the same amount.
  configure_admin_script_job || true

  # Settle first: a check against a half-formed topology reports numbers that mean nothing.
  sleep "$SEAWEEDFS_MONITOR_STARTUP_DELAY_SECONDS"
  while true; do
      warn_on_volume_slot_pressure || true
      check_stalled_jobs || true
      sleep "$SEAWEEDFS_MONITOR_INTERVAL_SECONDS"
  done
) &
MONITOR_PID=$!

# Shutdown order matters for data integrity: reap the monitor loop first (it is the only thing here
# that can be mid-RPC to the master), then admin+worker so their master-client sessions don't stall
# weed server's 2× ~10 s graceful-stop (filer gRPC + volume heartbeat drain) past docker's SIGKILL
# deadline; awk filter reaped last so final shutdown logs still reach docker.
trap '
  echo "Shutting down SeaweedFS..."
  kill -TERM "$MONITOR_PID" 2>/dev/null
  wait "$MONITOR_PID" 2>/dev/null
  kill -TERM "$ADMIN_PID"  2>/dev/null
  kill -TERM "$WORKER_PID" 2>/dev/null
  wait "$ADMIN_PID"  2>/dev/null
  wait "$WORKER_PID" 2>/dev/null
  kill -TERM "$WEED_PID"   2>/dev/null
  wait "$WEED_PID"   2>/dev/null
  kill -TERM "$WEED_LOG_FILTER_PID" 2>/dev/null
  wait "$WEED_LOG_FILTER_PID" 2>/dev/null
  rm -f "$WEED_LOG_PIPE" "$ADMIN_COOKIE_JAR"
  exit 0
' TERM INT

# Include the log filter in the final wait so a crash of the filter (which
# would cause `weed server` to block on the FIFO) exits the script and lets
# docker restart the container via `restart: unless-stopped`.
#
# MONITOR_PID is deliberately NOT waited on: it is a `while true` loop that never exits, so
# listing it would block forever and defeat the crash-restart above. The trap reaps it.
wait $WEED_PID $ADMIN_PID $WORKER_PID $WEED_LOG_FILTER_PID
