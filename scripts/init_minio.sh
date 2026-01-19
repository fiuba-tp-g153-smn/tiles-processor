#!/bin/sh
# Exit immediately if a command exits with a non-zero status
set -e

# =================================================================================================
# MinIO Initialization Script
# =================================================================================================
# This script configures a MinIO instance with:
# 1. A specific bucket.
# 2. A Read-Only user (e.g., for data-service).
# 3. A Read-Write user (e.g., for tiles-processor).
# 
# It is designed to be idempotent (safe to run multiple times).

# Env vars required:
# MINIO_ENDPOINT (e.g. http://minio:9000 or http://localhost:9000)
# MINIO_ROOT_USER
# MINIO_ROOT_PASSWORD
# BUCKET_NAME

# S3_TILES_DATA_TILES_PROCESSOR_USER (Read-Write User)
# S3_TILES_DATA_TILES_PROCESSOR_PASSWORD

# S3_TILES_DATA_DATA_SERVICE_USER (Read-Only User)
# S3_TILES_DATA_DATA_SERVICE_PASSWORD

echo "Initializing MinIO..."
echo "Endpoint: $MINIO_ENDPOINT"
echo "Bucket: $BUCKET_NAME"

# -------------------------------------------------------------------------------------------------
# 1. Connect to MinIO
# -------------------------------------------------------------------------------------------------
# We loop until we can successfully set an alias for the MinIO server.
# This effectively waits for the MinIO service to be ready and accessible.
until mc alias set myminio "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  echo "Waiting for MinIO..."
  sleep 1
done

# -------------------------------------------------------------------------------------------------
# 2. Bucket Creation
# -------------------------------------------------------------------------------------------------
# Create the bucket if it doesn't exist.
# --ignore-existing: Prevents error if bucket already exists.
# parenthesis || true: Ensures script doesn't fail even if mc returns error (though --ignore-existing handles most).
mc mb myminio/"$BUCKET_NAME" --ignore-existing || true

# Set the bucket policy to 'download' (public read-only) for anonymous users.
# This might be overlapping with our explicit RO user, but ensures public access if desired.
# It is commented to keep bucket private
# mc anonymous set download myminio/"$BUCKET_NAME"

# -------------------------------------------------------------------------------------------------
# 3. Create Data Service User (Read-Only)
# -------------------------------------------------------------------------------------------------
RO_USER="$S3_TILES_DATA_DATA_SERVICE_USER"
RO_PASSWORD="$S3_TILES_DATA_DATA_SERVICE_PASSWORD"

if [ -n "$RO_USER" ]; then
    # Check if user exists before attempting to create
    if ! mc admin user info myminio "$RO_USER" >/dev/null 2>&1; then
      echo "Creating user $RO_USER..."
      mc admin user add myminio "$RO_USER" "$RO_PASSWORD"
    else
      echo "User $RO_USER already exists."
    fi

    echo "Creating read-only policy..."
    # Create a JSON policy file that allows:
    # - Listing the bucket
    # - Getting objects from the bucket
    cat <<EOF > /tmp/readonly_policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::$BUCKET_NAME"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::$BUCKET_NAME/*"]
    }
  ]
}
EOF

    # Create the policy in MinIO
    mc admin policy create myminio "readonly-$BUCKET_NAME" /tmp/readonly_policy.json || true
    
    # Attach the policy to the user
    echo "Attaching read-only policy to user $RO_USER..."
    mc admin policy attach myminio "readonly-$BUCKET_NAME" --user "$RO_USER"
else
    echo "No Read-Only user configured, skipping."
fi

# -------------------------------------------------------------------------------------------------
# 4. Create Tiles Processor User (Read-Write)
# -------------------------------------------------------------------------------------------------
RW_USER="$S3_TILES_DATA_TILES_PROCESSOR_USER"
RW_PASSWORD="$S3_TILES_DATA_TILES_PROCESSOR_PASSWORD"

if [ -n "$RW_USER" ]; then
    # Check if user exists before attempting to create
    if ! mc admin user info myminio "$RW_USER" >/dev/null 2>&1; then
      echo "Creating user $RW_USER..."
      mc admin user add myminio "$RW_USER" "$RW_PASSWORD"
    else
      echo "User $RW_USER already exists."
    fi

    echo "Creating read-write policy..."
    # Create a JSON policy file that allows:
    # - All S3 actions (s3:*) on the specific bucket and its contents
    cat <<EOF > /tmp/readwrite_policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::$BUCKET_NAME", "arn:aws:s3:::$BUCKET_NAME/*"]
    }
  ]
}
EOF

    # Create the policy in MinIO
    mc admin policy create myminio "readwrite-$BUCKET_NAME" /tmp/readwrite_policy.json || true
    
    # Attach the policy to the user
    echo "Attaching read-write policy to user $RW_USER..."
    mc admin policy attach myminio "readwrite-$BUCKET_NAME" --user "$RW_USER"
else
    echo "No TILES_PROCESSOR user configured, skipping."
fi

echo "MinIO initialization complete."
