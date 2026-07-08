#!/usr/bin/env bash
# Start a local MinIO container (S3-compatible object storage) and create the run bucket.
# Idempotent: restarts the existing container if present. Data persists in ./minio-data/.
set -euo pipefail
cd "$(dirname "$0")/.."

# Pick up credentials/bucket from .env if present
if [ -f .env ]; then set -a; source .env; set +a; fi

MINIO_USER="${AWS_ACCESS_KEY_ID:-minioadmin}"
MINIO_PASS="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
BUCKET="${S3_BUCKET:-mlops-agent-eval-runs}"

if docker ps -a --format '{{.Names}}' | grep -q '^minio$'; then
    echo "MinIO container exists — starting it"
    docker start minio >/dev/null
else
    docker run -d --name minio --restart unless-stopped \
        -p 9000:9000 -p 9001:9001 \
        -e MINIO_ROOT_USER="$MINIO_USER" \
        -e MINIO_ROOT_PASSWORD="$MINIO_PASS" \
        -v "$PWD/minio-data:/data" \
        quay.io/minio/minio server /data --console-address ":9001"
fi

# Wait for the API to come up, then create the bucket (mc mb -p is idempotent)
echo "Waiting for MinIO to be ready..."
for _ in $(seq 1 30); do
    if curl -sf http://localhost:9000/minio/health/live >/dev/null; then break; fi
    sleep 1
done

docker run --rm --network host --entrypoint sh quay.io/minio/mc -c \
    "mc alias set local http://localhost:9000 '$MINIO_USER' '$MINIO_PASS' && mc mb -p local/$BUCKET"

echo
echo "MinIO is up:"
echo "  S3 API:      http://localhost:9000   (S3_ENDPOINT_URL)"
echo "  Web console: http://localhost:9001   (login: $MINIO_USER)"
echo "  Bucket:      s3://$BUCKET"
