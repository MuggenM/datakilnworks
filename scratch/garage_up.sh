#!/bin/sh
# A THROWAWAY single-node Deuxfleurs Garage (dxflrs/garage; a real S3 implementation). Garage stands in for MinIO in the manual S3 test scripts:
# MinIO's Docker Hub image can no longer be pulled without a login, and Garage is simpler to run for a single throwaway node. The one exception is
# scratch/verify_s3_events_ui.py, which needs MinIO's bucket-notification/webhook feature; Garage has no S3 notification API at all.
# Usage: eval "$(scratch/garage_up.sh [network] [container])"   # defaults: gtest-net-s3 / gtest-garage
# Prints GARAGE_ENDPOINT=<container>:3900, GARAGE_KEY_ID=..., GARAGE_SECRET_KEY=... to stdout (progress goes to stderr).
# Remove with: docker rm -f <container>; docker network rm <network>
set -e
NET="${1:-gtest-net-s3}"; NAME="${2:-gtest-garage}"
docker network create "$NET" >/dev/null 2>&1 || true
docker rm -f "$NAME" >/dev/null 2>&1 || true
CFG=$(mktemp -d)
SECRET=$(openssl rand -hex 32)
cat > "$CFG/garage.toml" <<EOF
metadata_dir = "/tmp/meta"
data_dir = "/tmp/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$SECRET"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"

[admin]
api_bind_addr = "[::]:3903"
admin_token = "adm"
EOF
docker run -d --name "$NAME" --network "$NET" -v "$CFG/garage.toml:/etc/garage.toml" dxflrs/garage:v1.0.1 >&2
for i in $(seq 30); do docker exec "$NAME" /garage node id -q >/dev/null 2>&1 && break; sleep 1; done
NODE=$(docker exec "$NAME" /garage node id -q | cut -d@ -f1)
docker exec "$NAME" /garage layout assign -z dc1 -c 1G "$NODE" >&2
docker exec "$NAME" /garage layout apply --version 1 >&2
OUT=$(docker exec "$NAME" /garage key create test 2>/dev/null)
KEY_ID=$(echo "$OUT" | awk -F': ' '/^Key ID:/{print $2}')
SECRET_KEY=$(echo "$OUT" | awk -F': ' '/^Secret key:/{print $2}')
docker exec "$NAME" /garage key allow --create-bucket test >&2
rm -rf "$CFG"
echo "GARAGE_ENDPOINT=$NAME:3900"
echo "GARAGE_KEY_ID=$KEY_ID"
echo "GARAGE_SECRET_KEY=$SECRET_KEY"
