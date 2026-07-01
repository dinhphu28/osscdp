#!/usr/bin/env bash
# Restore a pg_dump (custom format) into a target database in the compose
# postgres container. Creates the target DB if missing.
# Usage: scripts/restore.sh <dump-file> [target-db]
set -euo pipefail

FILE="${1:?usage: restore.sh <dump-file> [target-db]}"
TARGET="${2:-cdp_restore}"
CONTAINER="${PG_CONTAINER:-deploy-postgres-1}"
USER="${POSTGRES_USER:-cdp}"

docker exec "$CONTAINER" psql -U "$USER" -d postgres -c "DROP DATABASE IF EXISTS $TARGET;" >/dev/null
docker exec "$CONTAINER" psql -U "$USER" -d postgres -c "CREATE DATABASE $TARGET;" >/dev/null
docker exec -i "$CONTAINER" pg_restore -U "$USER" -d "$TARGET" --no-owner < "$FILE"
echo "restored into database: $TARGET"
