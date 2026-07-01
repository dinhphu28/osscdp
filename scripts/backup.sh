#!/usr/bin/env bash
# PostgreSQL backup via the compose postgres container (custom format).
# Usage: scripts/backup.sh [output-dir]
set -euo pipefail

OUT_DIR="${1:-backups}"
CONTAINER="${PG_CONTAINER:-deploy-postgres-1}"
DB="${POSTGRES_DB:-cdp}"
USER="${POSTGRES_USER:-cdp}"

mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
FILE="$OUT_DIR/cdp-$STAMP.dump"

docker exec "$CONTAINER" pg_dump -U "$USER" -d "$DB" -Fc > "$FILE"
echo "backup written: $FILE ($(du -h "$FILE" | cut -f1))"
