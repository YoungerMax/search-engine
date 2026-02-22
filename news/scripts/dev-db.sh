#!/usr/bin/env bash
set -euo pipefail

DIR="$(dirname "$0")"
set -a; source "$DIR/../.env"; set +a

POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-news}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"

echo "Starting temporary PostgreSQL database..."
docker run --rm \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -e POSTGRES_DB="$POSTGRES_DB" \
    -p "$POSTGRES_PORT:5432" \
    postgres:18-alpine

echo "PostgreSQL stopped."
