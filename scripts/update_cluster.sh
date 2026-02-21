#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="search"
COMPOSE_FILE="docker-compose.yml"

echo "[1/5] Deploying stack definition"
docker stack deploy -c "$COMPOSE_FILE" "$STACK_NAME"

echo "[2/5] Scaling app services down for migration window"
docker service scale \
  "${STACK_NAME}_crawler-worker=0" \
  "${STACK_NAME}_search-api=0" \
  "${STACK_NAME}_batch-jobs=0"

echo "[3/5] Running alembic migrations"
docker service update --force "${STACK_NAME}_migrator" >/dev/null

for i in $(seq 1 60); do
  state="$(docker service ps "${STACK_NAME}_migrator" --no-trunc --format '{{.CurrentState}}' | head -n 1 || true)"
  if [[ "$state" == Complete* ]]; then
    echo "Migration complete"
    break
  fi
  if [[ "$state" == Failed* ]] || [[ "$state" == Rejected* ]]; then
    echo "Migration failed with state: $state"
    exit 1
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "Migration timed out"
    exit 1
  fi
done

echo "[4/5] Scaling app services back up"
docker service scale \
  "${STACK_NAME}_crawler-worker=3" \
  "${STACK_NAME}_search-api=1" \
  "${STACK_NAME}_batch-jobs=1"

echo "[5/5] Update complete"
