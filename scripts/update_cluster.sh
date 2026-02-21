#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="search"
COMPOSE_FILE="docker-compose.yml"

echo "[1/5] Deploying stack definition"
docker stack deploy -c "$COMPOSE_FILE" "$STACK_NAME"

echo "[2/5] Scaling app services down for migration window"
docker service scale \
  "${STACK_NAME}_migrator=0" \
  "${STACK_NAME}_crawler-worker=0" \
  "${STACK_NAME}_search-api=0" \
  "${STACK_NAME}_batch-jobs=0"

echo "[3/5] Waiting for postgres to be running"
for i in $(seq 1 60); do
  postgres_state="$(docker service ps "${STACK_NAME}_postgres" --no-trunc --format '{{.CurrentState}}' | head -n 1 || true)"
  if [[ "$postgres_state" == Running* ]]; then
    echo "Postgres service is running"
    break
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "Postgres did not reach running state"
    exit 1
  fi
done

echo "[4/5] Running alembic migrations with retries"
migration_success=false
for attempt in $(seq 1 5); do
  echo "Migration attempt ${attempt}/5"
  docker service scale "${STACK_NAME}_migrator=1" >/dev/null
  docker service update --force "${STACK_NAME}_migrator" >/dev/null

  for i in $(seq 1 60); do
    state="$(docker service ps "${STACK_NAME}_migrator" --no-trunc --format '{{.CurrentState}}' | head -n 1 || true)"
    if [[ "$state" == Complete* ]]; then
      echo "Migration complete"
      migration_success=true
      break 2
    fi
    if [[ "$state" == Failed* ]] || [[ "$state" == Rejected* ]]; then
      echo "Migration attempt ${attempt} failed with state: $state"
      break
    fi
    sleep 2
    if [[ "$i" -eq 60 ]]; then
      echo "Migration attempt ${attempt} timed out"
      break
    fi
  done

  docker service scale "${STACK_NAME}_migrator=0" >/dev/null
  sleep 3
done

if [[ "$migration_success" != true ]]; then
  echo "Migration failed after all retry attempts"
  exit 1
fi

docker service scale "${STACK_NAME}_migrator=0" >/dev/null

echo "[5/5] Scaling app services back up"
docker service scale \
  "${STACK_NAME}_crawler-worker=3" \
  "${STACK_NAME}_search-api=1" \
  "${STACK_NAME}_batch-jobs=1"

echo "[6/6] Update complete"
