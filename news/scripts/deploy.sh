#!/usr/bin/env bash
# deploy.sh — Deploy the news stack to Docker Swarm
#
# Usage: ./deploy.sh [STACK_NAME] [IMAGE_TAG]
# Defaults: STACK_NAME=news  IMAGE_TAG=main

set -euo pipefail

STACK="news"
DIR="$(dirname "$0")/.."
IMAGE="ghcr.io/youngermax/news-api:main"

# Load .env
set -a; source "$DIR/.env"; set +a

# ── Scale down ────────────────────────────────────────────────────────────────

if docker stack ls --format '{{.Name}}' | grep -q "^${STACK}$"; then
  echo "Scaling down api and fetcher..."
  docker service scale "${STACK}_api=0" "${STACK}_fetcher=0"
fi

# ── Update service images so Swarm nodes pull the new version ─────────────────

echo "Updating service images to ${IMAGE}..."
docker service update --image "$IMAGE" "${STACK}_api"     2>/dev/null || true
docker service update --image "$IMAGE" "${STACK}_fetcher" 2>/dev/null || true

# ── Migrate ───────────────────────────────────────────────────────────────────

echo "Running migrations..."
docker run --rm \
  --network "${STACK}_news" \
  --env-file "$DIR/.env" \
  "$IMAGE" \
  bun run db:migrate

echo "Migrations complete."

# ── Deploy stack ──────────────────────────────────────────────────────────────

echo "Deploying stack '${STACK}'..."
docker stack deploy \
  --compose-file "$DIR/docker-compose.yml" \
  --with-registry-auth \
  --prune \
  "$STACK"

# ── Scale back up ─────────────────────────────────────────────────────────────

echo "Scaling up api and fetcher..."
docker service scale "${STACK}_api=1" "${STACK}_fetcher=1"

echo "Done. Stack '${STACK}' is running ${IMAGE}."