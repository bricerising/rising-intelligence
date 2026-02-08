#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.test.yml"
PROFILE="e2e"

cleanup() {
  docker compose -f "$COMPOSE_FILE" --profile "$PROFILE" down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" --profile "$PROFILE" up -d --build
node tests/e2e/brief-mock-llm.e2e.mjs
