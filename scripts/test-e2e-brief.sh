#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.test.yml"
PROFILE="e2e"
PROJECT="${E2E_BRIEF_COMPOSE_PROJECT:-ri-brief-e2e}"
E2E_KAFKA_HOST_PORT="${E2E_KAFKA_HOST_PORT:-9093}"
E2E_BRIEF_HOST_PORT="${E2E_BRIEF_HOST_PORT:-3006}"
E2E_KEEP_UP="${E2E_KEEP_UP:-false}"

cleanup() {
  if [ "$E2E_KEEP_UP" = "true" ]; then
    return
  fi
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" up -d --build \
  redpanda-test postgres-test redis-test mock-llm-test

redpanda_ready=false
for attempt in $(seq 1 60); do
  if docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" exec -T redpanda-test \
    rpk cluster health >/dev/null 2>&1; then
    redpanda_ready=true
    break
  fi
  sleep 1
done

if [ "$redpanda_ready" != "true" ]; then
  echo "Timed out waiting for redpanda-test to become healthy" >&2
  exit 1
fi

topics_ready=false
for attempt in $(seq 1 30); do
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" exec -T redpanda-test \
    rpk topic create summary.requests summary.results trends.snapshots \
    --brokers redpanda-test:9092 --partitions 1 --replicas 1 >/dev/null 2>&1 || true

  if docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" exec -T redpanda-test \
    rpk topic describe summary.requests --brokers redpanda-test:9092 >/dev/null 2>&1 \
    && docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" exec -T redpanda-test \
    rpk topic describe summary.results --brokers redpanda-test:9092 >/dev/null 2>&1 \
    && docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" exec -T redpanda-test \
    rpk topic describe trends.snapshots --brokers redpanda-test:9092 >/dev/null 2>&1; then
    topics_ready=true
    break
  fi

  sleep 1
done

if [ "$topics_ready" != "true" ]; then
  echo "Timed out initializing e2e Kafka topics" >&2
  exit 1
fi

docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --profile "$PROFILE" up -d --build brief-test
E2E_KAFKA_BROKER="localhost:${E2E_KAFKA_HOST_PORT}" \
E2E_BRIEF_HEALTH_URL="http://localhost:${E2E_BRIEF_HOST_PORT}/health" \
E2E_BRIEF_METRICS_URL="http://localhost:${E2E_BRIEF_HOST_PORT}/metrics" \
node tests/e2e/brief-mock-llm.e2e.mjs
