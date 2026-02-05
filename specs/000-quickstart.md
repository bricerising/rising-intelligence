# Spec 000: Quickstart (Local Development)

**Created**: 2026-02-05  
**Status**: Proposed

## Overview

This document describes how to run the Real-Time Personal Intelligence System locally using Docker Compose, following the same “production-aligned local stack” philosophy as `specify-poker`.

## Requirements

- Docker Desktop (or Docker Engine) with Compose
- Node.js 20 LTS (planned; for TypeScript services + tools)

## Install repo tooling (once)

```bash
npm install
npm run build
```

## Configure environment (recommended)

Create a local `.env` file (not committed) from the example:

```bash
cp .env.example .env
```

At minimum, set `POSTGRES_PASSWORD` to a non-default value.

## Start the local stack

The default workflow is:

```bash
docker compose up --build
```

Optional: run detached:

```bash
docker compose up -d --build
```

Stop (keep state):

```bash
docker compose down
```

Reset local state (remove volumes):

```bash
docker compose down -v
```

## Optional profiles

This repo uses Compose profiles for optional tooling:

- Redpanda Console (topic + schema browsing):
  - `docker compose --profile console up -d`
- Promtail (Linux-only; for scraping Docker container logs):
  - `docker compose --profile promtail up -d`

On macOS, prefer OTLP logs from services → `otel-collector` → Loki (Promtail host mounts are Linux-specific).

## Default local URLs

These ports intentionally mirror `specify-poker` to reduce cognitive load:

- Grafana: `http://localhost:3001` (default auth in local dev: `admin/admin`)
- Loki: `http://localhost:3100`
- Tempo: `http://localhost:3200`
- Mimir (Prometheus API): `http://localhost:9009`
- OTLP ingest: `localhost:4317` (gRPC), `localhost:4318` (HTTP)
- Kafka API (Redpanda or Kafka): `localhost:9092`
- Schema Registry (Redpanda): `http://localhost:8081`
- Redpanda Console (optional): `http://localhost:8080`
- Postgres: `localhost:5432` (db: `$POSTGRES_DB`, user: `$POSTGRES_USER`)

## First smoke test (planned)

1. Bring up the stack and confirm Grafana loads.
2. Confirm Loki is receiving logs from at least one service.
3. Confirm Mimir has `up` for expected services (or the collector scrape jobs).
4. Confirm a `trends.snapshot` (or equivalent) is visible in dashboards once the pipeline runs.

## Contracts (Schema Registry)

The `ops-cli` one-shot service runs on stack startup and publishes Protobuf contracts to Schema Registry (idempotent).

Manual schema publishing (re-run anytime):

```bash
docker compose run --rm ops-cli schema-registry publish-protos
```

Verify schemas in Schema Registry:

```bash
curl http://localhost:8081/subjects
```

## Where to look next

- System spec: `specs/001-real-time-personal-intelligence-system.md`
- Observability: `specs/002-observability-stack.md`
- Contracts + Schema Registry: `specs/003-contracts-and-schema-registry.md`
- Config + Secrets: `specs/004-config-and-secrets.md`
- Postgres read model: `specs/005-postgres-read-model.md`
