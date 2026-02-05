# Spec 000: Quickstart (Local Development)

**Created**: 2026-02-05  
**Status**: Proposed

## Overview

This document describes how to run the Real-Time Personal Intelligence System locally using Docker Compose, following the same “production-aligned local stack” philosophy as `specify-poker`.

## Requirements

- Docker Desktop (or Docker Engine) with Compose
- Node.js 20 LTS (planned; for TypeScript services + tools)

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

## Default local URLs

These ports intentionally mirror `specify-poker` to reduce cognitive load:

- Grafana: `http://localhost:3001` (default auth in local dev: `admin/admin`)
- Loki: `http://localhost:3100`
- Tempo: `http://localhost:3200`
- Mimir (Prometheus API): `http://localhost:9009`
- OTLP ingest: `localhost:4317` (gRPC), `localhost:4318` (HTTP)
- Kafka API (Redpanda or Kafka): `localhost:9092`

## First smoke test (planned)

1. Bring up the stack and confirm Grafana loads.
2. Confirm Loki is receiving logs from at least one service.
3. Confirm Mimir has `up` for expected services (or the collector scrape jobs).
4. Confirm a `trends.snapshot` (or equivalent) is visible in dashboards once the pipeline runs.

## Where to look next

- System spec: `specs/001-real-time-personal-intelligence-system.md`
- Observability: `specs/002-observability-stack.md`
