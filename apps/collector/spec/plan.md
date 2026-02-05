# Implementation Plan: Collector Service

## Overview

Build `apps/collector` with modular source adapters producing `RawEvent` to `events.raw`.

## Architecture (High Level)

- Adapters: `rss`, `hackernews`, `reddit`, `github` (optional), `twitter` (optional)
- Shared:
  - config + env parsing
  - HTTP client with retry/backoff
  - checkpoint storage (Redis or local file in dev; TBD)
- Outputs:
  - Kafka: `events.raw` + `events.raw.dlq`
  - Logs: Loki via OTLP (preferred) or stdout + promtail

## Phases

### Phase 1: Service skeleton + Kafka publish

- Create service bootstrap and config
- Implement `RawEvent` schema validation at the boundary
- Publish to `events.raw` and DLQ

### Phase 2: MVP sources

- RSS/Atom adapter (poll + dedupe)
- Hacker News adapter (poll + dedupe)
- Reddit adapter (poll + dedupe)

### Phase 3: Reliability + ops

- Backoff/jitter, rate limit handling
- Cursor checkpointing per source
- Metrics + traces + dashboards
