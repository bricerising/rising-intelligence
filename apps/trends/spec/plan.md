# Implementation Plan: Trends Service

## Overview

Build `apps/trends` as a Kafka consumer + periodic snapshot publisher.

## Architecture (High Level)

- Input: `events.raw` (`RawEvent`)
- Output: `trends.snapshots` (`TrendSnapshot`)
- State:
  - window buckets for 15m/60m
  - optional baseline store (TBD: Redis or DB)

## Phases

### Phase 1: Topic extraction + in-memory windows

- Allowlist matcher
- Windowed counters (15m/60m)
- Publish snapshots periodically

### Phase 2: Durable state + baselines

- Persist baseline history (7d)
- Deterministic evidence selection per topic

### Phase 3: Alerts + summary triggers

- Emit `summary.requests` for “flash brief” triggers (optional)
