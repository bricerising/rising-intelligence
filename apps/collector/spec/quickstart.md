# Quickstart: Collector Service

## Overview

This service runs as part of the local Compose stack.

## Run (planned)

```bash
docker compose up --build collector
```

## Verify (planned)

- `events.raw` contains `RawEvent` messages from enabled sources.
- Grafana shows `ri_collector_events_ingested_total{source=...}` increasing.
