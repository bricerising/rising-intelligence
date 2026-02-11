# Quickstart: Collector Service

## Overview

This service runs as part of the local Compose stack.

## Run (planned)

```bash
docker compose up --build collector
```

## Phase-1 POS Source-Pack Setup (planned)

- Tech feeds: `infra/config/feeds.tech.yaml` (renamed from `feeds.yaml`)
- POS feeds: `infra/config/feeds.pos.yaml`
- Market profiles: `infra/config/market-filters/*.yaml`
- EDGAR polling baseline: 30 minutes with jitter
- EDGAR form allowlist: `8-K,6-K,10-Q,10-K,20-F,40-F`

## Verify (planned)

- `events.raw` contains `RawEvent` messages from enabled sources.
- Grafana shows `ri_collector_events_ingested_total{source=...}` increasing.
- Retained events include market tags (for example `market.pos`) and `source_meta.market_profiles`.
