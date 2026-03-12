# Quickstart: Collector Service

## Overview

This service runs as part of the local Compose stack.

## Run

```bash
docker compose up --build collector
```

## Phase-1 POS Source-Pack Setup

- Tech feeds: `infra/config/feeds.tech.yaml` (renamed from `feeds.yaml`)
- Tech feed coverage now includes arXiv category feeds, ZDI advisories, vendor GitHub security streams, critical OSS release Atom feeds, and the Ethereum Foundation blog.
- POS feeds: `infra/config/feeds.pos.yaml`
- Baseline POS feeds include EDGAR/SEC/Fed/BIS/CISA/PR Newswire plus public payments newsroom feeds (PYMNTS, PaymentsJournal).
- Market profiles: `infra/config/market-filters/*.yaml`
- EDGAR polling baseline: 30 minutes with jitter
- EDGAR form allowlist: `8-K,6-K,10-Q,10-K,20-F,40-F`
- EDGAR watchlist includes baseline POS/payments issuers; extend with additional operator-provided entries as needed.
- Deferred JSON/API sources (NVD 2.0, OSV, KEV, Kubernetes CVE JSON) are present in config but remain `enabled: false` until a generic JSON adapter is implemented.

## Verify

- `events.raw` contains `RawEvent` messages from enabled sources.
- Grafana shows `ri_collector_events_ingested_total{source=...}` increasing.
- Retained events include market tags (for example `market.pos`) and `source_meta.market_profiles`.

```bash
npm --workspace @rising-intelligence/collector test -- tests/adapters/rss.test.ts tests/market-filters.test.ts tests/config.test.ts
```
