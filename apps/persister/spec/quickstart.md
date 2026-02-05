# Quickstart: Persister Service

## Overview

The Persister service consumes events from Kafka (`events.raw`) and materializes them to:
- **Postgres**: Queryable `raw_events` table for dashboards and evidence retrieval
- **Redis**: Short-term deduplication cache (`seen:*` keys)

This service is the bridge between the append-only event log (Kafka) and the queryable read model (Postgres).

## Run (planned)

```bash
docker compose up --build persister
```

## Verify (planned)

- `raw_events` table in Postgres contains events from `events.raw` topic
- `ri_persister_events_processed_total` metric increasing in Grafana
- No duplicate `event_id` values in Postgres (UNIQUE constraint)
- Consumer lag is low (`ri_persister_consumer_lag` < 1000)

## Dependencies

- **Kafka (Redpanda)**: Source of events
- **Postgres**: Target for materialized events
- **Redis**: Deduplication cache (optional but recommended)

## Configuration

| Env Var | Required | Default | Description |
|---------|----------|---------|-------------|
| `KAFKA_BROKERS` | Yes | — | Kafka broker addresses |
| `KAFKA_CONSUMER_GROUP` | Yes | `persister` | Consumer group ID |
| `POSTGRES_HOST` | Yes | — | Postgres host |
| `POSTGRES_PORT` | No | `5432` | Postgres port |
| `POSTGRES_DB` | Yes | — | Database name |
| `POSTGRES_USER` | Yes | — | Database user |
| `POSTGRES_PASSWORD` | Yes | — | Database password |
| `REDIS_URL` | No | — | Redis URL for dedup cache |
| `BATCH_SIZE` | No | `100` | Events per batch insert |
| `BATCH_TIMEOUT_MS` | No | `1000` | Max wait before flushing batch |
