# Quickstart: Persister Service

## Overview

The Persister service consumes events from Kafka (`events.raw`) and materializes them to:
- **Postgres**: Queryable `raw_events` table for dashboards and evidence retrieval
- **Redis**: Short-term deduplication cache (`seen:*` keys)

This service is the bridge between the append-only event log (Kafka) and the queryable read model (Postgres).

## Run

```bash
docker compose up --build persister
```

## Verify

- `raw_events` table in Postgres contains events from `events.raw` topic
- `ri_persister_events_processed_total` metric increasing in Grafana
- No duplicate `event_id` values in Postgres (UNIQUE constraint)
- Consumer lag is low (`ri_persister_consumer_lag` < 1000)

## Dependencies

- **Kafka (Redpanda)**: Source of events
- **Postgres**: Target for materialized events
- **Redis**: Required deduplication/state dependency

## Configuration

| Env Var | Required | Default | Description |
|---------|----------|---------|-------------|
| `KAFKA_BROKERS` | No | `localhost:9092` | Kafka broker addresses |
| `KAFKA_CLIENT_ID` | No | `persister` | Kafka client ID |
| `KAFKA_CONSUMER_GROUP` | No | `persister` | Consumer group ID |
| `KAFKA_TOPIC_RAW_EVENTS` | No | `events.raw` | Input topic |
| `DATABASE_URL` | No | derived from `POSTGRES_*` | Postgres connection string |
| `POSTGRES_HOST` | No | `localhost` | Fallback host when `DATABASE_URL` is unset |
| `POSTGRES_PORT` | No | `5432` | Fallback port when `DATABASE_URL` is unset |
| `POSTGRES_DB` | No | `rising_intelligence` | Fallback database when `DATABASE_URL` is unset |
| `POSTGRES_USER` | No | `rising` | Fallback user when `DATABASE_URL` is unset |
| `POSTGRES_PASSWORD` | No | `rising` | Fallback password when `DATABASE_URL` is unset |
| `REDIS_URL` | No | `redis://localhost:6379` | Redis URL for dedup cache |
| `SEEN_TTL_SECONDS` | No | `86400` | Redis TTL for `seen:*` keys |
| `CONSUMER_LAG_UPDATE_INTERVAL_MS` | No | `15000` | Lag write interval to `consumer_lag` |
| `POSTGRES_CIRCUIT_FAILURE_THRESHOLD` | No | `5` | Failures before opening circuit |
| `POSTGRES_CIRCUIT_OPEN_MS` | No | `30000` | Pause duration while circuit is open |
