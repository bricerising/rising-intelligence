# `@rising-intelligence/ops-cli` (riops)

This is the operations CLI for interacting with local infrastructure (Redpanda/Kafka, Schema Registry, and the LGTM stack).

Guiding rule: prefer extending `riops` over adding one-off shell scripts.

## Usage (local dev)

Install + build:

```bash
npm install
npm run build
```

Run:

```bash
./node_modules/.bin/riops --help
./node_modules/.bin/riops lgtm urls
./node_modules/.bin/riops schema-registry publish-protos
./node_modules/.bin/riops kafka ensure-topics
./node_modules/.bin/riops brief trigger --dry-run
./node_modules/.bin/riops brief trigger --lookback-days 7 --topic-globs "aws.*,ai.*" --dry-run
./node_modules/.bin/riops brief trigger --report-timezone America/New_York --report-start-at 2026-01-01T00:00:00-05:00 --report-end-at 2026-02-10T23:59:59-05:00 --dry-run
./node_modules/.bin/riops brief trigger --topic-key aws.bedrock --evidence-url https://example.com/bedrock
./node_modules/.bin/riops brief diagnose --timeout 180
./node_modules/.bin/riops brief diagnose --docker-compose-file docker-compose.test.yml --docker-compose-project ri-brief-e2e-a --docker-service brief-test
./node_modules/.bin/riops topics list --counts --min-count 5
./node_modules/.bin/riops topics retag --dry-run
./node_modules/.bin/riops topics retag --source rss --limit 100 --dry-run
./node_modules/.bin/riops topics retag --all --batch-size 500
./node_modules/.bin/riops events enrich --dry-run
./node_modules/.bin/riops events enrich --steps retag,quality --source rss --limit 100 --dry-run
./node_modules/.bin/riops events enrich --missing-only --batch-size 500 --dry-run
./node_modules/.bin/riops db snapshot
./node_modules/.bin/riops db snapshot --output-dir ./backups/postgres --label manual --retention-days 30
./node_modules/.bin/riops db snapshot --dry-run
./node_modules/.bin/riops db test-bootstrap-check --postgres-port 5433 --postgres-db rising_intelligence_test
./node_modules/.bin/riops e2e brief-run --compose-project ri-brief-e2e --keep-up
```

## Usage (via Docker Compose)

The local Compose stack includes an `ops-cli` one-shot service that runs on startup.
It also includes a long-running `postgres-snapshot` service that executes `riops db snapshot --loop` daily.

Re-run manually:

```bash
docker compose run --rm ops-cli schema-registry publish-protos
docker compose run --rm ops-cli db snapshot
```

## Commands

- `lgtm urls`: print local endpoints for Grafana/Loki/Tempo/Mimir/OTLP/Kafka/Schema Registry
- `schema-registry publish-protos`: publish Protobuf schemas + subjects to Schema Registry
- `kafka ensure-topics`: idempotently create required/custom topics and wait for leaders to be ready
- `brief trigger`: generate + publish a manual `SummaryRequest` to `summary.requests` (query mode by default, explicit mode supported with topic/evidence flags)
- `brief diagnose`: run health/topic/lag checks and trigger+classify a brief request outcome
- `topics list`: list distinct topic keys currently present in `raw_events` (optional counts)
- `topics retag`: recompute `raw_events.tags` + `raw_events.topics` from the allowlist rules (defaults to rows with empty tags/topics)
- `events enrich`: run a pluggable enrichment pipeline (default `retag,quality`) to retrofit topics + ingest quality metadata on `raw_events`
- `db snapshot`: create Postgres `pg_dump` snapshots with optional retention pruning and loop mode for schedulers
- `db test-bootstrap-check`: validate test database tables and expected Prisma migrations
- `e2e brief-run`: run compose-backed brief e2e with isolated project/port overrides

## Env

`riops` loads `.env` from repo root by default.

Override path:

- `RI_ENV_PATH=/path/to/.env ./node_modules/.bin/riops ...`
