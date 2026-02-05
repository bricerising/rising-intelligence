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
```

## Usage (via Docker Compose)

The local Compose stack includes an `ops-cli` one-shot service that runs on startup.

Re-run manually:

```bash
docker compose run --rm ops-cli schema-registry publish-protos
```

## Commands

- `lgtm urls`: print local endpoints for Grafana/Loki/Tempo/Mimir/OTLP/Kafka/Schema Registry
- `schema-registry publish-protos`: publish Protobuf schemas + subjects to Schema Registry

## Env

`riops` loads `.env` from repo root by default.

Override path:

- `RI_ENV_PATH=/path/to/.env ./node_modules/.bin/riops ...`
