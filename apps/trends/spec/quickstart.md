# Quickstart: Trends Service

## Run

```bash
docker compose up --build trends
```

## Verify

1. Check health and readiness:

```bash
curl -fsS http://localhost:3004/health | jq .
curl -fsS http://localhost:3004/ready | jq .
```

2. Confirm metrics endpoint is populated:

```bash
curl -fsS http://localhost:3004/metrics
```

3. Confirm snapshots are published (via Redpanda Console or consumer).
