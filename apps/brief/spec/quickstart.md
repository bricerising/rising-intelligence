# Quickstart: Brief Service

## Run

```bash
docker compose up --build brief
```

## Verify

1. Check health and readiness:

```bash
curl -fsS http://localhost:3005/health | jq .
curl -fsS http://localhost:3005/ready | jq .
```

2. Confirm metrics endpoint is exposed:

```bash
curl -fsS http://localhost:3005/metrics
```

3. Publish a test `summary.requests` message and confirm the service logs a consumed request.
