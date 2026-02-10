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

Recommended: use `riops brief trigger` to publish request payloads.

## E2E With Mock LLM

Run the Compose-backed end-to-end test (real Kafka/Postgres/Redis + mock HTTP LLM):

```bash
npm run test:e2e:brief:compose
```

## Local Codex CLI Provider

Use local Codex CLI for brief generation:

```bash
LLM_PROVIDER=codex-cli \
LLM_CODEX_CLI_COMMAND=codex \
LLM_CODEX_MODEL=gpt-5-codex \
npm run dev --workspace=@rising-intelligence/brief
```

## Planned Query-Mode Payload (Spec)

Target payload shape for query mode (implementation tracked in `apps/brief/spec/tasks.md` Phase 7):

```json
{
  "request_id": "manual-query-1700000000",
  "requested_at": "2026-02-10T14:10:00Z",
  "type": "daily",
  "windows": [2],
  "query": {
    "lookback_days": 7,
    "topic_globs": ["*"]
  },
  "budget": {
    "daily_budget_usd": 5,
    "max_topics": 10,
    "max_evidence_per_topic": 5,
    "max_output_tokens": 1800
  },
  "topics": []
}
```

Notes:
1. Query mode ranking reads `TREND_WINDOW_60M` snapshots only.
2. `lookback_days` defaults to `7` and is capped at `30`.

## Docker + Codex CLI Provider

Run the Dockerized brief service with local Codex CLI:

```bash
BRIEF_LLM_PROVIDER=codex-cli \
BRIEF_LLM_CODEX_MODEL=gpt-5-codex \
docker compose up --build brief
```

Notes:
1. The Compose service mounts `${HOME}/.codex` into the container.
2. Ensure you are already logged in with `codex login` on the host.
