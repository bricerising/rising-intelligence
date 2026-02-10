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
