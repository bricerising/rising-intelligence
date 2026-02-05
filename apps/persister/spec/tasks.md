# Tasks: Persister Service

## Phase 1: Basic Consumer + Postgres Write

- [ ] Create service skeleton (`src/index.ts`, `src/config.ts`)
- [ ] Set up Kafka consumer with manual offset commits
- [ ] Implement Postgres write with Prisma
- [ ] Handle unique constraint (duplicate detection)
- [ ] Add basic logging

## Phase 2: Redis Integration

- [ ] Set up Redis client
- [ ] Implement `seen:*` key writes with TTL
- [ ] Handle Redis unavailability gracefully
- [ ] Add Redis to readiness check

## Phase 3: Reliability + Observability

- [ ] Implement retry logic with backoff for Postgres
- [ ] Add Prometheus metrics
- [ ] Implement health check endpoints (`/healthz`, `/readyz`)
- [ ] Create Grafana dashboard
- [ ] Add OpenTelemetry tracing

## Phase 4: Testing + Hardening

- [ ] Unit tests for mapping logic
- [ ] Integration tests with Testcontainers
- [ ] Soak test (24h)
- [ ] Chaos test (Postgres/Redis failures)
