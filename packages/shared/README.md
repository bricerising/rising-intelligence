# `@rising-intelligence/shared` (Planned)

Shared contracts and platform utilities for this monorepo, following the same philosophy as `@specify-poker/shared`.

This package is intended to be the “boring platform layer” that keeps services consistent:

- **Contracts**: Zod schemas + TypeScript types for `RawEvent`, `TrendSnapshot`, `Brief`, and config shapes.
- **Config**: typed env parsing and config builders.
- **Lifecycle**: service bootstrap and graceful shutdown helpers.
- **Observability**: structured logger setup + Prometheus metrics server helpers + OTel bootstrap wiring.
- **Kafka helpers**: safe producers/consumers, DLQ helpers, idempotent consumption patterns.
- **Resilience**: retry/backoff + timeout helpers for outbound calls.

Non-goal: becoming a “utils junk drawer”. If a helper isn’t used by at least two services or isn’t a cross-cutting concern, it probably does not belong here.
