# rising-intelligence Development Guidelines

Last updated: 2026-02-05

## Spec-first workflow

- System intent and cross-cutting constraints live in `specs/`.
- Each service MUST maintain a local spec bundle in `apps/<service>/spec/`:
  - `spec.md`, `plan.md`, `tasks.md`, `data-model.md`, `quickstart.md`
- Specs are the source of truth; keep them aligned with implementation.

## Repository structure

```text
apps/           # Deployable services
packages/       # Shared contracts + platform utilities (ex: packages/shared)
infra/          # Local infra configs (Grafana, OTel, etc.)
specs/          # System-level specs
```

## Observability

- Use OpenTelemetry and the LGTM stack (see `specs/002-observability-stack.md`).
- Services SHOULD expose `/metrics` and emit logs with `traceId` for correlation.

## Operations CLI

- Infrastructure and LGTM stack interactions SHOULD go through the ops CLI: `packages/ops-cli` (binary: `riops`).
- Prefer extending `riops` over adding one-off shell scripts.
