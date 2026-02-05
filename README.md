# rising-intelligence

Self-hosted, near-real-time personal intelligence system for monitoring tech/AI/AWS trends.

This repo intentionally follows the same **spec-first + monorepo + local Compose** layout as `specify-poker`.

## Start here

- Quickstart: `specs/000-quickstart.md`
- System spec: `specs/001-real-time-personal-intelligence-system.md`
- Observability: `specs/002-observability-stack.md`

## Repository map

```text
apps/           # Deployable services (collector, trends, brief)
packages/       # Shared contracts + platform utilities
infra/          # Grafana/OTel/Loki/Tempo/Mimir provisioning + configs
specs/          # System-level specifications (source of truth)
```
