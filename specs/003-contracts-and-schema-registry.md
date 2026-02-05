# Spec 003: Contracts + Schema Registry (Kafka + gRPC)

**Created**: 2026-02-05  
**Status**: Proposed

## Overview

This project uses **Protobuf** as the canonical wire contract format for:

- Kafka topics (event stream contracts), and
- local-only gRPC APIs (future UI/CLI integrations).

Schemas are published to the **Schema Registry** provided by the local Redpanda stack.

## Goals

- One source of truth for cross-service contracts.
- Stronger safety for evolution (compatibility rules + reviewable diffs).
- Enable future polyglot consumers without rewriting contracts.
- Make it easy to publish/update schemas in local dev and CI.

## Non-goals (MVP)

- Supporting multiple schema formats (Avro/JSON Schema) in addition to Protobuf.
- Public, internet-facing gRPC endpoints.

## Source of truth (repository)

- Protobuf lives in `packages/shared/contracts/proto/`.
- Current canonical files:
  - `packages/shared/contracts/proto/rising_intelligence/v1/contracts.proto`
  - `packages/shared/contracts/proto/rising_intelligence/v1/services.proto`

The TypeScript interfaces shown in `specs/001-*` are **illustrative**. When code exists, TS types and runtime validators should be generated from (or kept strictly aligned with) these Protobuf contracts.

## Schema Registry (local dev)

- Schema Registry URL (host): `http://localhost:8081`
- Schema Registry URL (Compose network): `http://redpanda:8081`

## Subject naming strategy

### Kafka topics (MVP)

Use **TopicNameStrategy**:

- `events.raw-value`
- `events.raw.dlq-value`
- `trends.snapshots-value`
- `summary.requests-value`
- `summary.results-value`

Each subject stores the Protobuf schema that contains the message type published to that topic.

### gRPC Protobufs

Publish gRPC `.proto` files as registry subjects for distribution/audit, separate from Kafka subjects:

- `grpc.rising-intelligence.v1` → `services.proto` (with references)

This is intentionally local-first; we still keep Git as the true source of truth.

## Compatibility rules

Recommended defaults:

- Global compatibility: `BACKWARD`
- Kafka subjects: `BACKWARD_TRANSITIVE` once the system stabilizes (optional)

Protobuf evolution rules (enforced in review):

- Never reuse or renumber fields.
- Prefer additive changes (new optional fields).
- When removing, mark field numbers/names as `reserved`.

## Publishing workflow (planned)

Publishing should be deterministic and scripted:

1) Register `contracts.proto` first (no imports).  
2) Register `services.proto` with a reference to `contracts.proto` (because it imports it).  
3) Register Kafka topic subjects (if not using auto-registration in clients).

Publishing command:

- Preferred (via Compose; uses a Dockerized `riops`):
  - `docker compose run --rm ops-cli schema-registry publish-protos`
- Also supported (local tooling):
  - `npm exec -- riops schema-registry publish-protos`

Inputs:

- `SCHEMA_REGISTRY_URL` (default: `http://localhost:8081`)

Outputs:

- Updated subject versions in Schema Registry.

## Future (post-MVP)

- Adopt Buf for:
  - lint/breaking change detection,
  - generating TS/Go types,
  - publishing to a dedicated Protobuf registry (optional).
