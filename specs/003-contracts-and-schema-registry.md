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

## Publishing workflow

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

## Schema Evolution Strategy

### Guiding Principles

1. **Backward compatibility by default**: New consumers can read old messages
2. **No breaking changes in production**: If a change breaks old messages, create a new topic version
3. **Explicit over implicit**: When in doubt, add a new field rather than repurpose an existing one

### Safe Changes (Backward Compatible)

These changes can be deployed without coordination:

| Change | Safe? | Notes |
|--------|-------|-------|
| Add optional field | YES | Old consumers ignore it; new consumers use default |
| Add new enum value | YES | Old consumers treat as UNSPECIFIED |
| Add new message type | YES | Doesn't affect existing messages |
| Deprecate field | YES | Mark as `[deprecated = true]`; don't remove |
| Change field from required to optional | YES | Protobuf 3 has no required fields anyway |

### Unsafe Changes (Breaking)

These changes require migration planning:

| Change | Safe? | Mitigation |
|--------|-------|------------|
| Remove field | NO | Mark as `reserved` instead |
| Rename field | NO | Add new field, deprecate old |
| Change field type | NO | Add new field with new type |
| Change field number | NO | Never do this |
| Remove enum value | NO | Mark as `reserved` |

### Consumer Handling of Unknown Fields

All consumers MUST handle unknown fields gracefully:

```typescript
// Generated Protobuf code handles this automatically
// For JSON parsing, use safe defaults:
function parseRawEvent(data: unknown): RawEvent {
  const event = RawEventSchema.parse(data);

  // Provide defaults for optional fields that might not exist in old messages
  return {
    ...event,
    lang: event.lang ?? 'en',
    tags: event.tags ?? [],
    extracted: event.extracted ?? { hashtags: [], urls: [] },
  };
}
```

### Migration Playbook

#### Scenario 1: Adding a New Field

**Example**: Adding `sentiment` field to `RawEvent`.

**Steps**:
1. Add field to `contracts.proto`:
   ```protobuf
   message RawEvent {
     // ... existing fields ...
     string sentiment = 21;  // New field: "positive", "negative", "neutral"
   }
   ```
2. Publish updated schema to registry
3. Deploy new Collector (produces messages with `sentiment`)
4. Deploy new consumers (handles both old messages without `sentiment` and new ones with)
5. No data loss, no downtime

**Consumer code**:
```typescript
const sentiment = event.sentiment || 'unknown';  // Safe default for old messages
```

#### Scenario 2: Changing a Field Type

**Example**: Changing `engagement.score` from `int32` to `int64`.

**Steps**:
1. Add new field (don't modify existing):
   ```protobuf
   message Engagement {
     int32 score = 1;           // Keep for backward compat
     int64 score_v2 = 5;        // New field with larger type
   }
   ```
2. Deploy producer to write BOTH fields
3. Deploy consumers to prefer `score_v2`, fall back to `score`
4. After retention period expires, stop writing `score` (optional)

**Consumer code**:
```typescript
const score = event.engagement.score_v2 ?? event.engagement.score ?? 0;
```

#### Scenario 3: Breaking Change (Last Resort)

**Example**: Complete restructure of `TrendSnapshot` format.

**Steps**:
1. Create new topic: `trends.snapshots.v2`
2. Create new message type: `TrendSnapshotV2`
3. Deploy new producer to write to BOTH topics (transition period)
4. Migrate consumers to new topic one by one
5. After all consumers migrated, stop writing to old topic
6. Let old topic retention expire

**Timeline**:
```
Day 0:   Deploy producer writing to v1 + v2
Day 1-7: Migrate consumers to v2
Day 8:   Stop writing to v1
Day 22:  v1 topic retention expires (14 days)
```

### Retention Alignment

**CRITICAL**: Kafka retention MUST be >= the time needed to complete migrations.

Current retention (from `specs/001`):
- `events.raw`: 14 days
- `trends.snapshots`: 90 days
- `summary.results`: 180 days

This gives ample time for migration. If you need to do a breaking change, you have at least 14 days to coordinate deployment.

### Pre-Deployment Checklist

Before deploying schema changes:

- [ ] Change is backward compatible OR migration plan documented
- [ ] New fields have sensible defaults in consumer code
- [ ] Schema registered in local dev and tested
- [ ] PR includes updated `contracts.proto`
- [ ] Breaking changes discussed and approved

### Monitoring Schema Issues

**Metrics to watch**:
- `kafka_consumer_deserialization_errors_total`: Spike indicates schema mismatch
- `kafka_dlq_messages_total`: Malformed messages going to DLQ

**Alerts**:
```yaml
- alert: SchemaDeserializationErrors
  expr: rate(kafka_consumer_deserialization_errors_total[5m]) > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Schema deserialization errors detected"
    description: "Check for schema compatibility issues between producers and consumers"
```

## Future (post-MVP)

- Adopt Buf for:
  - lint/breaking change detection,
  - generating TS/Go types,
  - publishing to a dedicated Protobuf registry (optional).
