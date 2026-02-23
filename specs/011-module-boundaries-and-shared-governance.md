# Spec 011: Module Boundaries and Shared Package Governance

**Created**: 2026-02-23
**Status**: Proposed

## Overview

This spec proposes a repository-wide boundary hardening effort for `rising-intelligence`, focused on TypeScript module organization and explicit architectural seams.

The primary issue is not functional correctness. The issue is design drift risk:

- the `packages/shared` surface is broad and permissive,
- `runtime` can be interpreted as "anything reusable",
- domain communication logic (event types, topic classification, Kafka transport) is mixed into platform code,
- internal import boundaries are not currently enforced by linting,
- service `src/` roots are getting flat and broad in ways that hide domain seams.

This document defines:

1. what is allowed in shared platform code,
2. what must be extracted into a dedicated pipeline package,
3. what data pipeline protocol interfaces replace the current Kafka-coupled transport layer,
4. how import boundaries are enforced mechanically,
5. how to migrate without halting feature work.

## Problem Statement

### P1: Shared API surface is too broad by default

`packages/shared/src/index.ts` re-exports 23 modules from `runtime/*` plus inline constants, making root imports convenient but coupling-prone.

Consequence:

- callsites can import any cross-cutting or domain-adjacent utility without architecture review,
- small local decisions can silently increase shared coupling and blast radius.

### P2: `runtime` naming is too permissive

`packages/shared/src/runtime/` includes both platform primitives and product/domain-heavy logic.

Consequence:

- future contributors can rationalize placing product logic in shared runtime,
- shared becomes a long-term "miscellaneous layer" and architecture boundaries collapse.

### P3: Kafka transport leaks into application code

Apps import Kafka producer/consumer factories, batch helpers, and serialization directly from shared. Each service maintains its own `src/kafka/producer.ts` and `src/kafka/consumer.ts` that wrap shared primitives. Every one of these files imports `kafkajs` types (`Producer`, `Consumer`, `Kafka`, `EachBatchPayload`) directly. This couples every service to Kafka as an implementation detail rather than programming against a data pipeline protocol.

Current state: 18 files across `apps/` import directly from `kafkajs`.

Consequence:

- replacing or supplementing Kafka requires touching every service and every process file,
- no clean seam exists between "what the system communicates" (events, topics, sources) and "how it communicates" (Kafka, serialization, batching),
- each app duplicates near-identical producer/consumer boilerplate.

### P4: Boundaries are implicit, not enforced

Current root ESLint config contains no active architecture rules (`rules: {}`).

Consequence:

- boundary violations are detected ad hoc in review,
- drift accumulates until expensive refactors are required.

### P5: Testing API seam in `shared` is ambiguous

`packages/shared/testing/index.ts` documents an import path (`@rising-intelligence/shared/testing`) that is not currently exported by `packages/shared/package.json`.

Consequence:

- implied API and published API are misaligned,
- future tests may depend on unsupported package entry points.

### P6: Barrel-exported constants encode domain vocabulary in platform package

`packages/shared/src/index.ts` directly exports domain-specific constants (`KAFKA_VALUE_SUBJECTS`, `CONTRACTS_SUBJECT`, `GRPC_SUBJECT`, `CONTRACT_REFERENCE_NAME`) that encode product-specific topic names and schema registry subjects. These are not platform primitives.

Consequence:

- shared's index becomes a grab bag of domain vocabulary,
- adding new topics or subjects requires modifying the platform package.

## Goals

1. Make architecture boundaries explicit and machine-enforced.
2. Keep `packages/shared` as platform primitives, not product/domain logic.
3. Extract domain communication into a dedicated `packages/pipeline` package with a data pipeline protocol API.
4. Define concrete pipeline protocol interfaces that hide `kafkajs` from app code entirely.
5. Replace app-level `src/kafka/` directories with pipeline protocol API calls from the pipeline package.
6. Reduce accidental coupling from root-barrel imports via mandatory subpath exports and root barrel deletion.
7. Preserve delivery velocity via coordinated, non-breaking migration.

## Non-goals

1. Full codebase rewrite.
2. Introducing a new build system.
3. Forcing one exact file layout in all services immediately.
4. Changing runtime behavior solely for naming/style consistency.
5. Redesigning Kafka topic contracts or wire formats (the existing event shapes are preserved; only where they live changes).
6. Implementing a second transport backend (this spec creates the seam; a future spec would use it).

## Scope

### In scope

- `packages/shared` API and folder boundaries.
- Creation of `packages/pipeline` for the data pipeline protocol.
- Relocating event, topic, transport, and hydration modules from shared into pipeline.
- Relocating domain-specific constants from shared barrel into pipeline.
- Defining data pipeline protocol interfaces for publishing and consuming.
- Replacing app-level `src/kafka/` producer and consumer files with pipeline protocol API.
- Migrating all import sites (app source, test files, e2e tests, ops-cli) to new package paths.
- Lint/config enforcement for dependency boundaries.
- Docs and workspace dependency normalization.

### Out of scope

- Kafka topic contract redesign.
- Observability metric contract redesign.
- DB schema redesign.
- Replacing Kafka with an alternative transport (this spec creates the seam; a future spec would use it).

## Decision Summary

Selected approach:

1. keep `packages/shared` as platform-only,
2. create `packages/pipeline` (`@rising-intelligence/pipeline`) as the data pipeline protocol package,
3. move event types, topic classification, transport, hydration, and domain constants into `packages/pipeline`,
4. define new data pipeline protocol interfaces (`ProducerConnection`, `ConsumerConnection`, `TopicPublisher`, `BatchStrategy`, `BatchContext`, `PipelineMessage`, `MessageStrategy`, `MessageContext`, `createMessageBatchStrategy`),
5. delete app-level `src/kafka/` directories and replace with pipeline protocol API,
6. expose all shared functionality through explicit subpath exports and delete the root barrel,
7. enforce boundaries through lint rules.

Why:

- `topic-extraction.ts` has multiple consumers (collector and ops-cli) and encodes product-specific policy — it cannot stay in shared (fails Rule A) and cannot move into a single app (the other consumer would need a Rule E violation or code duplication),
- `raw-event.ts` and `source.ts` define the pipeline's API schema and source vocabulary — they describe what flows through the pipeline and belong in the package that owns the pipeline protocol,
- Kafka transport, event wire types, and topic classification are a cohesive domain communication concern that belongs together,
- "pipeline" accurately names a package that contains event types, classification, hydration, and transport — it covers the full event lifecycle, not just messaging,
- a dedicated package with pipeline protocol interfaces creates a clean seam for future transport substitution without touching app code,
- replacing app-level kafka/ directories eliminates 6 near-identical boilerplate files and removes all direct `kafkajs` imports from apps,
- apps and ops-cli are first-class consumers of the same pipeline protocol interface,
- strongest clarity for human and AI contributors,
- minimal runtime risk when all changes land together as a coordinated migration.

## Architecture Rules (Normative)

### Rule A: Shared eligibility rule

A module may live in `packages/shared` only if all are true:

1. it is cross-cutting platform behavior,
2. it is reused (or expected to be reused) by at least 2 consumers,
3. it does not encode product-specific policy or vocabulary,
4. it does not depend on app-local modules.

If any condition fails, the module belongs in an app or a dedicated domain package.

### Rule B: Shared runtime intent

`packages/shared/src/runtime/` is reserved for platform runtime concerns:

- lifecycle/bootstrap/shutdown,
- logging and telemetry primitives,
- config/env parsing primitives,
- HTTP helpers,
- URL utilities,
- generic resilience/control flow primitives.

Disallowed in `runtime/`:

- event types and serialization,
- transport wrappers (Kafka/NATS/SQS),
- business/domain heuristics,
- product-specific classifiers and taxonomy rules,
- topic-specific inference logic,
- domain-specific constants (topic names, schema registry subjects).

### Rule C: Pipeline package intent

`packages/pipeline` owns the data pipeline protocol:

- pipeline API schema and source vocabulary (`RawEvent`, `CanonicalSource`) — the data contract for what flows through the pipeline,
- topic classification and routing (`extractTopics`),
- pipeline protocol interfaces and Kafka adapter implementation,
- data hydration — deriving and attaching computed metadata (URL normalization, language detection, quality signals) to sparse pipeline records — as a decoupled submodule,
- domain constants (topic names, schema registry subjects, contract identifiers).

The public API is a data pipeline protocol (e.g., "create a producer connection", "consume messages with a strategy", "classify topics"). Kafka is an internal implementation detail, not part of the public interface.

### Rule D: No broad root imports for new code

All app code must import from explicit subpaths. Root barrel imports from `@rising-intelligence/shared` are banned. The root barrel (`packages/shared/src/index.ts`) is deleted — root imports are a compile error, not just a lint violation.

### Rule E: App-to-app imports are forbidden

`apps/*` must not import from other `apps/*`. Shared code moves to `packages/*`.

### Rule F: ops-cli is a first-class consumer

`packages/ops-cli` interacts with the system through the same `packages/pipeline` protocol interface as apps. It is not a special case and must not import from `apps/*`.

### Rule G: No direct transport imports in app code

`apps/*` must not import `kafkajs` directly. All transport interaction goes through `@rising-intelligence/pipeline` protocol interfaces.

Exception: `packages/ops-cli/src/commands/kafka/` commands that perform infrastructure management (topic creation, partition listing, cluster administration) may import `kafkajs` directly, as these are operational tasks that manage Kafka as infrastructure, not data pipeline operations. ops-cli commands that perform pipeline operations (e.g., `commands/brief/trigger.ts`, `commands/brief/result-waiter.ts`, `commands/brief/diagnose.ts`) must use the pipeline protocol interfaces, not direct `kafkajs` imports.

## Dependency Graph

```
apps/*      -->  @rising-intelligence/pipeline
apps/*      -->  @rising-intelligence/shared
ops-cli     -->  @rising-intelligence/pipeline
ops-cli     -->  @rising-intelligence/shared
ops-cli     -->  kafkajs (admin commands only, per Rule G exception)
pipeline    -->  @rising-intelligence/shared
```

No cycles. `packages/shared` is the foundational platform layer. `packages/pipeline` depends on shared for platform primitives (logging, config, resilience). Apps and ops-cli depend on both. No app code imports `kafkajs` directly. ops-cli infrastructure management commands (`packages/ops-cli/src/commands/kafka/`) are the sole exception.

## Protocol Interface Design

This section defines the data pipeline protocol interfaces that `packages/pipeline` exposes. Kafka is the sole implementation today, but app code never references `kafkajs` types. Interface methods use pipeline vocabulary grounded in data pipeline operations, not Kafka-specific terminology.

### Publishing

```typescript
// @rising-intelligence/pipeline/transport
// Logger type is imported from @rising-intelligence/shared/logging (pino Logger).

/** Opaque handle to a connected producer. */
interface ProducerConnection {
  /** Publish a single message to a topic. */
  publish(topic: string, key: string, value: Buffer): Promise<void>;
  /** Publish a batch of messages to a topic. Returns false if batch was empty. */
  publishBatch(topic: string, messages: Array<{ key: string; value: Buffer }>): Promise<boolean>;
  /** Gracefully disconnect. */
  disconnect(): Promise<void>;
}

/** Factory to create a producer connection from service config. */
interface CreateProducerConnectionOptions {
  brokers: string | string[];
  clientId: string;
  clientIdSuffix?: string;
  logger: Logger;
}

function createProducerConnection(
  options: CreateProducerConnectionOptions
): Promise<ProducerConnection>;

/** Typed publisher for a single topic. Wraps ProducerConnection. */
interface TopicPublisher<TPayload> {
  publish(key: string, payload: TPayload): Promise<void>;
}

/** Typed publisher that derives key from payload. */
interface KeyedTopicPublisher<TPayload> {
  publish(payload: TPayload): Promise<void>;
}

interface CreateTopicPublisherOptions<TPayload> {
  connection: ProducerConnection;
  topic: string;
  serialize?: (payload: TPayload) => Buffer;
}

function createTopicPublisher<TPayload>(
  options: CreateTopicPublisherOptions<TPayload>
): TopicPublisher<TPayload>;

interface CreateKeyedTopicPublisherOptions<TPayload>
  extends CreateTopicPublisherOptions<TPayload> {
  getKey(payload: TPayload): string;
}

function createKeyedTopicPublisher<TPayload>(
  options: CreateKeyedTopicPublisherOptions<TPayload>
): KeyedTopicPublisher<TPayload>;
```

### Consuming

The consuming API is two-tiered: `BatchStrategy` is the core primitive for full batch lifecycle control; `MessageStrategy` + `createMessageBatchStrategy()` is the convenience layer for per-message processing.

```typescript
// @rising-intelligence/pipeline/transport

// ── Message and batch primitives ──

/** A single message delivered from the pipeline. */
interface PipelineMessage {
  key: Buffer | null;
  value: Buffer | null;
  /** Opaque position identifier (maps to Kafka offset internally). */
  position: string;
  timestamp: string;
}

/**
 * Batch-level operations available during processing.
 * Provides lifecycle control without exposing transport internals.
 */
interface BatchContext {
  /** Topic this batch was delivered from. */
  topic: string;
  /** Partition identifier. */
  partition: number;
  /** High watermark position for consumer lag calculation. */
  highWatermark: string;
  /** Whether the consumer is still active (not shutting down or rebalancing). */
  isActive(): boolean;
  /** Signal liveness to the broker. Call periodically during long-running work. */
  keepAlive(): Promise<void>;
  /** Mark a message position as processed. */
  acknowledge(position: string): void;
  /** Flush acknowledged positions to the broker. */
  commit(): Promise<void>;
  /**
   * Pause message delivery for this partition. Returns a resume callback.
   * Use for backpressure when a downstream dependency is unavailable
   * (e.g., circuit breaker open, rate limit hit).
   */
  pause(): () => void;
}

// ── Strategy interfaces ──

/**
 * Batch-level processing strategy. The core primitive.
 *
 * Apps implement this interface when they need full control over message
 * iteration, acknowledgment timing, commit cadence, or partition lifecycle.
 * The pipeline delivers a batch of messages and a BatchContext; the
 * strategy owns the entire processing loop.
 *
 * Used by: persister (circuit breaker + batch-level persistence + custom
 * offset/commit/lag orchestration).
 */
interface BatchStrategy<TContext> {
  processBatch(
    ctx: TContext,
    batch: BatchContext,
    messages: readonly PipelineMessage[]
  ): Promise<void>;
}

/** Context for a single message, available to MessageStrategy handlers. */
interface MessageContext {
  topic: string;
  partition: number;
  position: string;
  /** Signal liveness during long-running message processing (e.g., LLM calls). */
  keepAlive(): Promise<void>;
}

/**
 * Per-message processing strategy for the common case.
 *
 * Apps implement this interface when per-message processing is sufficient
 * and standard lifecycle management (auto-acknowledge, periodic keepAlive,
 * commit after batch) is acceptable.
 *
 * Wrap with createMessageBatchStrategy() to produce a BatchStrategy.
 *
 * Used by: trends (per-message event/heartbeat processing),
 * brief (per-message request handling with keepAlive for LLM calls).
 */
interface MessageStrategy<TContext, TMessage> {
  deserialize(value: Buffer): TMessage;
  onEmptyValue?(ctx: TContext, messageContext: MessageContext): Promise<void> | void;
  onDeserializeFailure?(ctx: TContext, messageContext: MessageContext, error: unknown): Promise<void> | void;
  onMessage(ctx: TContext, messageContext: MessageContext, decoded: TMessage): Promise<void> | void;
}

// ── Strategy factory ──

/**
 * Wraps a per-message MessageStrategy into a BatchStrategy with standard
 * lifecycle: iterate messages → deserialize → call handler →
 * auto-acknowledge → periodic keepAlive → commit after batch.
 *
 * Replaces the existing createKafkaBatchLifecycle + processKafkaBatchMessages
 * + runKafkaMessageBatch composition for the common case.
 */
interface CreateMessageBatchStrategyOptions<TContext, TMessage> {
  strategy: MessageStrategy<TContext, TMessage>;
  /** Messages between automatic keepAlive calls. Default: 50. */
  progressInterval?: number;
  /** Whether to auto-acknowledge each processed message. Default: true. */
  acknowledge?: boolean;
  /** Called after all messages in a batch are processed and committed. */
  onBatchCompleted?(ctx: TContext): Promise<void> | void;
}

function createMessageBatchStrategy<TContext, TMessage>(
  options: CreateMessageBatchStrategyOptions<TContext, TMessage>
): BatchStrategy<TContext>;

// ── Consumer connection ──

/** Options for consuming messages from topics. */
interface ConsumeOptions<TContext> {
  topics: string | string[];
  ctx: TContext;
  /**
   * Single strategy for all topics, or per-topic strategy map for routing.
   * When a Map is provided, batches for unmapped topics are skipped
   * (offsets acknowledged, warning logged via the connection logger).
   * Replaces the existing createTopicBatchRouter pattern.
   */
  strategy: BatchStrategy<TContext> | ReadonlyMap<string, BatchStrategy<TContext>>;
  fromBeginning?: boolean;
}

/** Opaque handle to a connected consumer. */
interface ConsumerConnection {
  /**
   * Subscribe to topics and begin consuming using the provided strategy.
   * When strategy is a Map, each topic's batch is routed to its strategy.
   */
  consume<TContext>(options: ConsumeOptions<TContext>): Promise<void>;
  /** Gracefully disconnect. */
  disconnect(): Promise<void>;
}

/** Factory to create a consumer connection. */
interface CreateConsumerConnectionOptions {
  brokers: string | string[];
  clientId: string;
  groupId: string;
  logger: Logger;
  sessionTimeoutMs?: number;
}

function createConsumerConnection(
  options: CreateConsumerConnectionOptions
): Promise<ConsumerConnection>;
```

### Design Rationale

The consuming API is two-tiered to accommodate the range of batch processing complexity across services:

**`BatchStrategy` is the core primitive.** The pipeline delivers a batch of messages and a `BatchContext` with lifecycle operations. The strategy owns the entire processing loop: message iteration, deserialization, acknowledgment, commit timing, and partition control. This is not an abstraction layer — it is the direct replacement for receiving `EachBatchPayload` and manually calling its methods, with Kafka-specific names replaced by pipeline vocabulary.

**`MessageStrategy` + `createMessageBatchStrategy()` is the convenience layer.** Most services process messages individually with standard lifecycle management. `createMessageBatchStrategy()` wraps a `MessageStrategy` into a `BatchStrategy` that handles iteration, periodic keepAlive, auto-acknowledgment, and commit-after-batch automatically. This replaces the existing `createKafkaBatchLifecycle` + `processKafkaBatchMessages` + `runKafkaMessageBatch` three-function composition with a single factory call.

**Topic routing via strategy Map** replaces `createTopicBatchRouter`. When `ConsumeOptions.strategy` is a `ReadonlyMap<string, BatchStrategy>`, the consumer connection routes each batch to the strategy registered for that topic. Unmapped topics are skipped (offsets acknowledged, warning logged). This moves topic routing from app-level boilerplate into the pipeline protocol.

**`MessageContext.keepAlive()`** is available to `MessageStrategy.onMessage()` handlers for long-running per-message work (e.g., brief's LLM generation). The handler can call `keepAlive()` directly or wrap its work in a timer-based keepAlive loop — the mechanism is under app control, but the pipeline provides the hook without exposing Kafka's heartbeat API.

**`BatchContext.pause()`** enables backpressure patterns. Persister's circuit breaker pauses the partition when Postgres is unavailable, heartbeats while waiting for recovery, then resumes. This is a `BatchStrategy`-only capability — `MessageStrategy` users do not need partition control because `createMessageBatchStrategy()` handles lifecycle automatically.

#### Publishing rationale

`ProducerConnection.publish()` wraps the existing `publishKafkaTopicMessage()` + `createKafkaProducerProxy()` pattern. No changes from the original design.

#### Kafka adapter mapping

The Kafka adapter maps pipeline concepts to Kafka internals:

| Pipeline concept | Kafka implementation |
|---|---|
| `BatchContext.isActive()` | `payload.isRunning() && !payload.isStale()` |
| `BatchContext.keepAlive()` | `payload.heartbeat()` |
| `BatchContext.acknowledge(position)` | `payload.resolveOffset(offset)` |
| `BatchContext.commit()` | `payload.commitOffsetsIfNecessary()` |
| `BatchContext.pause()` | `payload.pause()` (returns resume callback) |
| `BatchContext.highWatermark` | `payload.batch.highWatermark` |
| `PipelineMessage.position` | `message.offset` |
| `PipelineMessage.timestamp` | `message.timestamp` |
| Strategy Map routing | Replaces `createTopicBatchRouter` dispatch |
| `createMessageBatchStrategy()` | Replaces `createKafkaBatchLifecycle` + `processKafkaBatchMessages` + `runKafkaMessageBatch` |

#### Per-service mapping

**Persister** implements `BatchStrategy` directly. Its 8-step async chain pipeline requires full batch lifecycle control — circuit breaker gating, batch-level persistence, custom offset resolution, commit timing, and consumer lag tracking:

```typescript
// Persister: BatchStrategy for full control
const persisterStrategy: BatchStrategy<PersisterContext> = {
  async processBatch(ctx, batch, messages) {
    // 1. Circuit breaker gate — pause partition if open
    if (ctx.circuitBreaker.isOpen()) {
      const waitMs = ctx.circuitBreaker.timeUntilClose();
      const resume = batch.pause();
      try {
        await waitWithHeartbeats(waitMs, () => batch.keepAlive());
      } finally {
        resume();
      }
      return;
    }

    // 2. Lifecycle gate
    if (!batch.isActive()) return;

    // 3. Collect + deserialize all messages
    const events = collectMessages(ctx, batch, messages);

    // 4. Persist batch to Postgres + Redis (with circuit breaker recording)
    await persistEventsWithCircuitHandling(ctx, events);

    // 5. Acknowledge all positions
    for (const msg of messages) {
      batch.acknowledge(msg.position);
    }

    // 6. Commit + keepAlive
    await batch.commit();
    if (!batch.isActive()) return;
    await batch.keepAlive();

    // 7. Update consumer lag (uses batch.highWatermark)
    await updateLag(ctx, batch);
  },
};
```

**Trends** uses `createMessageBatchStrategy()` with a strategy Map for per-topic routing:

```typescript
// Trends: MessageStrategy per topic, routed via Map
const rawEventStrategy = createMessageBatchStrategy({
  strategy: RAW_EVENT_BATCH_STRATEGY,
  progressInterval: 50,
});

const heartbeatStrategy = createMessageBatchStrategy({
  strategy: COLLECTOR_HEARTBEAT_STRATEGY,
  progressInterval: 50,
});

await consumer.consume({
  topics: [config.KAFKA_TOPIC_RAW_EVENTS, config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT],
  ctx: trendsContext,
  strategy: new Map([
    [config.KAFKA_TOPIC_RAW_EVENTS, rawEventStrategy],
    [config.KAFKA_TOPIC_COLLECTOR_HEARTBEAT, heartbeatStrategy],
  ]),
});
```

**Brief** uses `createMessageBatchStrategy()` with `keepAlive()` on `MessageContext` for LLM heartbeating:

```typescript
// Brief: MessageStrategy with keepAlive for long-running LLM calls
const summaryRequestStrategy: MessageStrategy<BriefContext, Buffer> = {
  deserialize: (value) => value,
  async onMessage(ctx, messageContext, messageValue) {
    const request = deserializeSummaryRequest(messageValue);
    // keepAlive available on messageContext — replaces runWithInFlightHeartbeats
    await processSummaryRequest(ctx, request, messageContext.keepAlive);
  },
};

const trendSnapshotStrategy = createMessageBatchStrategy({
  strategy: TREND_SNAPSHOT_STRATEGY,
  progressInterval: 20,
});

const summaryBatchStrategy = createMessageBatchStrategy({
  strategy: summaryRequestStrategy,
  progressInterval: 20,
});

await consumer.consume({
  topics: [config.KAFKA_TOPIC_TREND_SNAPSHOTS, config.KAFKA_TOPIC_SUMMARY_REQUESTS],
  ctx: briefContext,
  strategy: new Map([
    [config.KAFKA_TOPIC_TREND_SNAPSHOTS, trendSnapshotStrategy],
    [config.KAFKA_TOPIC_SUMMARY_REQUESTS, summaryBatchStrategy],
  ]),
});
```

**Collector** (producer-only) uses `createProducerConnection()` and `TopicPublisher` — no consuming API needed.

Batch processing internals (Kafka `eachBatch` handler, `EachBatchPayload` casting, message-to-`PipelineMessage` mapping, strategy Map dispatch with unknown-topic skipping) remain inside the Kafka adapter and are not part of the public API.

## Proposed Structural Changes

### 1) Create `packages/pipeline`

Create `packages/pipeline` (`@rising-intelligence/pipeline`) with data pipeline protocol interfaces backed by a Kafka adapter.

Modules moving from `packages/shared/src/runtime/` into `packages/pipeline/`:

| Module | Responsibility | Pipeline subpath |
|---|---|---|
| `kafka.ts` | Connection, producer/consumer factories | Internal (Kafka adapter, not exported) |
| `kafka-batch.ts` | Batch processing helpers | Internal (Kafka adapter, not exported) |
| `topic-router.ts` | Topic routing operators | Internal (Kafka adapter, not exported) |
| `topic-extraction.ts` | Allowlist loading, topic classification | `@rising-intelligence/pipeline` |
| `raw-event.ts` | Pipeline API schema, serialization/deserialization | `@rising-intelligence/pipeline` |
| `source.ts` | Pipeline source vocabulary (`CanonicalSource`) | `@rising-intelligence/pipeline` |
| `raw-event-enrichment.ts` | URL normalization, language detection, quality metadata | `@rising-intelligence/pipeline/hydration` |

Constants moving from `packages/shared/src/index.ts` inline exports into `packages/pipeline/`:

| Constant | Current location | Pipeline subpath |
|---|---|---|
| `KAFKA_VALUE_SUBJECTS` | `shared/src/index.ts` (inline) | `@rising-intelligence/pipeline` |
| `CONTRACTS_SUBJECT` | `shared/src/index.ts` (inline) | `@rising-intelligence/pipeline` |
| `GRPC_SUBJECT` | `shared/src/index.ts` (inline) | `@rising-intelligence/pipeline` |
| `CONTRACT_REFERENCE_NAME` | `shared/src/index.ts` (inline) | `@rising-intelligence/pipeline` |

Kafka modules (`kafka.ts`, `kafka-batch.ts`, `topic-router.ts`) become internal implementation details behind the pipeline protocol interfaces. Apps interact through `ProducerConnection`, `ConsumerConnection`, `TopicPublisher`, `BatchStrategy`, and `MessageStrategy` (via `createMessageBatchStrategy`).

**Note**: `paths.ts` stays in `packages/shared`. It uses `import.meta.url` to compute `SHARED_PACKAGE_ROOT` and derive proto file paths relative to shared's filesystem location. Moving it would break the path resolution. It is a legitimate platform primitive (file path computation), not a domain concern.

#### Hydration submodule

`raw-event-enrichment.ts` moves into `packages/pipeline` but lives behind a separate internal seam (e.g., `src/hydration/`). It is exported via a dedicated subpath (`@rising-intelligence/pipeline/hydration`). This keeps hydration decoupled from the protocol core so it can be extracted into its own package in the future if multiple hydration strategies emerge.

Hydration depends on `@rising-intelligence/shared/http` for `url.ts` (generic URL utilities). This is a legitimate cross-package dependency — URL parsing is a platform primitive, not a pipeline concern.

#### Transport submodule

Pipeline protocol interfaces and the Kafka adapter live in `src/transport/`. The public API is exported via `@rising-intelligence/pipeline/transport`. The Kafka adapter implements the interfaces but is not directly importable by apps.

### 2) Replace app-level `src/kafka/` directories

Each app currently has `src/kafka/producer.ts` and/or `src/kafka/consumer.ts` files that:
1. import `createKafkaProducerFactory`/`createKafkaConsumerFactory` from shared,
2. import `Producer`, `Consumer`, `Kafka` types from `kafkajs`,
3. export thin wrappers like `createKafkaProducer(logger)` and `publishEvent(producer, topic, key, value, logger)`.

These are replaced by calling the pipeline protocol API directly:

**Before** (`apps/collector/src/kafka/producer.ts`):
```typescript
import type { Kafka, Producer } from "kafkajs";
import { createKafkaProducerFactory, publishKafkaTopicMessage } from "@rising-intelligence/shared";
// ... 65 lines of boilerplate
```

**After** (in `apps/collector/src/runtime-factory.ts` or callsite):
```typescript
import { createProducerConnection } from "@rising-intelligence/pipeline/transport";
// ProducerConnection is opaque — no kafkajs types leak
```

**Before** (`apps/persister/src/process.ts`):
```typescript
import type { EachBatchPayload } from "kafkajs";
import { createKafkaBatchLifecycle, processKafkaBatchMessages, ... } from "@rising-intelligence/shared";
// Manual batch lifecycle: payload.pause(), payload.resolveOffset(), payload.heartbeat()...
// 8-step async chain pipeline operating on EachBatchPayload directly
```

**After** (`apps/persister/src/process.ts`):
```typescript
import type { BatchStrategy, BatchContext, PipelineMessage } from "@rising-intelligence/pipeline/transport";
// Implement BatchStrategy.processBatch() for full control over batch lifecycle
// BatchContext provides pause(), keepAlive(), acknowledge(), commit() — same operations, pipeline vocabulary
```

**Before** (`apps/trends/src/index.ts`):
```typescript
import type { EachBatchPayload } from "kafkajs";
import { createTopicBatchRouter, runKafkaMessageBatch, type BatchTopicHandler } from "@rising-intelligence/shared";
// Manual topic routing + batch lifecycle composition
```

**After** (`apps/trends/src/index.ts`):
```typescript
import { createMessageBatchStrategy, type MessageStrategy } from "@rising-intelligence/pipeline/transport";
// Per-topic MessageStrategy wrapped via createMessageBatchStrategy(), routed via strategy Map
```

Files to delete:

| File | Lines | Replacement |
|---|---|---|
| `apps/collector/src/kafka/producer.ts` | 65 | `createProducerConnection()` in runtime-factory |
| `apps/persister/src/kafka/consumer.ts` | 33 | `createConsumerConnection()` in runtime-factory |
| `apps/trends/src/kafka/producer.ts` | 55 | `createProducerConnection()` in runtime-factory |
| `apps/trends/src/kafka/consumer.ts` | 30 | `createConsumerConnection()` in runtime-factory |
| `apps/brief/src/kafka/producer.ts` | 43 | `createProducerConnection()` in runtime-factory |
| `apps/brief/src/kafka/consumer.ts` | 30 | `createConsumerConnection()` in runtime-factory |

The domain-specific publishing facades (`CollectorPublisher`, `BriefResultPublisher`) stay in their respective apps — they own domain semantics (which topics, which serialization). They change their constructor to accept `ProducerConnection` instead of `Producer`.

Process files change from manual batch lifecycle management (calling `runMessageBatch` with `EachBatchPayload`) to one of two patterns: persister implements `BatchStrategy` directly for full batch lifecycle control (circuit breaker, custom offset/commit/lag); trends and brief use `createMessageBatchStrategy()` wrapping per-message `MessageStrategy` implementations, with per-topic routing via strategy Map on `ConsumerConnection.consume()`.

### 3) Shared package API segmentation

Update `packages/shared/package.json` exports to include explicit subpaths only. The root barrel (`packages/shared/src/index.ts`) is deleted — there is no `.` export. With event/transport modules and domain constants removed, shared contains only platform primitives:

| Subpath | Modules |
|---|---|
| `@rising-intelligence/shared/config` | `config.ts`, `env.ts`, `secrets.ts`, `paths.ts` |
| `@rising-intelligence/shared/lifecycle` | `lifecycle.ts`, `service-bootstrap.ts`, `startup-facade.ts`, `initialization-rollback.ts`, `runtime-resource-facade.ts`, `function-dependency-builder.ts` |
| `@rising-intelligence/shared/logging` | `logger.ts` |
| `@rising-intelligence/shared/http` | `http.ts`, `health.ts`, `url.ts` |
| `@rising-intelligence/shared/resilience` | `circuit-breaker.ts`, `backoff.ts`, `execution-pipeline.ts` |
| `@rising-intelligence/shared/errors` | `errors.ts` |
| `@rising-intelligence/shared/testing` | `testing/*` |

Attempting to import from `@rising-intelligence/shared` (root) is a compile error — Rule D is self-enforcing.

### 4) Remove dead re-export wrappers in services

Several services contain single-line files that re-export shared symbols without adding value:

- `apps/persister/src/enrich.ts` — `export { prepareRawEventForPersistence } from "@rising-intelligence/shared"`
- `apps/persister/src/circuit-breaker.ts` — `export { CircuitBreaker as PostgresCircuitBreaker } from "@rising-intelligence/shared"`

These create indirection without a meaningful seam. Delete them and import directly from the appropriate package subpath at the callsite. Exception: the `PostgresCircuitBreaker` rename adds semantic value (names what the circuit breaker protects) — evaluate whether the alias is worth keeping as a local `type` alias at the import site instead of a dedicated file.

### 5) Enforce boundaries with lint rules

Extend root `eslint.config.mjs` to include:

1. `no-restricted-imports` banning `@rising-intelligence/shared` root barrel in all app/ops-cli code (redundant with compile error after barrel deletion, but documents intent),
2. `no-restricted-imports` banning `apps/*` cross-imports (Rule E),
3. `no-restricted-imports` banning direct `kafkajs` imports in `apps/*` (Rule G),
4. `no-restricted-imports` banning direct `kafkajs` imports in `packages/ops-cli` non-admin code (Rule G with exception),
5. start in warning mode, promote to error after tuning.

### 6) Fix testing entrypoint contract

Add `@rising-intelligence/shared/testing` as an actual subpath export in `package.json`. The 6 files in `packages/shared/testing/` already exist — they just need a published entry point.

### 7) Workspace dependency normalization

Use consistent workspace-local dependency specifiers for internal packages and align package version semantics across workspaces to reduce tooling ambiguity.

### 8) Documentation alignment

Update root docs and relevant specs to reflect:

- shared package boundary rules,
- pipeline package protocol interface,
- subpath export inventories for both packages.

## Migration Plan

All structural changes, module moves, and import migrations land together as a coordinated change. No temporary re-exports or compatibility shims are needed — compatibility shims risk becoming permanent, and a single coordinated migration eliminates that risk.

### Phase 1: Create pipeline package, migrate all imports, delete shared barrel

Deliverables:

1. Rename `packages/shared/src/runtime/pipeline.ts` to `execution-pipeline.ts`. Update all internal references within shared.
   Acceptance: shared builds; no references to old filename remain.

2. Create `packages/pipeline` with `package.json`, `tsconfig.json`, and directory structure (`src/`, `src/hydration/`, `src/transport/`).
   Acceptance: package exists in workspace; `npm install` resolves it.

3. Define pipeline protocol interfaces in `packages/pipeline/src/transport/`: `ProducerConnection`, `ConsumerConnection`, `TopicPublisher`, `KeyedTopicPublisher`, `BatchStrategy`, `BatchContext`, `PipelineMessage`, `MessageStrategy`, `MessageContext`. Implement `createMessageBatchStrategy()` factory that wraps a `MessageStrategy` into a `BatchStrategy` with standard lifecycle (iterate, deserialize, auto-acknowledge, periodic keepAlive, commit).
   Acceptance: interfaces compile; types are exported from `@rising-intelligence/pipeline/transport`; `createMessageBatchStrategy()` produces a working `BatchStrategy`.

4. Move event/transport/topic modules from `packages/shared/src/runtime/` into `packages/pipeline/src/`: `kafka.ts`, `kafka-batch.ts`, `topic-router.ts`, `topic-extraction.ts`, `raw-event.ts`, `source.ts`. Consolidate `apps/trends/src/allowlist.ts` (which duplicates `CompiledAllowlist` and related logic from `topic-extraction.ts`) — delete the duplicate and import from `@rising-intelligence/pipeline` instead.
   Acceptance: modules compile in new location; no duplicate allowlist implementation remains in `apps/trends/`.

5. Move domain constants (`KAFKA_VALUE_SUBJECTS`, `CONTRACTS_SUBJECT`, `GRPC_SUBJECT`, `CONTRACT_REFERENCE_NAME`) from `packages/shared/src/index.ts` into `packages/pipeline/src/`.
   Acceptance: constants importable from `@rising-intelligence/pipeline`.

6. Implement Kafka adapter for pipeline protocol interfaces: wrap `createKafkaProducerFactory`/`createKafkaConsumerFactory` behind `createProducerConnection`/`createConsumerConnection`. Implement `ConsumerConnection.consume()` to map `EachBatchPayload` to `BatchContext` + `PipelineMessage[]`, support strategy Map routing (replaces `createTopicBatchRouter`), and delegate to the provided `BatchStrategy`. The adapter handles `eachBatch` subscription, unknown-topic skipping, and `PipelineMessage` construction.
   Acceptance: Kafka adapter passes unit tests; pipeline protocol interfaces work end-to-end in integration tests; strategy Map routing dispatches correctly per topic.

7. Move `raw-event-enrichment.ts` into `packages/pipeline/src/hydration/`. Export via `@rising-intelligence/pipeline/hydration` subpath.
   Acceptance: hydration module compiles; subpath resolves correctly.

8. Add shared subpath exports to `packages/shared/package.json` per the mapping table.
   Acceptance: `@rising-intelligence/shared/config`, `@rising-intelligence/shared/lifecycle`, etc. resolve correctly.

9. Add `@rising-intelligence/shared/testing` subpath export.
   Acceptance: test files can import from `@rising-intelligence/shared/testing`.

10. Delete app-level `src/kafka/producer.ts` and `src/kafka/consumer.ts` across all 4 services. Update runtime-factory files to use `createProducerConnection()`/`createConsumerConnection()` from pipeline.
    Acceptance: no `apps/*/src/kafka/` directories exist; runtime-factory files use pipeline protocol API.

11. Update app publishing facades and process files to use pipeline protocol types. Persister: implement `BatchStrategy` directly — its 8-step async chain pipeline uses `BatchContext` for circuit breaker pausing (`batch.pause()`), batch-level offset acknowledgment, commit timing, and lag tracking. Trends/Brief: wrap per-topic `MessageStrategy` implementations in `createMessageBatchStrategy()`, pass as strategy Map to `ConsumerConnection.consume()` for topic routing. Brief: use `MessageContext.keepAlive()` for LLM heartbeating. All apps: `ProducerConnection` replaces `Producer` in publishing facades. Additionally, update `apps/trends/src/snapshot.ts` to replace direct `kafkajs` imports with pipeline protocol types.
    Acceptance: zero `kafkajs` imports in `apps/*`; persister uses `BatchStrategy`; trends and brief use `createMessageBatchStrategy()` with strategy Map routing.

12. Migrate all import sites — app source, test files, e2e tests, and ops-cli — to import event/topic/transport symbols from `@rising-intelligence/pipeline` subpaths instead of `@rising-intelligence/shared`. ops-cli pipeline commands (`commands/brief/trigger.ts`, `commands/brief/result-waiter.ts`, `commands/brief/diagnose.ts`) must migrate from direct `kafkajs` imports to pipeline protocol interfaces.
    Acceptance: no event/transport imports from shared remain in any file; no direct `kafkajs` imports in ops-cli outside of `commands/kafka/`.

13. Migrate all import sites — app source, test files, e2e tests, and ops-cli — to import platform symbols from shared subpaths instead of the root barrel.
    Acceptance: no root-barrel imports remain in any file.

14. Delete `packages/shared/src/index.ts` and remove the `.` export from `packages/shared/package.json`.
    Acceptance: `@rising-intelligence/shared` root import is a compile error.

15. Delete dead re-export wrappers in services (evaluate case by case).
    Acceptance: no single-line re-export files remain unless they add semantic value.

Acceptance (phase-level):

- `packages/pipeline` builds and its exports resolve correctly,
- pipeline protocol interfaces compile and wrap Kafka implementation without behavior change,
- all apps, ops-cli, tests, and e2e tests compile with updated imports,
- zero `kafkajs` imports in `apps/*`,
- zero `apps/*/src/kafka/` directories,
- no root-barrel imports remain,
- no runtime behavior change,
- `npm run build && npm run test` passes across all workspaces.

### Phase 2: Lint enforcement (warning mode)

Deliverables:

1. Add `no-restricted-imports` lint rules in warning mode (root barrels, cross-app, direct kafkajs in apps).

Acceptance:

- `npm run lint` produces warnings for any remaining violations but does not fail builds.

### Phase 3: Lint enforcement (error mode)

Deliverables:

1. Turn lint rules from warn to error for changed files.
2. Clean up any remaining violations opportunistically during feature work.

Acceptance:

- boundary violations fail lint for touched files,
- no new root-barrel usage introduced in changed files,
- no direct `kafkajs` imports in app code.

## Option Analysis

### Option 1: Keep current structure, add docs only

Pros:

- zero migration cost.

Cons:

- no enforcement,
- drift continues,
- Kafka coupling remains hidden in 18 app files,
- high medium-term refactor risk.

Verdict: Rejected.

### Option 2: Keep shared broad, enforce only import boundaries

Pros:

- less initial extraction work.

Cons:

- shared still mixes platform and domain concerns,
- no transport abstraction seam,
- policy disputes continue at placement time.

Verdict: Rejected.

### Option 3: Segment shared + relocate domain into owning services + enforce boundaries

Pros:

- no new package overhead.

Cons:

- `topic-extraction.ts` has multiple consumers (collector and ops-cli) — relocating to a single app forces the other consumer into a Rule E violation or code duplication,
- no transport abstraction seam.

Verdict: Rejected — multi-consumer domain logic cannot live in a single app.

### Option 4: Segment shared + create pipeline package + protocol interfaces + enforce boundaries (selected)

Pros:

- strongest architectural clarity: platform primitives in shared, data pipeline protocol in pipeline,
- multi-consumer domain logic (topic extraction, event types) has a proper home,
- "pipeline" honestly names the package contents (event types + classification + hydration + transport),
- two-tier consuming API (`BatchStrategy` for full control, `MessageStrategy` for convenience) accommodates real service complexity: persister's circuit breaker + batch persistence needs `BatchStrategy`; trends/brief's per-message processing uses `createMessageBatchStrategy()`,
- data pipeline protocol interfaces create a clean seam for future transport substitution without touching app code,
- deleting app-level kafka/ directories eliminates 6 boilerplate files and all direct kafkajs imports from apps,
- apps and ops-cli are first-class consumers of the same pipeline protocol interface,
- coordinated migration without temporary compatibility shims,
- improves multi-agent consistency.

Cons:

- new package overhead (build ordering, workspace config),
- requires upfront protocol interface design discipline,
- moderate migration effort, larger than a pure module relocation (protocol interfaces + app kafka/ removal).

Verdict: Selected — overhead is justified by multi-consumer need, pipeline abstraction value, and app-level boilerplate elimination.

## Assumptions

### Facts

1. Repository uses npm workspaces and TypeScript.
2. `packages/shared` is imported broadly across services (64 import sites across 4 apps and ops-cli).
3. 18 files in `apps/` import directly from `kafkajs`.
4. Lint boundary enforcement is currently minimal (empty `rules: {}` in ESLint config).
5. `topic-extraction.ts` is consumed by collector and ops-cli.
6. `raw-event-enrichment.ts` is consumed by persister and ops-cli.
7. ops-cli is a first-class system participant, not an auxiliary tool.
8. Each app has near-identical `src/kafka/producer.ts` and `src/kafka/consumer.ts` boilerplate.
9. `paths.ts` uses `import.meta.url` and is physically tied to shared's filesystem location.
10. `packages/shared/src/index.ts` exports domain constants inline (not from runtime/ files).
11. `packages/shared/src/runtime/pipeline.ts` shares a name with the new `packages/pipeline` and must be renamed to `execution-pipeline.ts` to avoid confusion.
12. Persister's batch processing requires full lifecycle control (circuit breaker partition pausing, batch-level persistence, custom offset/commit/lag orchestration) — a per-message `MessageStrategy` cannot express this; `BatchStrategy` is required.
13. Trends and Brief both use multi-topic routing (`createTopicBatchRouter`) with different handlers per topic — strategy Map routing is required.
14. Brief's LLM generation requires in-flight heartbeating during long-running `onMessage` calls — `MessageContext.keepAlive()` is required.

### Assumptions to validate

1. Most shared root imports can migrate to subpaths without major churn.
2. Pipeline protocol interfaces can wrap existing Kafka factories without changing runtime behavior.
3. App-level kafka/ directories can be deleted without losing app-specific transport customization (validated: they are thin wrappers with no custom logic beyond topic names and log messages).
4. Test files and e2e tests can be migrated alongside app source without introducing test-only compatibility paths.
5. `BatchContext` surface (`isActive`, `keepAlive`, `acknowledge`, `commit`, `pause`, `highWatermark`) covers all `EachBatchPayload` operations currently used by persister — no additional Kafka-specific operations are needed (validated: persister uses `isRunning`, `isStale`, `heartbeat`, `resolveOffset`, `commitOffsetsIfNecessary`, `pause`, `batch.highWatermark`, all of which map to `BatchContext`).

Validation checkpoints:

- after Phase 1: pipeline package builds with protocol interfaces, all workspaces compile, all tests pass,
- after Phase 2: lint warnings active,
- after Phase 3: lint errors active on touched files.

## Risks and Mitigations

### Risk 1: Migration churn slows feature delivery

Mitigation:

- coordinated migration lands all changes together, avoiding extended compatibility windows,
- phase gating with measurable exit criteria,
- lint enforcement deferred to Phase 2/3 to separate structural changes from tooling changes.

### Risk 2: Over-engineering the pipeline protocol interface

This risk is higher now that the scope includes both pipeline protocol interfaces and app kafka/ deletion. The temptation is to design a fully abstract transport layer.

Mitigation:

- the two-tier strategy design (`BatchStrategy` + `MessageStrategy`) is grounded in actual service needs, not speculative abstraction — persister needs batch-level control, trends/brief need per-message convenience,
- `BatchContext` exposes exactly the operations apps already use (`pause`, `keepAlive`, `acknowledge`, `commit`, `isActive`, `highWatermark`) — it is a vocabulary change on the existing `EachBatchPayload` surface, not new capability,
- `createMessageBatchStrategy()` replaces a three-function composition (`createKafkaBatchLifecycle` + `processKafkaBatchMessages` + `runKafkaMessageBatch`) with a single factory — a simplification, not added complexity,
- `MessageStrategy` is structurally identical to the existing `KafkaBatchMessageStrategy` with one addition (`keepAlive()` on `MessageContext`),
- strategy Map routing replaces `createTopicBatchRouter` — same dispatch semantics, fewer moving parts,
- resist adding methods or abstractions that aren't needed by current consumers,
- the goal is a pipeline seam, not a framework.

### Risk 3: Boundary rules become noisy/overly strict

Mitigation:

- start warning-only,
- tune false positives before error mode,
- support temporary, time-boxed waivers.

### Risk 4: Hydration coupling makes future extraction difficult

Mitigation:

- hydration lives behind its own subpath (`@rising-intelligence/pipeline/hydration`) from day one,
- hydration depends on shared for generic utilities (URL parsing), not on pipeline internals,
- extraction into `packages/hydration` later is a clean cut along the subpath boundary.

### Risk 5: App-level kafka/ removal breaks test mocking patterns

App tests may mock `createKafkaProducer` or `createKafkaConsumer` from the local kafka/ files. Deleting these files requires updating test mocking to use the pipeline protocol interfaces.

Mitigation:

- runtime-factory dependency injection pattern already supports this — tests override the dependency function, which now returns `ProducerConnection`/`ConsumerConnection` instead of `KafkaProducerContext`/`KafkaConsumerContext`,
- the change is mechanical: update type signatures in test overrides,
- validate by running full test suite after each app migration.

### Risk 6: BatchStrategy gives full power without guardrails

`BatchStrategy` gives the implementing service complete control over acknowledgment, commit, and partition lifecycle. A bug in a `BatchStrategy` implementation (e.g., forgetting to call `commit()`, or acknowledging positions out of order) would cause silent data loss or reprocessing.

Mitigation:

- only persister uses `BatchStrategy` directly — the other three services use `createMessageBatchStrategy()` which handles lifecycle correctly by construction,
- persister's existing `process.ts` already manages these operations manually via `EachBatchPayload` — the migration does not introduce new risk, it preserves existing responsibility,
- integration tests validate end-to-end message flow (publish → consume → persist → verify) and will catch commit/acknowledge regressions,
- `createMessageBatchStrategy()` is the default recommendation — `BatchStrategy` is explicitly documented as the escape hatch for services that need it.

### Risk 7: Test and e2e file migration introduces unexpected breakage

Test files and e2e tests import from shared and kafkajs. Migrating these alongside app source increases the blast radius of Phase 1.

Mitigation:

- test files follow the same import patterns as app source — the migration is mechanical,
- full test suite is the primary validation gate (`npm run build && npm run test`),
- e2e tests that use kafkajs for test infrastructure setup (not pipeline operations) may retain direct imports with explicit justification.

## Kill Criteria / Reversal Trigger

Re-evaluate or pause rollout if:

1. migration feels like it is blocking feature work for more than a few sessions,
2. lint rules produce persistent false positives after a tuning window,
3. extraction introduces runtime regressions,
4. protocol interface design stalls Phase 1 delivery.

If triggered:

- freeze new lint rules,
- keep already completed structural improvements (subpath exports, testing fix),
- reassess scope before continuing.

## Success Indicators

Measurable signals that the migration is working:

1. **Root-barrel import count reaches zero** — no imports from `@rising-intelligence/shared` root in any file. Root import is a compile error after barrel deletion.
2. **Zero domain/transport modules in `packages/shared/src/runtime/`** — event types, Kafka transport, topic classification all live in `packages/pipeline`.
3. **Zero direct `kafkajs` imports in app code** — apps use the pipeline protocol interfaces exclusively.
4. **Zero app-level `src/kafka/` directories** — producer/consumer boilerplate replaced by pipeline protocol API.
5. **Lint rules catch violations** — new code cannot introduce cross-app or direct transport imports.

## Verification Plan

Run after each phase:

```bash
npm run build
npm run test
npm run lint
```

Additional checks:

1. Dependency-boundary report command (to be added) shows zero hard violations.
2. `grep -r 'from "kafkajs"' apps/` returns zero results after Phase 1.
3. `grep -r 'from "@rising-intelligence/shared"' .` returns zero results after Phase 1 (excluding `packages/shared/` internal imports).
4. No `apps/*/src/kafka/` directories exist after Phase 1.
5. Package export checks confirm documented subpaths resolve for both shared and pipeline.
6. `grep -r 'from "@rising-intelligence/shared"' apps/*/tests/ tests/` returns zero results after Phase 1 (test files migrated).

## Implementation Tasks (Ordered)

### Phase 1 Tasks

1. Rename `packages/shared/src/runtime/pipeline.ts` to `execution-pipeline.ts`. Update all internal references within shared.
   Acceptance: shared builds; no references to old filename remain.

2. Create `packages/pipeline` with `package.json`, `tsconfig.json`, and directory structure (`src/`, `src/hydration/`, `src/transport/`).
   Acceptance: package exists in workspace; `npm install` resolves it.

3. Define pipeline protocol interfaces in `packages/pipeline/src/transport/`: `ProducerConnection`, `ConsumerConnection`, `TopicPublisher`, `KeyedTopicPublisher`, `BatchStrategy`, `BatchContext`, `PipelineMessage`, `MessageStrategy`, `MessageContext`. Implement `createMessageBatchStrategy()` factory that wraps a `MessageStrategy` into a `BatchStrategy` with standard lifecycle (iterate, deserialize, auto-acknowledge, periodic keepAlive, commit).
   Acceptance: interfaces compile; types are exported from `@rising-intelligence/pipeline/transport`; `createMessageBatchStrategy()` produces a working `BatchStrategy`.

4. Move event/transport/topic modules from `packages/shared/src/runtime/` into `packages/pipeline/src/`: `kafka.ts`, `kafka-batch.ts`, `topic-router.ts`, `topic-extraction.ts`, `raw-event.ts`, `source.ts`. Consolidate `apps/trends/src/allowlist.ts` (which duplicates `CompiledAllowlist` and related logic from `topic-extraction.ts`) — delete the duplicate and import from `@rising-intelligence/pipeline` instead.
   Acceptance: modules compile in new location; no duplicate allowlist implementation remains in `apps/trends/`.

5. Move domain constants (`KAFKA_VALUE_SUBJECTS`, `CONTRACTS_SUBJECT`, `GRPC_SUBJECT`, `CONTRACT_REFERENCE_NAME`) from `packages/shared/src/index.ts` into `packages/pipeline/src/`.
   Acceptance: constants importable from `@rising-intelligence/pipeline`.

6. Implement Kafka adapter for pipeline protocol interfaces: wrap `createKafkaProducerFactory`/`createKafkaConsumerFactory` behind `createProducerConnection`/`createConsumerConnection`. Implement `ConsumerConnection.consume()` to map `EachBatchPayload` to `BatchContext` + `PipelineMessage[]`, support strategy Map routing (replaces `createTopicBatchRouter`), and delegate to the provided `BatchStrategy`. The adapter handles `eachBatch` subscription, unknown-topic skipping, and `PipelineMessage` construction.
   Acceptance: Kafka adapter passes unit tests; pipeline protocol interfaces work end-to-end in integration tests; strategy Map routing dispatches correctly per topic.

7. Move `raw-event-enrichment.ts` into `packages/pipeline/src/hydration/`. Export via `@rising-intelligence/pipeline/hydration` subpath.
   Acceptance: hydration module compiles; subpath resolves correctly.

8. Add shared subpath exports to `packages/shared/package.json` per the mapping table.
   Acceptance: `@rising-intelligence/shared/config`, `@rising-intelligence/shared/lifecycle`, etc. resolve correctly.

9. Add `@rising-intelligence/shared/testing` subpath export.
   Acceptance: test files can import from `@rising-intelligence/shared/testing`.

10. Delete app-level `src/kafka/producer.ts` and `src/kafka/consumer.ts` across all 4 services. Update runtime-factory files to use `createProducerConnection()`/`createConsumerConnection()` from pipeline.
    Acceptance: no `apps/*/src/kafka/` directories exist; runtime-factory files use pipeline protocol API.

11. Update app publishing facades and process files to use pipeline protocol types. Persister: implement `BatchStrategy` directly — its 8-step async chain pipeline uses `BatchContext` for circuit breaker pausing (`batch.pause()`), batch-level offset acknowledgment, commit timing, and lag tracking. Trends/Brief: wrap per-topic `MessageStrategy` implementations in `createMessageBatchStrategy()`, pass as strategy Map to `ConsumerConnection.consume()` for topic routing. Brief: use `MessageContext.keepAlive()` for LLM heartbeating. All apps: `ProducerConnection` replaces `Producer` in publishing facades. Additionally, update `apps/trends/src/snapshot.ts` to replace direct `kafkajs` imports with pipeline protocol types.
    Acceptance: zero `kafkajs` imports in `apps/*`; persister uses `BatchStrategy`; trends and brief use `createMessageBatchStrategy()` with strategy Map routing.

12. Migrate all import sites — app source, test files, e2e tests, and ops-cli — to import event/topic/transport symbols from `@rising-intelligence/pipeline` subpaths instead of `@rising-intelligence/shared`. ops-cli pipeline commands (`commands/brief/trigger.ts`, `commands/brief/result-waiter.ts`, `commands/brief/diagnose.ts`) must migrate from direct `kafkajs` imports to pipeline protocol interfaces.
    Acceptance: no event/transport imports from shared remain in any file; no direct `kafkajs` imports in ops-cli outside of `commands/kafka/`.

13. Migrate all import sites — app source, test files, e2e tests, and ops-cli — to import platform symbols from shared subpaths instead of the root barrel.
    Acceptance: no root-barrel imports remain in any file.

14. Delete `packages/shared/src/index.ts` and remove the `.` export from `packages/shared/package.json`.
    Acceptance: `@rising-intelligence/shared` root import is a compile error.

15. Delete dead re-export wrappers in services (evaluate case by case).
    Acceptance: no single-line re-export files remain unless they add semantic value.

### Phase 2 Tasks

16. Add `no-restricted-imports` lint rules in warning mode (root barrels, cross-app, direct kafkajs in apps).
    Acceptance: `npm run lint` produces warnings for any remaining violations but does not fail builds.

### Phase 3 Tasks

17. Turn lint rules from warn to error for changed files.
    Acceptance: lint fails on new root-barrel, cross-app, or direct transport imports.

## Related Specs

- `specs/001-real-time-personal-intelligence-system.md`
- `specs/003-contracts-and-schema-registry.md`
- `specs/009-testing-strategy.md`
- `specs/010-pos-intelligence-source-pack.md`
