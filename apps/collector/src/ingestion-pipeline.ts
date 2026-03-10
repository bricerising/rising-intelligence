import type { Logger } from "pino";
import {
  runAsyncChain,
  type AsyncChainStep,
} from "@rising-intelligence/shared/resilience";
import type { CheckpointStore } from "./checkpoint.js";
import {
  incrementEventsFailed,
  incrementEventsIngested,
  incrementRssFeedError,
  incrementTopicsExtracted,
  type HealthContext,
} from "./health.js";
import { generateDlqId as defaultGenerateDlqId } from "./serializer.js";
import {
  extractTopics as defaultTopicExtractor,
  type CompiledAllowlist,
} from "@rising-intelligence/pipeline";
import type { CollectorIngestionPublisher } from "./publishing-facade.js";
import {
  isRawEvent,
  normalizeCollectorIngestionEvent,
  type CollectorAcceptedEvent,
  type CollectorIngestionEvent,
  type DeadLetterEvent,
  type Source,
} from "./types.js";

/**
 * Collector-internal ingestion job chain.
 * External callers should depend on the collector ingestion boundary.
 */
export type CollectorEventProcessResult =
  | {
    status: "ingested";
    topics: string[];
  }
  | {
    status: "duplicate";
  }
  | {
    status: "invalid";
    errorCode: "VALIDATION_FAILED";
  };

export interface CollectorEventProcessorInput {
  adapterName: string;
  adapterSource: Source;
  allowlist: CompiledAllowlist;
  checkpointStore: Pick<CheckpointStore, "hasSeen" | "markSeen">;
  healthContext: HealthContext;
  logger: Logger;
  publisher: CollectorIngestionPublisher;
  now?: () => Date;
  generateDlqId?: () => string;
  topicExtractor?: (
    event: { title?: string; text: string; url?: string | null },
    allowlist: CompiledAllowlist
  ) => string[];
}

export interface CollectorIngestionJob {
  event: CollectorAcceptedEvent;
}

export function createCollectorIngestionJob(
  event: CollectorAcceptedEvent
): CollectorIngestionJob {
  return { event };
}

export interface CollectorIngestionCommand {
  execute(job: CollectorIngestionJob): Promise<CollectorEventProcessResult>;
}

export interface CollectorEventProcessor extends CollectorIngestionCommand {
  process(event: CollectorAcceptedEvent): Promise<CollectorEventProcessResult>;
}

interface ProcessingState {
  event: CollectorIngestionEvent;
  acceptedEvent: CollectorAcceptedEvent;
  topics: string[];
}

function mergeTags(existingTags: string[] | undefined, extractedTopics: string[]): string[] {
  const merged = [...(existingTags ?? []), ...extractedTopics];
  const deduped: string[] = [];
  const seen = new Set<string>();
  for (const tag of merged) {
    const normalizedTag = tag.trim();
    if (normalizedTag.length === 0 || seen.has(normalizedTag)) {
      continue;
    }
    seen.add(normalizedTag);
    deduped.push(normalizedTag);
  }
  return deduped;
}

interface RuntimeContext {
  adapterName: string;
  adapterSource: Source;
  allowlist: CompiledAllowlist;
  checkpointStore: Pick<CheckpointStore, "hasSeen" | "markSeen">;
  healthContext: HealthContext;
  logger: Logger;
  publisher: CollectorIngestionPublisher;
  now: () => Date;
  generateDlqId: () => string;
  topicExtractor: (
    event: { title?: string; text: string; url?: string | null },
    allowlist: CompiledAllowlist
  ) => string[];
  validationFailureStrategy: ValidationFailureStrategy;
}

interface ProcessingContext {
  runtime: RuntimeContext;
  state: ProcessingState;
}

type ProcessingStep = AsyncChainStep<ProcessingContext, CollectorEventProcessResult>;

interface ValidationFailureStrategyContext {
  runtime: RuntimeContext;
  event: CollectorIngestionEvent;
}

interface ValidationFailureStrategy {
  readonly name: string;
  handle(input: ValidationFailureStrategyContext): void;
}

function isNonBlank(value: string): boolean {
  return value.trim().length > 0;
}

function hasRequiredFields(event: CollectorIngestionEvent): boolean {
  return isNonBlank(event.eventId) && isNonBlank(event.text);
}

interface RssFeedMetadata {
  feed: string;
  feedUrl: string;
}

function parseRssFeedMetadata(
  sourceMeta: CollectorIngestionEvent["sourceMeta"]
): RssFeedMetadata | null {
  if (!sourceMeta || typeof sourceMeta !== "object") {
    return null;
  }

  const feed = sourceMeta.feed_name;
  const feedUrl = sourceMeta.feed_url;
  if (typeof feed !== "string" || feed.trim() === "") {
    return null;
  }
  if (typeof feedUrl !== "string" || feedUrl.trim() === "") {
    return null;
  }

  return {
    feed,
    feedUrl,
  };
}

const RECORD_PARSE_ERROR_STRATEGY: ValidationFailureStrategy = {
  name: "record-parse-error",
  handle({ runtime }): void {
    incrementEventsFailed(runtime.healthContext, runtime.adapterSource, "parse_error");
  },
};

const RECORD_RSS_FEED_ERROR_STRATEGY: ValidationFailureStrategy = {
  name: "record-rss-feed-error",
  handle({ runtime, event }): void {
    const feedMetadata = parseRssFeedMetadata(event.sourceMeta);
    if (!feedMetadata) {
      return;
    }

    incrementRssFeedError(
      runtime.healthContext,
      {
        feed: feedMetadata.feed,
        feedUrl: feedMetadata.feedUrl,
        errorType: "parse_error",
      }
    );
  },
};

const SOURCE_VALIDATION_FAILURE_STRATEGIES: Readonly<
  Partial<Record<Source, readonly ValidationFailureStrategy[]>>
> = {
  rss: [RECORD_RSS_FEED_ERROR_STRATEGY],
};

function createValidationFailureStrategy(adapterSource: Source): ValidationFailureStrategy {
  const sourceStrategies = SOURCE_VALIDATION_FAILURE_STRATEGIES[adapterSource] ?? [];
  const strategies = [RECORD_PARSE_ERROR_STRATEGY, ...sourceStrategies];

  return {
    name: `validation-failure:${adapterSource}`,
    handle(input): void {
      for (const strategy of strategies) {
        strategy.handle(input);
      }
    },
  };
}

function createDeduplicateStep(): ProcessingStep {
  return {
    name: "deduplicate",
    async execute({ runtime, state }, next): Promise<CollectorEventProcessResult> {
      if (runtime.checkpointStore.hasSeen(runtime.adapterSource, state.event.eventId)) {
        runtime.logger.debug(
          { eventId: state.event.eventId },
          "Duplicate event skipped"
        );
        return { status: "duplicate" };
      }

      return next();
    },
  };
}

function createValidationStep(): ProcessingStep {
  return {
    name: "validate",
    async execute({ runtime, state }, next): Promise<CollectorEventProcessResult> {
      if (hasRequiredFields(state.event)) {
        return next();
      }

      const dlqEvent: DeadLetterEvent = {
        dlq_id: runtime.generateDlqId(),
        occurred_at: runtime.now().toISOString(),
        source: runtime.adapterName,
        error_code: "VALIDATION_FAILED",
        error_message: "Missing required fields: event_id or text",
        raw_reference: state.event.url ?? state.event.eventId,
      };

      await runtime.publisher.publishRejectedEvent(dlqEvent);
      runtime.validationFailureStrategy.handle({
        runtime,
        event: state.event,
      });

      return {
        status: "invalid",
        errorCode: "VALIDATION_FAILED",
      };
    },
  };
}

function createTopicExtractionStep(): ProcessingStep {
  return {
    name: "extract-topics",
    async execute({ runtime, state }, next): Promise<CollectorEventProcessResult> {
      const topics = runtime.topicExtractor(
        { title: state.event.title, text: state.event.text, url: state.event.url },
        runtime.allowlist
      );
      state.topics = topics;
      state.event.tags = mergeTags(state.event.tags, topics);
      if (isRawEvent(state.acceptedEvent)) {
        state.acceptedEvent.tags = state.event.tags;
      }

      for (const topic of topics) {
        incrementTopicsExtracted(runtime.healthContext, topic);
      }

      return next();
    },
  };
}

function createPublishStep(): ProcessingStep {
  return {
    name: "publish",
    async execute({ runtime, state }): Promise<CollectorEventProcessResult> {
      await runtime.publisher.publishAcceptedEvent(state.acceptedEvent);
      runtime.checkpointStore.markSeen(runtime.adapterSource, state.event.eventId);
      incrementEventsIngested(runtime.healthContext, runtime.adapterSource);
      runtime.healthContext.lastEventAt = runtime.now();

      return {
        status: "ingested",
        topics: state.topics,
      };
    },
  };
}

export function createCollectorEventProcessor(
  input: CollectorEventProcessorInput
): CollectorEventProcessor {
  const runtime: RuntimeContext = {
    ...input,
    now: input.now ?? (() => new Date()),
    generateDlqId: input.generateDlqId ?? defaultGenerateDlqId,
    topicExtractor: input.topicExtractor ?? defaultTopicExtractor,
    validationFailureStrategy: createValidationFailureStrategy(input.adapterSource),
  };

  const steps: ReadonlyArray<ProcessingStep> = [
    createDeduplicateStep(),
    createValidationStep(),
    createTopicExtractionStep(),
    createPublishStep(),
  ];

  const execute = async (
    job: CollectorIngestionJob
  ): Promise<CollectorEventProcessResult> => {
    const event = job.event;
    const normalizedEvent = normalizeCollectorIngestionEvent(event);
    const acceptedEvent = isRawEvent(event)
      ? event
      : normalizedEvent;

    return runAsyncChain(
      steps,
      {
        runtime,
        state: {
          event: normalizedEvent,
          acceptedEvent,
          topics: [],
        },
      },
      {
        onEnd() {
          throw new Error("Collector event pipeline terminated unexpectedly");
        },
        duplicateNextError(stepName) {
          return new Error(
            `Collector event pipeline step "${stepName}" called next() multiple times`
          );
        },
      }
    );
  };

  return {
    execute,
    async process(event: CollectorAcceptedEvent): Promise<CollectorEventProcessResult> {
      return execute(createCollectorIngestionJob(event));
    },
  };
}
