import type { Logger } from "pino";
import { runAsyncChain, type AsyncChainStep } from "@rising-intelligence/shared";
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
} from "./topics/extractor.js";
import type { DeadLetterEvent, RawEvent, Source } from "./types.js";

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
  publishRawEvent(event: RawEvent): Promise<void>;
  publishDeadLetterEvent(event: DeadLetterEvent): Promise<void>;
  now?: () => Date;
  generateDlqId?: () => string;
  topicExtractor?: (
    event: { title?: string; text: string; url?: string | null },
    allowlist: CompiledAllowlist
  ) => string[];
}

export interface CollectorEventProcessor {
  process(event: RawEvent): Promise<CollectorEventProcessResult>;
}

interface ProcessingState {
  event: RawEvent;
  topics: string[];
}

function mergeTags(existingTags: string[] | undefined, extractedTopics: string[]): string[] {
  const merged = [...(existingTags ?? []), ...extractedTopics];
  const deduped: string[] = [];
  const seen = new Set<string>();
  for (const tag of merged) {
    if (!tag || seen.has(tag)) {
      continue;
    }
    seen.add(tag);
    deduped.push(tag);
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
  publishRawEvent(event: RawEvent): Promise<void>;
  publishDeadLetterEvent(event: DeadLetterEvent): Promise<void>;
  now: () => Date;
  generateDlqId: () => string;
  topicExtractor: (
    event: { title?: string; text: string; url?: string | null },
    allowlist: CompiledAllowlist
  ) => string[];
}

interface ProcessingContext {
  runtime: RuntimeContext;
  state: ProcessingState;
}

type ProcessingStep = AsyncChainStep<ProcessingContext, CollectorEventProcessResult>;

function isNonBlank(value: string): boolean {
  return value.trim().length > 0;
}

function hasRequiredFields(event: RawEvent): boolean {
  return isNonBlank(event.event_id) && isNonBlank(event.text);
}

interface RssFeedMetadata {
  feed: string;
  feedUrl: string;
}

function parseRssFeedMetadata(sourceMeta: RawEvent["source_meta"]): RssFeedMetadata | null {
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

function createDeduplicateStep(): ProcessingStep {
  return {
    name: "deduplicate",
    async execute({ runtime, state }, next): Promise<CollectorEventProcessResult> {
      if (runtime.checkpointStore.hasSeen(runtime.adapterSource, state.event.event_id)) {
        runtime.logger.debug(
          { eventId: state.event.event_id },
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
        raw_reference: state.event.url ?? state.event.event_id,
      };

      await runtime.publishDeadLetterEvent(dlqEvent);
      incrementEventsFailed(runtime.healthContext, runtime.adapterSource, "parse_error");
      if (runtime.adapterSource === "rss") {
        const feedMetadata = parseRssFeedMetadata(state.event.source_meta);
        if (feedMetadata) {
          incrementRssFeedError(
            runtime.healthContext,
            {
              feed: feedMetadata.feed,
              feedUrl: feedMetadata.feedUrl,
              errorType: "parse_error",
            }
          );
        }
      }

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
      await runtime.publishRawEvent(state.event);
      runtime.checkpointStore.markSeen(runtime.adapterSource, state.event.event_id);
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
  };

  const steps: ReadonlyArray<ProcessingStep> = [
    createDeduplicateStep(),
    createValidationStep(),
    createTopicExtractionStep(),
    createPublishStep(),
  ];

  return {
    async process(event: RawEvent): Promise<CollectorEventProcessResult> {
      return runAsyncChain(
        steps,
        {
          runtime,
          state: { event, topics: [] },
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
    },
  };
}
