import type { Logger } from "pino";
import type { CheckpointStore } from "./checkpoint.js";
import {
  incrementEventsFailed,
  incrementEventsIngested,
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
    event: { title?: string; text: string },
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
    event: { title?: string; text: string },
    allowlist: CompiledAllowlist
  ) => string[];
}

interface ProcessingContext {
  runtime: RuntimeContext;
  state: ProcessingState;
}

interface ProcessingStep {
  readonly name: string;
  execute(
    ctx: ProcessingContext,
    next: () => Promise<CollectorEventProcessResult>
  ): Promise<CollectorEventProcessResult>;
}

type CollectorNext = () => Promise<CollectorEventProcessResult>;

function isNonBlank(value: string): boolean {
  return value.trim().length > 0;
}

function hasRequiredFields(event: RawEvent): boolean {
  return isNonBlank(event.event_id) && isNonBlank(event.text);
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
        { title: state.event.title, text: state.event.text },
        runtime.allowlist
      );
      state.topics = topics;
      state.event.tags = topics;

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

function runChain(
  steps: readonly ProcessingStep[],
  ctx: ProcessingContext
): Promise<CollectorEventProcessResult> {
  const dispatch = async (index: number): Promise<CollectorEventProcessResult> => {
    const step = steps[index];
    if (!step) {
      throw new Error("Collector event pipeline terminated unexpectedly");
    }

    let nextCalled = false;
    const next: CollectorNext = async () => {
      if (nextCalled) {
        throw new Error(
          `Collector event pipeline step "${step.name}" called next() multiple times`
        );
      }
      nextCalled = true;
      return dispatch(index + 1);
    };

    return step.execute(ctx, next);
  };

  return dispatch(0);
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
      return runChain(steps, {
        runtime,
        state: { event, topics: [] },
      });
    },
  };
}
