import type { EachBatchPayload } from "kafkajs";
import type { Logger } from "pino";

export type BatchTopicHandler = (payload: EachBatchPayload) => Promise<void>;

export interface TopicBatchRouter {
  handle(payload: EachBatchPayload): Promise<void>;
}

export interface CreateTopicBatchRouterInput {
  logger: Logger;
  handlers: ReadonlyMap<string, BatchTopicHandler>;
  onUnknownTopic?: BatchTopicHandler;
}

async function skipUnknownTopicBatch(payload: EachBatchPayload): Promise<void> {
  for (const message of payload.batch.messages) {
    payload.resolveOffset(message.offset);
  }

  await payload.commitOffsetsIfNecessary();
  await payload.heartbeat();
}

/**
 * Facade/Proxy around per-topic batch handlers so unknown topics are handled
 * consistently (warn + commit/heartbeat) instead of stalling a partition.
 */
export function createTopicBatchRouter(
  input: CreateTopicBatchRouterInput
): TopicBatchRouter {
  const onUnknownTopic = input.onUnknownTopic ?? skipUnknownTopicBatch;

  return {
    async handle(payload: EachBatchPayload): Promise<void> {
      const handler = input.handlers.get(payload.batch.topic);
      if (handler) {
        await handler(payload);
        return;
      }

      input.logger.warn(
        { topic: payload.batch.topic, partition: payload.batch.partition },
        "Received batch for unexpected topic; skipping"
      );
      await onUnknownTopic(payload);
    },
  };
}
