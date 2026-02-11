import type { Producer } from "kafkajs";
import type { Logger } from "pino";
import { publishBriefResult } from "./kafka/producer.js";

type PublishBriefResultFn = typeof publishBriefResult;

export interface BriefResultPublisher {
  publishResult(requestId: string, payload: Record<string, unknown>): Promise<void>;
}

export interface CreateBriefResultPublisherInput {
  producer: Producer;
  logger: Logger;
  topic: string;
  publish?: PublishBriefResultFn;
}

/**
 * Facade that encapsulates brief result serialization and topic routing.
 * Callers publish typed result payloads without repeating Kafka details.
 */
export function createBriefResultPublisher(
  input: CreateBriefResultPublisherInput
): BriefResultPublisher {
  const publish = input.publish ?? publishBriefResult;
  const { producer, logger, topic } = input;

  return {
    async publishResult(requestId, payload): Promise<void> {
      await publish(
        producer,
        topic,
        requestId,
        Buffer.from(JSON.stringify(payload), "utf-8"),
        logger
      );
    },
  };
}
