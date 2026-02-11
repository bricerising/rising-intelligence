import type { Producer } from "kafkajs";
import type { Logger } from "pino";
import { createKafkaTopicPublisher } from "@rising-intelligence/shared";
import { publishBriefResult } from "./kafka/producer.js";

type PublishBriefResultFn = typeof publishBriefResult;

export interface BriefResultPublisher<
  TPayload extends Record<string, unknown> = Record<string, unknown>
> {
  publishResult(requestId: string, payload: TPayload): Promise<void>;
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
export function createBriefResultPublisher<
  TPayload extends Record<string, unknown> = Record<string, unknown>
>(
  input: CreateBriefResultPublisherInput
): BriefResultPublisher<TPayload> {
  const publish = input.publish ?? publishBriefResult;
  const { producer, logger, topic } = input;
  const topicPublisher = createKafkaTopicPublisher<TPayload, Logger>({
    producer,
    logger,
    topic,
    publish,
  });

  return {
    async publishResult(requestId, payload): Promise<void> {
      await topicPublisher.publish(requestId, payload);
    },
  };
}
