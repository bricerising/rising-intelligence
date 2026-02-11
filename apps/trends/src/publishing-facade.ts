import type { Producer } from "kafkajs";
import type { Logger } from "pino";
import { createKafkaTopicPublisher } from "@rising-intelligence/shared";
import { publishSnapshot as publishKafkaSnapshot } from "./kafka/producer.js";

type PublishSnapshotFn = typeof publishKafkaSnapshot;

export interface TrendsSnapshotPublisher<
  TPayload extends Record<string, unknown> = Record<string, unknown>
> {
  publishSnapshot(snapshotKey: string, payload: TPayload): Promise<void>;
}

export interface CreateTrendsSnapshotPublisherInput {
  producer: Producer;
  logger: Logger;
  topic: string;
  publish?: PublishSnapshotFn;
}

/**
 * Facade that encapsulates trends snapshot serialization and Kafka routing.
 * Callers provide a typed payload and snapshot key only.
 */
export function createTrendsSnapshotPublisher<
  TPayload extends Record<string, unknown> = Record<string, unknown>
>(
  input: CreateTrendsSnapshotPublisherInput
): TrendsSnapshotPublisher<TPayload> {
  const publish = input.publish ?? publishKafkaSnapshot;
  const { producer, logger, topic } = input;
  const topicPublisher = createKafkaTopicPublisher<TPayload, Logger>({
    producer,
    logger,
    topic,
    publish,
  });

  return {
    async publishSnapshot(snapshotKey, payload): Promise<void> {
      await topicPublisher.publish(snapshotKey, payload);
    },
  };
}
