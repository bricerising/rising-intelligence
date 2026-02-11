import type { Producer } from "kafkajs";
import type { Logger } from "pino";
import { publishSnapshot as publishKafkaSnapshot } from "./kafka/producer.js";

type PublishSnapshotFn = typeof publishKafkaSnapshot;

export interface TrendsSnapshotPublisher {
  publishSnapshot(snapshotKey: string, payload: Record<string, unknown>): Promise<void>;
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
export function createTrendsSnapshotPublisher(
  input: CreateTrendsSnapshotPublisherInput
): TrendsSnapshotPublisher {
  const publish = input.publish ?? publishKafkaSnapshot;
  const { producer, logger, topic } = input;

  return {
    async publishSnapshot(snapshotKey, payload): Promise<void> {
      await publish(
        producer,
        topic,
        snapshotKey,
        Buffer.from(JSON.stringify(payload), "utf-8"),
        logger
      );
    },
  };
}
