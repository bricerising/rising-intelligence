import type { Logger } from "pino";
import {
  createTopicPublisher,
  type ProducerConnection,
} from "@rising-intelligence/pipeline/transport";

export interface TrendsSnapshotPublisher<
  TPayload extends Record<string, unknown> = Record<string, unknown>
> {
  publishSnapshot(snapshotKey: string, payload: TPayload): Promise<void>;
}

export interface CreateTrendsSnapshotPublisherInput {
  connection: ProducerConnection;
  logger: Logger;
  topic: string;
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
  const { connection, topic } = input;
  const topicPublisher = createTopicPublisher<TPayload>({
    connection,
    topic,
  });

  return {
    async publishSnapshot(snapshotKey, payload): Promise<void> {
      await topicPublisher.publish(snapshotKey, payload);
    },
  };
}
