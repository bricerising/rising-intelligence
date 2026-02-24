import type { Logger } from "pino";
import {
  createTopicPublisher,
  type ProducerConnection,
} from "@rising-intelligence/pipeline/transport";

export interface BriefResultPublisher<
  TPayload extends Record<string, unknown> = Record<string, unknown>
> {
  publishResult(requestId: string, payload: TPayload): Promise<void>;
}

export interface CreateBriefResultPublisherInput {
  connection: ProducerConnection;
  logger: Logger;
  topic: string;
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
  const { connection, topic } = input;
  const topicPublisher = createTopicPublisher<TPayload>({
    connection,
    topic,
  });

  return {
    async publishResult(requestId, payload): Promise<void> {
      await topicPublisher.publish(requestId, payload);
    },
  };
}
