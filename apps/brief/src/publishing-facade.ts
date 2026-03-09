import type { Logger } from "pino";
import {
  createTopicPublisher,
  type ProducerConnection,
} from "@rising-intelligence/pipeline/transport";
import type { BriefResultPayload } from "./result-payload-adapter.js";

export interface BriefResultPublisher {
  publishResult(requestId: string, payload: BriefResultPayload): Promise<void>;
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
export function createBriefResultPublisher(
  input: CreateBriefResultPublisherInput
): BriefResultPublisher {
  const { connection, topic } = input;
  const topicPublisher = createTopicPublisher<BriefResultPayload>({
    connection,
    topic,
  });

  return {
    async publishResult(requestId, payload): Promise<void> {
      await topicPublisher.publish(requestId, payload);
    },
  };
}
