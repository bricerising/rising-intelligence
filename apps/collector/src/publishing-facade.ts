import type { Logger } from "pino";
import {
  createKeyedTopicPublisher,
  type ProducerConnection,
} from "@rising-intelligence/pipeline/transport";
import {
  serializeDeadLetterEvent,
  serializeHeartbeat,
  serializeRawEvent,
} from "./serializer.js";
import type { CollectorHeartbeat, DeadLetterEvent, RawEvent } from "./types.js";

export const TOPICS = {
  RAW_EVENTS: "events.raw",
  DLQ: "events.raw.dlq",
  HEARTBEAT: "collector.heartbeat",
} as const;

export interface CollectorPublisher {
  publishRawEvent(event: RawEvent): Promise<void>;
  publishDeadLetterEvent(event: DeadLetterEvent): Promise<void>;
  publishHeartbeat(event: CollectorHeartbeat): Promise<void>;
}

export interface CreateCollectorPublisherInput {
  connection: ProducerConnection;
  logger: Logger;
}

/**
 * Facade that centralizes collector Kafka topic routing and payload serialization.
 * Callers publish typed domain events without repeating topic/key/serialization details.
 */
export function createCollectorPublisher(
  input: CreateCollectorPublisherInput
): CollectorPublisher {
  const { connection } = input;
  const rawEventPublisher = createKeyedTopicPublisher<RawEvent>({
    connection,
    topic: TOPICS.RAW_EVENTS,
    serialize: serializeRawEvent,
    getKey(event) {
      return event.event_id;
    },
  });
  const deadLetterPublisher = createKeyedTopicPublisher<DeadLetterEvent>({
    connection,
    topic: TOPICS.DLQ,
    serialize: serializeDeadLetterEvent,
    getKey(event) {
      return event.dlq_id;
    },
  });
  const heartbeatPublisher = createKeyedTopicPublisher<CollectorHeartbeat>({
    connection,
    topic: TOPICS.HEARTBEAT,
    serialize: serializeHeartbeat,
    getKey(event) {
      return event.source;
    },
  });

  return {
    async publishRawEvent(event: RawEvent): Promise<void> {
      await rawEventPublisher.publish(event);
    },
    async publishDeadLetterEvent(event: DeadLetterEvent): Promise<void> {
      await deadLetterPublisher.publish(event);
    },
    async publishHeartbeat(event: CollectorHeartbeat): Promise<void> {
      await heartbeatPublisher.publish(event);
    },
  };
}
