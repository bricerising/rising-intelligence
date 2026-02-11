import type { Producer } from "kafkajs";
import type { Logger } from "pino";
import { publishEvent, TOPICS } from "./kafka/producer.js";
import {
  serializeDeadLetterEvent,
  serializeHeartbeat,
  serializeRawEvent,
} from "./serializer.js";
import type { CollectorHeartbeat, DeadLetterEvent, RawEvent } from "./types.js";

export interface CollectorPublisher {
  publishRawEvent(event: RawEvent): Promise<void>;
  publishDeadLetterEvent(event: DeadLetterEvent): Promise<void>;
  publishHeartbeat(event: CollectorHeartbeat): Promise<void>;
}

type PublishEventFn = typeof publishEvent;

export interface CreateCollectorPublisherInput {
  producer: Producer;
  logger: Logger;
  publish?: PublishEventFn;
}

/**
 * Facade that centralizes collector Kafka topic routing and payload serialization.
 * Callers publish typed domain events without repeating topic/key/serialization details.
 */
export function createCollectorPublisher(
  input: CreateCollectorPublisherInput
): CollectorPublisher {
  const publish = input.publish ?? publishEvent;
  const { producer, logger } = input;

  return {
    async publishRawEvent(event: RawEvent): Promise<void> {
      await publish(
        producer,
        TOPICS.RAW_EVENTS,
        event.event_id,
        serializeRawEvent(event),
        logger
      );
    },
    async publishDeadLetterEvent(event: DeadLetterEvent): Promise<void> {
      await publish(
        producer,
        TOPICS.DLQ,
        event.dlq_id,
        serializeDeadLetterEvent(event),
        logger
      );
    },
    async publishHeartbeat(event: CollectorHeartbeat): Promise<void> {
      await publish(
        producer,
        TOPICS.HEARTBEAT,
        event.source,
        serializeHeartbeat(event),
        logger
      );
    },
  };
}
