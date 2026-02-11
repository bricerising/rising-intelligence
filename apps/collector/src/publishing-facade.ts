import type { Producer } from "kafkajs";
import type { Logger } from "pino";
import { createKafkaTopicPublisher } from "@rising-intelligence/shared";
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
  const rawEventPublisher = createKafkaTopicPublisher<RawEvent, Logger>({
    producer,
    logger,
    topic: TOPICS.RAW_EVENTS,
    publish,
    serialize: serializeRawEvent,
  });
  const deadLetterPublisher = createKafkaTopicPublisher<DeadLetterEvent, Logger>({
    producer,
    logger,
    topic: TOPICS.DLQ,
    publish,
    serialize: serializeDeadLetterEvent,
  });
  const heartbeatPublisher = createKafkaTopicPublisher<CollectorHeartbeat, Logger>({
    producer,
    logger,
    topic: TOPICS.HEARTBEAT,
    publish,
    serialize: serializeHeartbeat,
  });

  return {
    async publishRawEvent(event: RawEvent): Promise<void> {
      await rawEventPublisher.publish(event.event_id, event);
    },
    async publishDeadLetterEvent(event: DeadLetterEvent): Promise<void> {
      await deadLetterPublisher.publish(event.dlq_id, event);
    },
    async publishHeartbeat(event: CollectorHeartbeat): Promise<void> {
      await heartbeatPublisher.publish(event.source, event);
    },
  };
}
