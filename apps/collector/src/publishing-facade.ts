import type { Producer } from "kafkajs";
import type { Logger } from "pino";
import { createKeyedKafkaTopicPublisher } from "@rising-intelligence/shared";
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
  const rawEventPublisher = createKeyedKafkaTopicPublisher<RawEvent, Logger>({
    producer,
    logger,
    topic: TOPICS.RAW_EVENTS,
    publish,
    serialize: serializeRawEvent,
    getKey(event) {
      return event.event_id;
    },
  });
  const deadLetterPublisher = createKeyedKafkaTopicPublisher<DeadLetterEvent, Logger>({
    producer,
    logger,
    topic: TOPICS.DLQ,
    publish,
    serialize: serializeDeadLetterEvent,
    getKey(event) {
      return event.dlq_id;
    },
  });
  const heartbeatPublisher = createKeyedKafkaTopicPublisher<CollectorHeartbeat, Logger>({
    producer,
    logger,
    topic: TOPICS.HEARTBEAT,
    publish,
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
