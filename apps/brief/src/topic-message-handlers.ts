import { TrendWindow } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared";
import type pino from "pino";

export interface TopicMessageHandlerInput {
  messageValue: Buffer;
  messageLogger: pino.Logger;
  heartbeat: () => Promise<void>;
}

export type TopicMessageHandler = (input: TopicMessageHandlerInput) => Promise<void>;

export interface TopicMessageCommand {
  readonly name: string;
  readonly topic: string;
  execute(input: TopicMessageHandlerInput): Promise<void>;
}

const TREND_WINDOW_ENUM_BY_VALUE = new Map<number, TrendWindow>([
  [1, TrendWindow.WINDOW_15M],
  [2, TrendWindow.WINDOW_60M],
  [3, TrendWindow.WINDOW_24H],
]);

const HEARTBEAT_WARNING_MESSAGE =
  "Background Kafka heartbeat failed while processing summary request";

export function mapTrendWindowToEnum(window: number): TrendWindow {
  const mappedWindow = TREND_WINDOW_ENUM_BY_VALUE.get(window);
  if (mappedWindow) {
    return mappedWindow;
  }

  throw new Error(`Unsupported trend window: ${window}`);
}

export function createTopicMessageHandlerMap(
  commands: readonly TopicMessageCommand[]
): Map<string, TopicMessageHandler> {
  const handlers = new Map<string, TopicMessageHandler>();

  for (const command of commands) {
    const topic = command.topic.trim();
    if (!topic) {
      throw new Error(`Topic message command "${command.name}" resolved to an empty topic`);
    }

    if (handlers.has(topic)) {
      throw new Error(
        `Topic message command "${command.name}" duplicates topic handler for "${topic}"`
      );
    }

    handlers.set(topic, async (input) => {
      await command.execute(input);
    });
  }

  return handlers;
}

export async function runWithInFlightHeartbeats(
  heartbeat: () => Promise<void>,
  logger: Pick<pino.Logger, "warn">,
  work: () => Promise<void>,
  intervalMs: number
): Promise<void> {
  if (!Number.isInteger(intervalMs) || intervalMs <= 0) {
    throw new Error(
      `Heartbeat interval must be a positive integer, received: ${intervalMs}`
    );
  }

  let heartbeatInFlight: Promise<void> | null = null;
  const scheduleHeartbeat = (): void => {
    if (heartbeatInFlight) {
      return;
    }

    heartbeatInFlight = heartbeat()
      .catch((error) => {
        logger.warn(
          { error: serializeError(error) },
          HEARTBEAT_WARNING_MESSAGE
        );
      })
      .finally(() => {
        heartbeatInFlight = null;
      });
  };

  const interval = setInterval(scheduleHeartbeat, intervalMs);
  interval.unref();

  try {
    await work();
  } finally {
    clearInterval(interval);
    await heartbeatInFlight;
  }
}
