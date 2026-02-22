import { describe, expect, it, vi } from "vitest";
import {
  createTopicMessageHandlerMap,
  mapTrendWindowToEnum,
  runWithInFlightHeartbeats,
  type TopicMessageCommand,
} from "../src/topic-message-handlers.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createLogger() {
  return {
    warn: vi.fn(),
  } as any;
}

describe("topic message handlers", () => {
  it("maps supported trend window numbers to Prisma enums", () => {
    expect(mapTrendWindowToEnum(1)).toBe("WINDOW_15M");
    expect(mapTrendWindowToEnum(2)).toBe("WINDOW_60M");
    expect(mapTrendWindowToEnum(3)).toBe("WINDOW_24H");
  });

  it("throws for unsupported trend window values", () => {
    expect(() => mapTrendWindowToEnum(99)).toThrow(
      "Unsupported trend window: 99"
    );
  });

  it("creates executable topic handlers from command definitions", async () => {
    const execute = vi.fn(async () => undefined);
    const commands: readonly TopicMessageCommand[] = [
      {
        name: "summary-request",
        topic: "brief.summary.requests",
        execute,
      },
    ];

    const handlers = createTopicMessageHandlerMap(commands);
    const handler = handlers.get("brief.summary.requests");
    expect(handler).toBeDefined();

    const input = {
      messageValue: Buffer.from("{}"),
      messageLogger: createLogger(),
      heartbeat: async () => undefined,
    };

    await handler?.(input);
    expect(execute).toHaveBeenCalledOnce();
    expect(execute).toHaveBeenCalledWith(input);
  });

  it("fails fast when two commands target the same topic", () => {
    const commands: readonly TopicMessageCommand[] = [
      {
        name: "trend-snapshot",
        topic: "brief.messages",
        async execute() {
          return undefined;
        },
      },
      {
        name: "summary-request",
        topic: "brief.messages",
        async execute() {
          return undefined;
        },
      },
    ];

    expect(() => createTopicMessageHandlerMap(commands)).toThrow(
      'Topic message command "summary-request" duplicates topic handler for "brief.messages"'
    );
  });

  it("fails fast when a command resolves to an empty topic", () => {
    const commands: readonly TopicMessageCommand[] = [
      {
        name: "empty-topic",
        topic: "   ",
        async execute() {
          return undefined;
        },
      },
    ];

    expect(() => createTopicMessageHandlerMap(commands)).toThrow(
      'Topic message command "empty-topic" resolved to an empty topic'
    );
  });

  it("does not run overlapping heartbeat calls while work is in flight", async () => {
    let inFlightHeartbeats = 0;
    let maxInFlightHeartbeats = 0;
    const heartbeat = vi.fn(async () => {
      inFlightHeartbeats += 1;
      maxInFlightHeartbeats = Math.max(maxInFlightHeartbeats, inFlightHeartbeats);
      await sleep(20);
      inFlightHeartbeats -= 1;
    });

    await runWithInFlightHeartbeats(
      heartbeat,
      createLogger(),
      async () => {
        await sleep(70);
      },
      5
    );

    expect(heartbeat).toHaveBeenCalled();
    expect(maxInFlightHeartbeats).toBe(1);
  });

  it("logs and suppresses heartbeat failures", async () => {
    const logger = createLogger();
    const heartbeat = vi.fn(async () => {
      throw new Error("heartbeat failure");
    });

    await expect(
      runWithInFlightHeartbeats(
        heartbeat,
        logger,
        async () => {
          await sleep(20);
        },
        5
      )
    ).resolves.toBeUndefined();

    expect(logger.warn).toHaveBeenCalled();
  });

  it("throws when heartbeat interval is invalid", async () => {
    await expect(
      runWithInFlightHeartbeats(
        async () => undefined,
        createLogger(),
        async () => undefined,
        0
      )
    ).rejects.toThrow(
      "Heartbeat interval must be a positive integer, received: 0"
    );
  });
});
