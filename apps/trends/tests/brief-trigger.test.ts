import { describe, expect, it, vi } from "vitest";
import { createHealthContext } from "../src/health.js";
import { maybeTriggerDailySummaryRequest } from "../src/brief-trigger.js";

function makeConfig() {
  return {
    DAILY_BRIEF_ENABLED: true,
  } as any;
}

function makeLogger() {
  const logger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: vi.fn(),
  };
  logger.child.mockReturnValue(logger);
  return logger as any;
}

describe("maybeTriggerDailySummaryRequest", () => {
  it("does not auto-trigger brief requests in request-driven mode", async () => {
    const producer = {
      send: vi.fn().mockResolvedValue(undefined),
    };

    const healthContext = createHealthContext();
    const logger = makeLogger();
    const lastDate = await maybeTriggerDailySummaryRequest({
      config: makeConfig(),
      logger,
      prisma: {} as any,
      producer: producer as any,
      healthContext,
      snapshots: [
        {
          window: "60m",
          generatedAt: new Date("2026-02-06T01:00:00.000Z"),
          topMetrics: [
            {
              topic: "aws.bedrock",
              window: "60m",
              volume: 25,
              prevVolume: 10,
              acceleration: 1.5,
              baselineVolume: 12,
              baselineDelta: 1.08,
              score: 62.5,
              evidenceEventIds: ["evt-1"],
            },
          ],
        },
      ],
      lastDailyTriggerDate: "2026-02-05",
      now: new Date("2026-02-06T01:05:00.000Z"),
    } as any);

    expect(lastDate).toBe("2026-02-05");
    expect(producer.send).not.toHaveBeenCalled();
    expect(logger.debug).toHaveBeenCalled();
  });
});
