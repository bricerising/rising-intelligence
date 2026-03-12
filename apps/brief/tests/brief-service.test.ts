import { describe, expect, it, vi } from "vitest";
import { createHealthContext } from "../src/health.js";
import { createBriefService } from "../src/brief-service.js";

function makeLogger() {
  return {
    warn: vi.fn(),
  } as any;
}

describe("brief service trend snapshot handling", () => {
  it("treats duplicate trend snapshots as idempotent", async () => {
    const service = createBriefService();
    const logger = makeLogger();
    const ctx = {
      healthContext: createHealthContext(5),
      prisma: {
        briefTrendSnapshot: {
          create: vi.fn().mockRejectedValue(Object.assign(new Error("duplicate"), { code: "P2002" })),
        },
      },
    } as any;

    await service.handleTrendSnapshot(
      ctx,
      Buffer.from(
        JSON.stringify({
          generated_at: "2026-03-12T15:53:35.949Z",
          window: 2,
          topics: [{ topic: "aws.bedrock", score: 5, volume: 3, acceleration: 0.2 }],
        }),
        "utf-8"
      ),
      logger
    );

    expect(ctx.prisma.briefTrendSnapshot.create).toHaveBeenCalledOnce();
    expect(ctx.healthContext.postgresHealthy).toBe(true);
    expect(logger.warn).not.toHaveBeenCalled();
  });
});
