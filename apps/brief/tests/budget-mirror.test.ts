import { describe, expect, it, vi } from "vitest";
import { createBudgetMirrorCommand } from "../src/budget-mirror.js";

function makeLogger() {
  return {
    warn: vi.fn(),
    info: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: vi.fn(),
  } as any;
}

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("budget mirror command", () => {
  it("creates a missing budget row with Redis cumulative spend", async () => {
    const logger = makeLogger();
    const state = { spentUsd: 0, requestCount: 0 };
    const prisma = {
      briefBudgetTracking: {
        findUnique: vi.fn().mockResolvedValue(null),
        create: vi.fn().mockImplementation(async (args: any) => {
          state.spentUsd = Number(args.data.spentUsd);
          state.requestCount = Number(args.data.requestCount);
          return { spentUsd: state.spentUsd };
        }),
        update: vi.fn(),
      },
    } as any;

    const command = createBudgetMirrorCommand();
    await command.execute({
      prisma,
      logger,
      dateKey: "2026-02-10",
      budgetDate: new Date("2026-02-10T00:00:00.000Z"),
      dailyBudgetUsd: 5,
      cachedSpentUsd: 0.4,
    });

    expect(prisma.briefBudgetTracking.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          spentUsd: 0.4,
          requestCount: 1,
        }),
      })
    );
    expect(prisma.briefBudgetTracking.update).not.toHaveBeenCalled();
    expect(state.spentUsd).toBe(0.4);
    expect(state.requestCount).toBe(1);
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("keeps mirrored spend monotonic when async create calls complete out of order", async () => {
    const logger = makeLogger();
    const state = { spentUsd: 0, requestCount: 0 };
    const firstCreateStarted = createDeferred<void>();
    const releaseFirstCreate = createDeferred<void>();
    let createCallCount = 0;
    const duplicateError = Object.assign(new Error("duplicate"), { code: "P2002" });

    const prisma = {
      briefBudgetTracking: {
        findUnique: vi.fn().mockImplementation(async () => {
          if (state.requestCount === 0) {
            return null;
          }
          return { spentUsd: state.spentUsd };
        }),
        create: vi.fn().mockImplementation(async (args: any) => {
          createCallCount += 1;
          if (createCallCount === 1) {
            firstCreateStarted.resolve();
            await releaseFirstCreate.promise;
          }

          if (state.requestCount > 0) {
            throw duplicateError;
          }

          state.spentUsd = Number(args.data.spentUsd);
          state.requestCount = Number(args.data.requestCount);
          return { spentUsd: state.spentUsd };
        }),
        update: vi.fn().mockImplementation(async (args: any) => {
          state.spentUsd = Number(args.data.spentUsd);
          state.requestCount += Number(args.data.requestCount.increment);
          return { spentUsd: state.spentUsd };
        }),
      },
    } as any;

    const command = createBudgetMirrorCommand();
    const baseInput = {
      prisma,
      logger,
      dateKey: "2026-02-10",
      budgetDate: new Date("2026-02-10T00:00:00.000Z"),
      dailyBudgetUsd: 5,
    };

    const first = command.execute({
      ...baseInput,
      cachedSpentUsd: 0.1,
    });
    await firstCreateStarted.promise;

    await command.execute({
      ...baseInput,
      cachedSpentUsd: 0.2,
    });

    releaseFirstCreate.resolve();
    await first;

    expect(state.spentUsd).toBe(0.2);
    expect(state.requestCount).toBe(2);
    expect(prisma.briefBudgetTracking.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          spentUsd: 0.2,
        }),
      })
    );
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("never lowers Postgres spend when Redis cached spend is stale", async () => {
    const logger = makeLogger();
    const state = { spentUsd: 0.45, requestCount: 3 };
    const prisma = {
      briefBudgetTracking: {
        findUnique: vi.fn().mockResolvedValue({ spentUsd: state.spentUsd }),
        create: vi.fn(),
        update: vi.fn().mockImplementation(async (args: any) => {
          state.spentUsd = Number(args.data.spentUsd);
          state.requestCount += Number(args.data.requestCount.increment);
          return { spentUsd: state.spentUsd };
        }),
      },
    } as any;

    const command = createBudgetMirrorCommand();
    await command.execute({
      prisma,
      logger,
      dateKey: "2026-02-10",
      budgetDate: new Date("2026-02-10T00:00:00.000Z"),
      dailyBudgetUsd: 5,
      cachedSpentUsd: 0.2,
    });

    expect(prisma.briefBudgetTracking.create).not.toHaveBeenCalled();
    expect(prisma.briefBudgetTracking.update).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          spentUsd: 0.45,
        }),
      })
    );
    expect(state.spentUsd).toBe(0.45);
    expect(state.requestCount).toBe(4);
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("logs and swallows unexpected mirror failures", async () => {
    const logger = makeLogger();
    const prisma = {
      briefBudgetTracking: {
        findUnique: vi.fn().mockRejectedValue(new Error("db unavailable")),
        create: vi.fn(),
        update: vi.fn(),
      },
    } as any;

    const command = createBudgetMirrorCommand();
    await command.execute({
      prisma,
      logger,
      dateKey: "2026-02-10",
      budgetDate: new Date("2026-02-10T00:00:00.000Z"),
      dailyBudgetUsd: 5,
      cachedSpentUsd: 0.2,
    });

    expect(logger.warn).toHaveBeenCalledWith(
      expect.any(Object),
      "Failed to asynchronously mirror reserved budget to Postgres"
    );
  });
});
