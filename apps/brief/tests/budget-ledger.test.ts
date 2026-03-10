import { describe, expect, it, vi } from "vitest";
import {
  createBriefBudgetGovernor,
  createBriefBudgetLedger,
} from "../src/contract.js";

function makeLogger() {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: vi.fn(),
  } as any;
}

interface BudgetState {
  spentUsd: number;
}

function makePrisma(state: BudgetState) {
  return {
    briefBudgetTracking: {
      upsert: vi.fn().mockImplementation(async () => ({ spentUsd: state.spentUsd })),
      updateMany: vi.fn().mockImplementation(async (args: any) => {
        const lteThreshold = args?.where?.spentUsd?.lte;
        if (typeof lteThreshold !== "undefined") {
          if (state.spentUsd > Number(lteThreshold)) {
            return { count: 0 };
          }

          const increment = Number(args?.data?.spentUsd?.increment ?? 0);
          state.spentUsd += increment;
          return { count: 1 };
        }

        const gteThreshold = args?.where?.spentUsd?.gte;
        const decrement = args?.data?.spentUsd?.decrement;
        if (typeof gteThreshold !== "undefined" && typeof decrement !== "undefined") {
          if (state.spentUsd < Number(gteThreshold)) {
            return { count: 0 };
          }

          state.spentUsd -= Number(decrement);
          return { count: 1 };
        }

        const increment = args?.data?.spentUsd?.increment;
        if (typeof increment !== "undefined") {
          state.spentUsd += Number(increment);
          return { count: 1 };
        }

        if (typeof args?.data?.spentUsd === "number") {
          state.spentUsd = args.data.spentUsd;
          return { count: 1 };
        }

        return { count: 0 };
      }),
      findUnique: vi.fn().mockImplementation(async () => ({ spentUsd: state.spentUsd })),
      update: vi.fn().mockImplementation(async (args: any) => {
        if (typeof args?.data?.spentUsd === "number") {
          state.spentUsd = args.data.spentUsd;
        }
        return { spentUsd: state.spentUsd };
      }),
    },
  } as any;
}

function makeRedis() {
  return {
    eval: vi.fn(),
    set: vi.fn().mockResolvedValue("OK"),
  } as any;
}

describe("createBriefBudgetLedger", () => {
  it("uses Redis reservation result when cache admits spend", async () => {
    const state: BudgetState = { spentUsd: 0.25 };
    const prisma = makePrisma(state);
    const redis = makeRedis();
    const logger = makeLogger();
    const mirrorCommand = { execute: vi.fn().mockResolvedValue(undefined) };

    redis.eval.mockResolvedValue([1, "0.5"]);

    const ledger = createBriefBudgetLedger({
      prisma,
      redis,
      logger,
      mirrorCommand,
    });

    const result = await ledger.reserve({
      dateKey: "2026-02-06",
      dailyBudgetUsd: 5,
      amountUsd: 0.25,
    });

    expect(result).toEqual({ reserved: true, spentUsd: 0.5 });
    expect(mirrorCommand.execute).toHaveBeenCalledOnce();
    expect(prisma.briefBudgetTracking.upsert).not.toHaveBeenCalled();
  });

  it("falls back to Postgres reservation when Redis denies the spend", async () => {
    const state: BudgetState = { spentUsd: 0.5 };
    const prisma = makePrisma(state);
    const redis = makeRedis();
    const logger = makeLogger();
    const mirrorCommand = { execute: vi.fn().mockResolvedValue(undefined) };

    redis.eval.mockResolvedValue([0, "0.5"]);

    const ledger = createBriefBudgetLedger({
      prisma,
      redis,
      logger,
      mirrorCommand,
    });

    const result = await ledger.reserve({
      dateKey: "2026-02-06",
      dailyBudgetUsd: 5,
      amountUsd: 0.2,
    });

    expect(result).toEqual({ reserved: true, spentUsd: 0.7 });
    expect(prisma.briefBudgetTracking.upsert).toHaveBeenCalledOnce();
    expect(redis.set).toHaveBeenCalledWith("brief:budget:2026-02-06", "0.7", "EX", 172800);
    expect(mirrorCommand.execute).not.toHaveBeenCalled();
  });

  it("warns and falls back to Postgres when Redis reservation errors", async () => {
    const state: BudgetState = { spentUsd: 4.95 };
    const prisma = makePrisma(state);
    const redis = makeRedis();
    const logger = makeLogger();

    redis.eval.mockRejectedValue(new Error("redis unavailable"));

    const ledger = createBriefBudgetLedger({
      prisma,
      redis,
      logger,
    });

    const result = await ledger.reserve({
      dateKey: "2026-02-06",
      dailyBudgetUsd: 5,
      amountUsd: 0.1,
    });

    expect(result).toEqual({ reserved: false, spentUsd: 4.95 });
    expect(logger.warn).toHaveBeenCalledWith(
      expect.objectContaining({
        dateKey: "2026-02-06",
        amountUsd: 0.1,
      }),
      "Redis budget reservation unavailable; falling back to Postgres"
    );
  });

  it("releases and settles budget spend through Postgres source of truth", async () => {
    const state: BudgetState = { spentUsd: 1.5 };
    const prisma = makePrisma(state);
    const redis = makeRedis();
    const logger = makeLogger();

    redis.eval.mockResolvedValue([1, "1.5"]);

    const ledger = createBriefBudgetLedger({
      prisma,
      redis,
      logger,
    });

    const released = await ledger.release({
      dateKey: "2026-02-06",
      amountUsd: 0.4,
    });
    const settled = await ledger.settle({
      dateKey: "2026-02-06",
      deltaUsd: -3,
    });

    expect(released).toBe(1.1);
    expect(settled).toBe(0);
    expect(redis.set).toHaveBeenCalledWith("brief:budget:2026-02-06", "0", "EX", 172800);
  });

  it("exposes a budget governor contract over the ledger", async () => {
    const state: BudgetState = { spentUsd: 0.5 };
    const prisma = makePrisma(state);
    const redis = makeRedis();
    const logger = makeLogger();
    const mirrorCommand = { execute: vi.fn().mockResolvedValue(undefined) };

    redis.eval.mockResolvedValue([1, "0.75"]);

    const governor = createBriefBudgetGovernor({
      prisma,
      redis,
      logger,
      mirrorCommand,
    });

    const decision = await governor.authorize({
      dateKey: "2026-02-06",
      dailyBudgetUsd: 5,
      estimatedCostUsd: 0.25,
    });
    const rolledBack = await governor.rollback(decision);
    const settled = await governor.settle({
      decision,
      actualCostUsd: 0.1,
    });

    expect(decision).toEqual({
      authorized: true,
      dateKey: "2026-02-06",
      dailyBudgetUsd: 5,
      reservedCostUsd: 0.25,
      spentUsd: 0.75,
    });
    expect(rolledBack).toBe(0.25);
    expect(settled).toBe(0.1);
  });
});
