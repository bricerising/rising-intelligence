import type { PrismaClient } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type { Redis } from "ioredis";
import type pino from "pino";
import {
  createBudgetMirrorCommand,
  type BudgetMirrorCommand,
} from "./budget-mirror.js";

const BUDGET_KEY_PREFIX = "brief:budget";
const BUDGET_KEY_TTL_SECONDS = 48 * 60 * 60;
const BUDGET_RESERVATION_SCRIPT = `
local key = KEYS[1]
local max_budget = tonumber(ARGV[1])
local amount = tonumber(ARGV[2])
local ttl_seconds = tonumber(ARGV[3])

local current = tonumber(redis.call("GET", key) or "0")
if (current + amount) > max_budget then
  return {0, tostring(current)}
end

local next = redis.call("INCRBYFLOAT", key, amount)
redis.call("EXPIRE", key, ttl_seconds)
return {1, tostring(next)}
`;

export interface BudgetReservationResult {
  reserved: boolean;
  spentUsd: number;
}

export interface ReserveBudgetInput {
  dateKey: string;
  dailyBudgetUsd: number;
  amountUsd: number;
}

export interface ReleaseBudgetInput {
  dateKey: string;
  amountUsd: number;
}

export interface SettleBudgetInput {
  dateKey: string;
  deltaUsd: number;
}

export interface BriefBudgetLedger {
  reserve(input: ReserveBudgetInput): Promise<BudgetReservationResult>;
  release(input: ReleaseBudgetInput): Promise<number>;
  settle(input: SettleBudgetInput): Promise<number>;
}

export interface CreateBriefBudgetLedgerInput {
  prisma: PrismaClient;
  redis: Redis;
  logger: pino.Logger;
  mirrorCommand?: BudgetMirrorCommand;
}

export interface AuthorizeBudgetInput {
  dateKey: string;
  dailyBudgetUsd: number;
  estimatedCostUsd: number;
}

export interface BriefBudgetDecision {
  authorized: boolean;
  dateKey: string;
  dailyBudgetUsd: number;
  reservedCostUsd: number;
  spentUsd: number;
}

export interface SettleBudgetDecisionInput {
  decision: BriefBudgetDecision;
  actualCostUsd: number;
}

export interface BriefBudgetGovernor {
  authorize(input: AuthorizeBudgetInput): Promise<BriefBudgetDecision>;
  rollback(decision: BriefBudgetDecision): Promise<number>;
  settle(input: SettleBudgetDecisionInput): Promise<number>;
}

function toBudgetDate(dateKey: string): Date {
  return new Date(`${dateKey}T00:00:00Z`);
}

function getBudgetKey(dateKey: string): string {
  return `${BUDGET_KEY_PREFIX}:${dateKey}`;
}

function toNumeric(value: unknown): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function parseBudgetReservationResult(result: unknown): BudgetReservationResult {
  if (!Array.isArray(result) || result.length < 2) {
    throw new Error("Unexpected Redis budget reservation response");
  }

  return {
    reserved: toNumeric(result[0]) === 1,
    spentUsd: toNumeric(result[1]),
  };
}

function toBudgetDecision(
  input: AuthorizeBudgetInput,
  reservation: BudgetReservationResult
): BriefBudgetDecision {
  return {
    authorized: reservation.reserved,
    dateKey: input.dateKey,
    dailyBudgetUsd: input.dailyBudgetUsd,
    reservedCostUsd: input.estimatedCostUsd,
    spentUsd: reservation.spentUsd,
  };
}

async function syncBudgetCacheBestEffort(
  redis: Redis,
  dateKey: string,
  spentUsd: number,
  logger: pino.Logger
): Promise<void> {
  try {
    await redis.set(
      getBudgetKey(dateKey),
      spentUsd.toString(),
      "EX",
      BUDGET_KEY_TTL_SECONDS
    );
  } catch (error) {
    logger.warn(
      {
        dateKey,
        spentUsd,
        error: serializeError(error),
      },
      "Failed to sync brief budget cache; continuing with Postgres source of truth"
    );
  }
}

class PostgresBudgetLedger implements BriefBudgetLedger {
  constructor(
    private readonly prisma: PrismaClient,
    private readonly redis: Redis,
    private readonly logger: pino.Logger
  ) {}

  async reserve(input: ReserveBudgetInput): Promise<BudgetReservationResult> {
    const budgetDate = toBudgetDate(input.dateKey);
    const record = await this.prisma.briefBudgetTracking.upsert({
      where: { date: budgetDate },
      create: {
        date: budgetDate,
        spentUsd: 0,
        budgetUsd: input.dailyBudgetUsd,
        requestCount: 0,
      },
      update: {},
      select: { spentUsd: true },
    });

    const maxSpendBeforeReservation = Math.max(0, input.dailyBudgetUsd - input.amountUsd);
    const whereClause = Number.isFinite(maxSpendBeforeReservation)
      ? { date: budgetDate, spentUsd: { lte: maxSpendBeforeReservation } }
      : { date: budgetDate };

    const updateResult = await this.prisma.briefBudgetTracking.updateMany({
      // Prevent concurrent workers from oversubscribing daily budget.
      where: whereClause,
      data: {
        spentUsd: { increment: input.amountUsd },
        requestCount: { increment: 1 },
      },
    });

    if (updateResult.count === 0) {
      const latest = await this.prisma.briefBudgetTracking.findUnique({
        where: { date: budgetDate },
        select: { spentUsd: true },
      });
      const spentUsd = Number(latest?.spentUsd ?? record.spentUsd);
      await syncBudgetCacheBestEffort(this.redis, input.dateKey, spentUsd, this.logger);
      return {
        reserved: false,
        spentUsd,
      };
    }

    const latest = await this.prisma.briefBudgetTracking.findUnique({
      where: { date: budgetDate },
      select: { spentUsd: true },
    });
    const spentUsd = Number(latest?.spentUsd ?? Number(record.spentUsd) + input.amountUsd);
    await syncBudgetCacheBestEffort(this.redis, input.dateKey, spentUsd, this.logger);
    return {
      reserved: true,
      spentUsd,
    };
  }

  async release(input: ReleaseBudgetInput): Promise<number> {
    return this.updateSpentUsd(input.dateKey, -input.amountUsd);
  }

  async settle(input: SettleBudgetInput): Promise<number> {
    return this.updateSpentUsd(input.dateKey, input.deltaUsd);
  }

  private async updateSpentUsd(
    dateKey: string,
    deltaUsd: number
  ): Promise<number> {
    const budgetDate = toBudgetDate(dateKey);
    if (!Number.isFinite(deltaUsd) || deltaUsd === 0) {
      const existing = await this.prisma.briefBudgetTracking.findUnique({
        where: { date: budgetDate },
        select: { spentUsd: true },
      });
      const spentUsd = Number(existing?.spentUsd ?? 0);
      await syncBudgetCacheBestEffort(this.redis, dateKey, spentUsd, this.logger);
      return spentUsd;
    }

    if (deltaUsd > 0) {
      const incrementResult = await this.prisma.briefBudgetTracking.updateMany({
        where: { date: budgetDate },
        data: { spentUsd: { increment: deltaUsd } },
      });
      if (incrementResult.count === 0) {
        return 0;
      }
    } else {
      const decrementAmount = Math.abs(deltaUsd);
      const decrementResult = await this.prisma.briefBudgetTracking.updateMany({
        where: {
          date: budgetDate,
          spentUsd: { gte: decrementAmount },
        },
        data: { spentUsd: { decrement: decrementAmount } },
      });

      if (decrementResult.count === 0) {
        const clampResult = await this.prisma.briefBudgetTracking.updateMany({
          where: { date: budgetDate },
          data: { spentUsd: 0 },
        });
        if (clampResult.count === 0) {
          return 0;
        }
      }
    }

    const latest = await this.prisma.briefBudgetTracking.findUnique({
      where: { date: budgetDate },
      select: { spentUsd: true },
    });
    const spentUsd = Number(latest?.spentUsd ?? 0);
    await syncBudgetCacheBestEffort(this.redis, dateKey, spentUsd, this.logger);
    return spentUsd;
  }
}

interface RedisFirstBudgetLedgerProxyInput {
  prisma: PrismaClient;
  redis: Redis;
  logger: pino.Logger;
  mirrorCommand: BudgetMirrorCommand;
  next: BriefBudgetLedger;
}

/**
 * Proxy that prefers Redis for fast reservation checks while delegating
 * source-of-truth writes to Postgres via the wrapped ledger.
 */
class RedisFirstBudgetLedgerProxy implements BriefBudgetLedger {
  private readonly prisma: PrismaClient;
  private readonly redis: Redis;
  private readonly logger: pino.Logger;
  private readonly mirrorCommand: BudgetMirrorCommand;
  private readonly next: BriefBudgetLedger;

  constructor(input: RedisFirstBudgetLedgerProxyInput) {
    this.prisma = input.prisma;
    this.redis = input.redis;
    this.logger = input.logger;
    this.mirrorCommand = input.mirrorCommand;
    this.next = input.next;
  }

  async reserve(input: ReserveBudgetInput): Promise<BudgetReservationResult> {
    try {
      const result = await this.redis.eval(
        BUDGET_RESERVATION_SCRIPT,
        1,
        getBudgetKey(input.dateKey),
        input.dailyBudgetUsd.toString(),
        input.amountUsd.toString(),
        BUDGET_KEY_TTL_SECONDS.toString()
      );
      const cached = parseBudgetReservationResult(result);
      if (!cached.reserved) {
        return this.next.reserve(input);
      }

      void this.mirrorCommand.execute({
        prisma: this.prisma,
        logger: this.logger,
        dateKey: input.dateKey,
        budgetDate: toBudgetDate(input.dateKey),
        dailyBudgetUsd: input.dailyBudgetUsd,
        cachedSpentUsd: cached.spentUsd,
      });

      return cached;
    } catch (error) {
      this.logger.warn(
        {
          dateKey: input.dateKey,
          amountUsd: input.amountUsd,
          error: serializeError(error),
        },
        "Redis budget reservation unavailable; falling back to Postgres"
      );
      return this.next.reserve(input);
    }
  }

  async release(input: ReleaseBudgetInput): Promise<number> {
    return this.next.release(input);
  }

  async settle(input: SettleBudgetInput): Promise<number> {
    return this.next.settle(input);
  }
}

class LedgerBackedBudgetGovernor implements BriefBudgetGovernor {
  constructor(private readonly ledger: BriefBudgetLedger) {}

  async authorize(input: AuthorizeBudgetInput): Promise<BriefBudgetDecision> {
    const reservation = await this.ledger.reserve({
      dateKey: input.dateKey,
      dailyBudgetUsd: input.dailyBudgetUsd,
      amountUsd: input.estimatedCostUsd,
    });

    return toBudgetDecision(input, reservation);
  }

  async rollback(decision: BriefBudgetDecision): Promise<number> {
    if (decision.reservedCostUsd <= 0) {
      return decision.spentUsd;
    }

    return this.ledger.release({
      dateKey: decision.dateKey,
      amountUsd: decision.reservedCostUsd,
    });
  }

  async settle(input: SettleBudgetDecisionInput): Promise<number> {
    return this.ledger.settle({
      dateKey: input.decision.dateKey,
      deltaUsd: input.actualCostUsd - input.decision.reservedCostUsd,
    });
  }
}

const DEFAULT_BUDGET_MIRROR_COMMAND = createBudgetMirrorCommand();

export function createBriefBudgetLedger(
  input: CreateBriefBudgetLedgerInput
): BriefBudgetLedger {
  const postgresLedger = new PostgresBudgetLedger(
    input.prisma,
    input.redis,
    input.logger
  );

  return new RedisFirstBudgetLedgerProxy({
    prisma: input.prisma,
    redis: input.redis,
    logger: input.logger,
    mirrorCommand: input.mirrorCommand ?? DEFAULT_BUDGET_MIRROR_COMMAND,
    next: postgresLedger,
  });
}

export function createBriefBudgetGovernor(
  input: CreateBriefBudgetLedgerInput
): BriefBudgetGovernor {
  return new LedgerBackedBudgetGovernor(createBriefBudgetLedger(input));
}
