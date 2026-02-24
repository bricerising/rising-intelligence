import { Prisma, type PrismaClient } from "@rising-intelligence/db";
import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";

export interface BudgetMirrorCommandInput {
  prisma: PrismaClient;
  logger: pino.Logger;
  dateKey: string;
  budgetDate: Date;
  dailyBudgetUsd: number;
  cachedSpentUsd: number;
}

export interface BudgetMirrorCommand {
  execute(input: BudgetMirrorCommandInput): Promise<void>;
}

function toFiniteNumber(value: unknown): number {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function isDuplicateBudgetDateError(error: unknown): boolean {
  if (error instanceof Prisma.PrismaClientKnownRequestError) {
    return error.code === "P2002";
  }

  if (typeof error !== "object" || error === null) {
    return false;
  }

  const maybeCode = (error as { code?: unknown }).code;
  return maybeCode === "P2002";
}

async function loadTrackedSpendUsd(
  prisma: PrismaClient,
  budgetDate: Date
): Promise<number | null> {
  const existing = await prisma.briefBudgetTracking.findUnique({
    where: { date: budgetDate },
    select: { spentUsd: true },
  });
  if (!existing) {
    return null;
  }

  return toFiniteNumber(existing.spentUsd);
}

class MaxSpendBudgetMirrorCommand implements BudgetMirrorCommand {
  async execute({
    prisma,
    logger,
    dateKey,
    budgetDate,
    dailyBudgetUsd,
    cachedSpentUsd,
  }: BudgetMirrorCommandInput): Promise<void> {
    try {
      const trackedSpentUsd = await loadTrackedSpendUsd(prisma, budgetDate);
      if (trackedSpentUsd === null) {
        try {
          await prisma.briefBudgetTracking.create({
            data: {
              date: budgetDate,
              spentUsd: cachedSpentUsd,
              budgetUsd: dailyBudgetUsd,
              requestCount: 1,
            },
          });
          return;
        } catch (error) {
          if (!isDuplicateBudgetDateError(error)) {
            throw error;
          }
        }
      }

      const latestTrackedSpendUsd = await loadTrackedSpendUsd(prisma, budgetDate);
      if (latestTrackedSpendUsd === null) {
        await prisma.briefBudgetTracking.create({
          data: {
            date: budgetDate,
            spentUsd: cachedSpentUsd,
            budgetUsd: dailyBudgetUsd,
            requestCount: 1,
          },
        });
        return;
      }

      await prisma.briefBudgetTracking.update({
        where: { date: budgetDate },
        data: {
          spentUsd: Math.max(latestTrackedSpendUsd, cachedSpentUsd),
          budgetUsd: dailyBudgetUsd,
          requestCount: { increment: 1 },
        },
      });
    } catch (error) {
      logger.warn(
        {
          dateKey,
          cachedSpentUsd,
          error: serializeError(error),
        },
        "Failed to asynchronously mirror reserved budget to Postgres"
      );
    }
  }
}

export function createBudgetMirrorCommand(): BudgetMirrorCommand {
  return new MaxSpendBudgetMirrorCommand();
}
