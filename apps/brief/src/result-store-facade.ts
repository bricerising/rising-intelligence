import { BriefStatus, Prisma, type PrismaClient } from "@rising-intelligence/db";
import type { HealthContext } from "./health.js";
import {
  parseBriefResultPayload,
  type BriefResultPayload,
} from "./result-payload-adapter.js";
import { createPostgresHealthProxy } from "./postgres-health-proxy.js";

export type PersistBriefResultOutcome = "created" | "duplicate";

export interface StoredBriefResult {
  status: BriefStatus;
  payload: BriefResultPayload;
}

export interface BriefResultStore {
  load(requestId: string): Promise<StoredBriefResult | null>;
  persist(
    payload: BriefResultPayload,
    status: BriefStatus
  ): Promise<PersistBriefResultOutcome>;
}

function isDuplicateKeyError(error: unknown): boolean {
  return error instanceof Prisma.PrismaClientKnownRequestError && error.code === "P2002";
}

/**
 * Facade around Prisma brief-result storage so processing code only depends on
 * a stable load/persist boundary.
 */
class PrismaBriefResultStore implements BriefResultStore {
  constructor(private readonly prisma: PrismaClient) {}

  async load(requestId: string): Promise<StoredBriefResult | null> {
    const existing = await this.prisma.briefResult.findUnique({
      where: { requestId },
      select: { status: true, result: true },
    });
    if (!existing) {
      return null;
    }

    return {
      status: existing.status,
      payload: parseBriefResultPayload(existing.result),
    };
  }

  async persist(
    payload: BriefResultPayload,
    status: BriefStatus
  ): Promise<PersistBriefResultOutcome> {
    try {
      await this.prisma.briefResult.create({
        data: {
          requestId: payload.request_id,
          producedAt: new Date(payload.produced_at),
          status,
          result: payload as Prisma.InputJsonValue,
        },
      });
      return "created";
    } catch (error) {
      if (isDuplicateKeyError(error)) {
        return "duplicate";
      }
      throw error;
    }
  }
}

export function createBriefResultStore(
  prisma: PrismaClient,
  healthContext: HealthContext
): BriefResultStore {
  return createPostgresHealthProxy(
    new PrismaBriefResultStore(prisma),
    healthContext,
    ["load", "persist"]
  );
}
