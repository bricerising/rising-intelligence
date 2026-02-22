import { BriefStatus, Prisma, type PrismaClient } from "@rising-intelligence/db";
import type { HealthContext } from "./health.js";
import {
  parseBriefResultPayload,
  type BriefResultPayload,
} from "./result-payload-adapter.js";

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

/**
 * Proxy that keeps Postgres health status aligned with brief-result store I/O.
 */
class HealthTrackedBriefResultStoreProxy implements BriefResultStore {
  constructor(
    private readonly next: BriefResultStore,
    private readonly healthContext: HealthContext
  ) {}

  async load(requestId: string): Promise<StoredBriefResult | null> {
    return this.withPostgresHealth(() => this.next.load(requestId));
  }

  async persist(
    payload: BriefResultPayload,
    status: BriefStatus
  ): Promise<PersistBriefResultOutcome> {
    return this.withPostgresHealth(() => this.next.persist(payload, status));
  }

  private async withPostgresHealth<T>(operation: () => Promise<T>): Promise<T> {
    try {
      const result = await operation();
      this.healthContext.postgresHealthy = true;
      return result;
    } catch (error) {
      this.healthContext.postgresHealthy = false;
      throw error;
    }
  }
}

export function createBriefResultStore(
  prisma: PrismaClient,
  healthContext: HealthContext
): BriefResultStore {
  return new HealthTrackedBriefResultStoreProxy(
    new PrismaBriefResultStore(prisma),
    healthContext
  );
}
