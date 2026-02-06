// Re-export Prisma client and types
export { prisma, PrismaClient } from "./client.js";
export { upsertConsumerLag, type ConsumerLagUpdate } from "./consumer-lag.js";

// Re-export generated types for convenience
export type {
  RawEvent,
  TrendSnapshot,
  BriefResult,
  ConsumerLag,
  RetentionPolicy,
  // SourceCheckpoint is deprecated - collector uses local SQLite
} from "@prisma/client";

export { BriefStatus, Prisma, Source, TrendWindow } from "@prisma/client";
