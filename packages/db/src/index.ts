// Re-export Prisma client and types
export { prisma, PrismaClient } from "./client.js";

// Re-export generated types for convenience
export type {
  RawEvent,
  TrendSnapshot,
  BriefResult,
  ConsumerLag,
  RetentionPolicy,
  // SourceCheckpoint is deprecated - collector uses local SQLite
} from "@prisma/client";

export { Source, TrendWindow, BriefStatus } from "@prisma/client";
