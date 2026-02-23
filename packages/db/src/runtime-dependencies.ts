import {
  createConnectedPrismaClient,
  type PrismaClient,
} from "./client.js";

export interface PrismaRuntimeDependencyFactoryInput<TConfig> {
  getDatabaseUrl(config: TConfig): string;
}

export interface PrismaRuntimeDependencies<TConfig> {
  createPrismaClient(config: TConfig): Promise<PrismaClient>;
  closePrismaClient(prisma: PrismaClient): Promise<void>;
}

/**
 * Factory Method for Prisma runtime lifecycle dependencies used by services.
 * Keeps service runtime factories focused on wiring other resources.
 */
export function createPrismaRuntimeDependencies<TConfig>(
  input: PrismaRuntimeDependencyFactoryInput<TConfig>
): PrismaRuntimeDependencies<TConfig> {
  return {
    createPrismaClient(config): Promise<PrismaClient> {
      return createConnectedPrismaClient({
        databaseUrl: input.getDatabaseUrl(config),
      });
    },
    closePrismaClient(prisma): Promise<void> {
      return prisma.$disconnect();
    },
  };
}
