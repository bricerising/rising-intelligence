import { PrismaClient, type Prisma } from "@prisma/client";

const DEVELOPMENT_PRISMA_LOG_LEVELS = ["query", "warn", "error"] as const;
const DEFAULT_PRISMA_LOG_LEVELS = ["error"] as const;

export interface PrismaClientFactoryInput {
  databaseUrl?: string;
  nodeEnv?: string;
}

function resolvePrismaLogLevels(nodeEnv: string | undefined): Prisma.LogLevel[] {
  if (nodeEnv === "development") {
    return [...DEVELOPMENT_PRISMA_LOG_LEVELS];
  }
  return [...DEFAULT_PRISMA_LOG_LEVELS];
}

function createPrismaClientOptions(
  input: PrismaClientFactoryInput = {}
): Prisma.PrismaClientOptions {
  const options: Prisma.PrismaClientOptions = {
    log: resolvePrismaLogLevels(input.nodeEnv ?? process.env.NODE_ENV),
  };

  if (input.databaseUrl) {
    options.datasources = {
      db: {
        url: input.databaseUrl,
      },
    };
  }

  return options;
}

export function createPrismaClient(
  input: PrismaClientFactoryInput = {}
): PrismaClient {
  return new PrismaClient(createPrismaClientOptions(input));
}

export async function createConnectedPrismaClient(
  input: PrismaClientFactoryInput = {}
): Promise<PrismaClient> {
  const prisma = createPrismaClient(input);
  await prisma.$connect();
  return prisma;
}

export async function withPrismaClient<TResult>(
  input: PrismaClientFactoryInput,
  work: (prisma: PrismaClient) => Promise<TResult>
): Promise<TResult> {
  const prisma = createPrismaClient(input);
  try {
    return await work(prisma);
  } finally {
    await prisma.$disconnect();
  }
}

// Singleton pattern for Prisma client to prevent connection exhaustion
// See: https://www.prisma.io/docs/guides/performance-and-optimization/connection-management
const globalForPrisma = globalThis as unknown as {
  prisma: PrismaClient | undefined;
};

export const prisma = globalForPrisma.prisma ?? createPrismaClient();

if (process.env.NODE_ENV !== "production") {
  globalForPrisma.prisma = prisma;
}

export { PrismaClient };
