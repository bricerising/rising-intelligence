import { prisma } from "./client.js";

type CleanupResult = {
  tableName: string;
  retentionDays: number;
  rowsDeleted: number;
  durationMs: number;
};

type CleanupFailure = {
  tableName: string;
  retentionDays: number;
  durationMs: number;
  error: string;
};

const CLEANUP_HANDLERS = {
  raw_events: async (cutoff: Date) =>
    prisma.rawEvent.deleteMany({
      where: { fetchedAt: { lt: cutoff } },
    }),
  trend_snapshots: async (cutoff: Date) =>
    prisma.trendSnapshot.deleteMany({
      where: { generatedAt: { lt: cutoff } },
    }),
  brief_results: async (cutoff: Date) =>
    prisma.briefResult.deleteMany({
      where: { producedAt: { lt: cutoff } },
    }),
  consumer_lag: async (cutoff: Date) =>
    prisma.consumerLag.deleteMany({
      where: { updatedAt: { lt: cutoff } },
    }),
  discovery_candidates: async (cutoff: Date) =>
    prisma.discoveryCandidate.deleteMany({
      where: { lastSeenAt: { lt: cutoff } },
    }),
} as const satisfies Record<string, (cutoff: Date) => Promise<{ count: number }>>;

function isCleanupTableName(
  tableName: string,
): tableName is keyof typeof CLEANUP_HANDLERS {
  return Object.prototype.hasOwnProperty.call(CLEANUP_HANDLERS, tableName);
}

async function runRetentionCleanup(): Promise<{
  successes: CleanupResult[];
  failures: CleanupFailure[];
}> {
  const policies = await prisma.retentionPolicy.findMany({
    where: { enabled: true, retentionDays: { gt: 0 } },
    orderBy: { tableName: "asc" },
  });

  const successes: CleanupResult[] = [];
  const failures: CleanupFailure[] = [];

  for (const policy of policies) {
    if (!isCleanupTableName(policy.tableName)) {
      console.warn(`Skipping unknown retention policy table: ${policy.tableName}`);
      continue;
    }
    const handler = CLEANUP_HANDLERS[policy.tableName];

    const cutoff = new Date(Date.now() - policy.retentionDays * 24 * 60 * 60 * 1000);
    const start = Date.now();
    try {
      const result = await handler(cutoff);

      await prisma.retentionPolicy.update({
        where: { tableName: policy.tableName },
        data: { lastCleanupAt: new Date() },
      });

      successes.push({
        tableName: policy.tableName,
        retentionDays: policy.retentionDays,
        rowsDeleted: result.count,
        durationMs: Date.now() - start,
      });
    } catch (error) {
      failures.push({
        tableName: policy.tableName,
        retentionDays: policy.retentionDays,
        durationMs: Date.now() - start,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return { successes, failures };
}

async function main() {
  console.log("Running retention cleanup...");

  const { successes, failures } = await runRetentionCleanup();

  for (const result of successes) {
    console.log(
      `  - ${result.tableName}: deleted ${result.rowsDeleted} rows (retention ${result.retentionDays}d) in ${result.durationMs}ms`,
    );
  }

  for (const failure of failures) {
    console.error(
      `  - ${failure.tableName}: FAILED (retention ${failure.retentionDays}d) in ${failure.durationMs}ms: ${failure.error}`,
    );
  }

  if (failures.length > 0) {
    throw new Error(`Retention cleanup failed for ${failures.length} table(s)`);
  }

  console.log("Retention cleanup complete.");
}

main()
  .catch((error) => {
    console.error("Retention cleanup failed:", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
