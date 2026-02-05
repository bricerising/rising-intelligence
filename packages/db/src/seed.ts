import { prisma } from "./client.js";

/**
 * Seed script for initializing default data.
 * Run with: npm run db:seed
 */
async function main() {
  console.log("Seeding database...");

  // Initialize retention policies
  const retentionPolicies = [
    { tableName: "raw_events", retentionDays: 14 },
    { tableName: "trend_snapshots", retentionDays: 90 },
    { tableName: "brief_results", retentionDays: 180 },
    { tableName: "consumer_lag", retentionDays: 7 },
    // Note: source_checkpoints is deprecated (collector uses local SQLite)
  ];

  for (const policy of retentionPolicies) {
    await prisma.retentionPolicy.upsert({
      where: { tableName: policy.tableName },
      update: { retentionDays: policy.retentionDays },
      create: policy,
    });
    console.log(`  - Retention policy: ${policy.tableName} = ${policy.retentionDays} days`);
  }

  console.log("Seeding complete.");
}

main()
  .catch((e) => {
    console.error("Seed failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
