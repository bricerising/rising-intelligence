import { readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import {
  createPrismaClient,
  type PrismaClient,
} from "@rising-intelligence/db";
import { REPO_ROOT } from "@rising-intelligence/shared";
import type { CliFlags } from "../../lib/args.js";
import { getBooleanFlag, getStringFlag } from "../../lib/flags.js";
import { resolveTopicsDatabaseUrl } from "../topics/database-url.js";

const DEFAULT_REQUIRED_TABLES = [
  "raw_events",
  "trend_snapshots",
  "consumer_lag",
  "brief_results",
  "brief_budget_tracking",
] as const;

interface TestBootstrapCheckConfig {
  databaseUrl: string;
  requiredTables: string[];
  migrationsDir: string;
  skipMigrationsCheck: boolean;
  requiredMigrations: string[] | null;
}

interface BootstrapCheckResult {
  missingTables: string[];
  missingMigrations: string[];
  checkedTables: string[];
  checkedMigrations: string[];
}

function parseCsvFlag(rawValue: string, flagName: string): string[] {
  const values = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  const deduped = [...new Set(values)];
  if (deduped.length === 0) {
    throw new Error(`${flagName} resolved to an empty list`);
  }
  return deduped;
}

export function resolveTestBootstrapCheckConfig(flags: CliFlags): TestBootstrapCheckConfig {
  const requiredTablesRaw = getStringFlag(flags, "required-tables");
  const requiredTables = requiredTablesRaw
    ? parseCsvFlag(requiredTablesRaw, "--required-tables")
    : [...DEFAULT_REQUIRED_TABLES];

  const requiredMigrationsRaw = getStringFlag(flags, "required-migrations");
  const requiredMigrations = requiredMigrationsRaw
    ? parseCsvFlag(requiredMigrationsRaw, "--required-migrations")
    : null;

  const migrationsDir =
    getStringFlag(flags, "migrations-dir") ||
    resolve(REPO_ROOT, "packages", "db", "prisma", "migrations");

  return {
    databaseUrl: resolveTopicsDatabaseUrl(flags),
    requiredTables,
    migrationsDir,
    skipMigrationsCheck: getBooleanFlag(flags, "skip-migrations-check"),
    requiredMigrations,
  };
}

async function listExpectedMigrations(config: TestBootstrapCheckConfig): Promise<string[]> {
  if (config.requiredMigrations) {
    return config.requiredMigrations;
  }

  const entries = await readdir(config.migrationsDir, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((left, right) => left.localeCompare(right));
}

async function checkTables(prisma: PrismaClient, requiredTables: string[]): Promise<string[]> {
  const rows = await prisma.$queryRawUnsafe<Array<{ table_name: string }>>(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
  );
  const existing = new Set(rows.map((row) => row.table_name));
  return requiredTables.filter((table) => !existing.has(table));
}

async function checkMigrations(prisma: PrismaClient, expected: string[]): Promise<string[]> {
  const rows = await prisma.$queryRawUnsafe<Array<{ migration_name: string }>>(
    "SELECT migration_name FROM _prisma_migrations"
  );
  const applied = new Set(rows.map((row) => row.migration_name));
  return expected.filter((migration) => !applied.has(migration));
}

async function runBootstrapCheck(config: TestBootstrapCheckConfig): Promise<BootstrapCheckResult> {
  const prisma = createPrismaClient({
    databaseUrl: config.databaseUrl,
  });
  const result: BootstrapCheckResult = {
    missingTables: [],
    missingMigrations: [],
    checkedTables: [...config.requiredTables],
    checkedMigrations: [],
  };

  try {
    result.missingTables = await checkTables(prisma, config.requiredTables);

    if (!config.skipMigrationsCheck) {
      const expectedMigrations = await listExpectedMigrations(config);
      result.checkedMigrations = [...expectedMigrations];
      try {
        result.missingMigrations = await checkMigrations(prisma, expectedMigrations);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (message.includes("_prisma_migrations")) {
          result.missingMigrations = [...expectedMigrations];
        } else {
          throw error;
        }
      }
    }
  } finally {
    await prisma.$disconnect();
  }

  return result;
}

export async function dbTestBootstrapCheck(flags: CliFlags): Promise<void> {
  const config = resolveTestBootstrapCheckConfig(flags);
  const result = await runBootstrapCheck(config);
  const hasFailures = result.missingTables.length > 0 || result.missingMigrations.length > 0;

  // eslint-disable-next-line no-console
  console.log("Test bootstrap check:");
  // eslint-disable-next-line no-console
  console.log(`  required tables checked: ${result.checkedTables.length}`);
  // eslint-disable-next-line no-console
  console.log(`  missing tables: ${result.missingTables.length}`);
  for (const table of result.missingTables) {
    // eslint-disable-next-line no-console
    console.log(`    - ${table}`);
  }

  if (!config.skipMigrationsCheck) {
    // eslint-disable-next-line no-console
    console.log(`  expected migrations checked: ${result.checkedMigrations.length}`);
    // eslint-disable-next-line no-console
    console.log(`  missing migrations: ${result.missingMigrations.length}`);
    for (const migration of result.missingMigrations) {
      // eslint-disable-next-line no-console
      console.log(`    - ${migration}`);
    }
  }

  if (hasFailures) {
    throw new Error("Test bootstrap check failed: missing required tables and/or migrations");
  }

  // eslint-disable-next-line no-console
  console.log("✅ Test bootstrap check passed.");
}

export function resolveDefaultMigrationsDir(): string {
  return join(REPO_ROOT, "packages", "db", "prisma", "migrations");
}
