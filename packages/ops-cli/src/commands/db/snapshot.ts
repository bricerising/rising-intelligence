import { spawn } from "node:child_process";
import { mkdir, readdir, rm, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { REPO_ROOT, getEnvString } from "@rising-intelligence/shared/config";
import type { CliFlags } from "../../lib/args.js";
import { getBooleanFlag, getStringFlag } from "../../lib/flags.js";
import {
  parseNonNegativeIntegerStrict,
  parsePositiveIntegerStrict,
} from "../../lib/number.js";
import { resolveTopicsDatabaseUrl } from "../topics/database-url.js";

const DEFAULT_OUTPUT_DIR = resolve(REPO_ROOT, "backups", "postgres");
const DEFAULT_RETENTION_DAYS = 14;
const DEFAULT_INTERVAL_SECONDS = 86_400;
const SNAPSHOT_PREFIX = "postgres-";
const SNAPSHOT_SUFFIX = ".dump";

interface SnapshotConfig {
  databaseUrl: string;
  outputDir: string;
  label: string | undefined;
  retentionDays: number;
  dryRun: boolean;
  loop: boolean;
  intervalSeconds: number;
}

interface SnapshotRunResult {
  outputPath: string;
  outputBytes: number;
  prunedFiles: string[];
}

interface DatabaseUrlCredentials {
  connectionUrl: string;
  password: string | null;
  databaseName: string;
}

function pad2(value: number): string {
  return String(value).padStart(2, "0");
}

export function formatUtcTimestamp(value: Date): string {
  return [
    String(value.getUTCFullYear()),
    pad2(value.getUTCMonth() + 1),
    pad2(value.getUTCDate()),
    "T",
    pad2(value.getUTCHours()),
    pad2(value.getUTCMinutes()),
    pad2(value.getUTCSeconds()),
    "Z",
  ].join("");
}

export function sanitizeSnapshotLabel(rawValue: string): string {
  return rawValue
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9._-]+/g, "-")
    .replaceAll(/-+/g, "-")
    .replaceAll(/^[-._]+|[-._]+$/g, "");
}

export function splitDatabaseUrlCredentials(databaseUrl: string): DatabaseUrlCredentials {
  try {
    const parsed = new URL(databaseUrl);
    if (parsed.protocol !== "postgresql:" && parsed.protocol !== "postgres:") {
      return {
        connectionUrl: databaseUrl,
        password: null,
        databaseName: "database",
      };
    }

    const password = parsed.password.length > 0
      ? decodeURIComponent(parsed.password)
      : null;

    const rawPath = parsed.pathname.startsWith("/") ? parsed.pathname.slice(1) : parsed.pathname;
    const databaseName = sanitizeSnapshotLabel(decodeURIComponent(rawPath)) || "database";

    parsed.password = "";

    return {
      connectionUrl: parsed.toString(),
      password,
      databaseName,
    };
  } catch {
    return {
      connectionUrl: databaseUrl,
      password: null,
      databaseName: "database",
    };
  }
}

export function buildSnapshotFileName(
  databaseUrl: string,
  at: Date,
  label: string | undefined
): string {
  const credentials = splitDatabaseUrlCredentials(databaseUrl);
  const timestamp = formatUtcTimestamp(at);
  const sanitizedLabel = label ? sanitizeSnapshotLabel(label) : "";
  const suffix = sanitizedLabel.length > 0 ? `-${sanitizedLabel}` : "";

  return `${SNAPSHOT_PREFIX}${credentials.databaseName}-${timestamp}${suffix}${SNAPSHOT_SUFFIX}`;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const kb = bytes / 1024;
  if (kb < 1024) {
    return `${kb.toFixed(1)} KiB`;
  }
  const mb = kb / 1024;
  if (mb < 1024) {
    return `${mb.toFixed(1)} MiB`;
  }
  const gb = mb / 1024;
  return `${gb.toFixed(2)} GiB`;
}

function parseSnapshotConfig(flags: CliFlags): SnapshotConfig {
  const databaseUrl = resolveTopicsDatabaseUrl(flags);
  const outputDir = resolve(
    getStringFlag(flags, "output-dir")
    || getEnvString("POSTGRES_SNAPSHOT_DIR")
    || DEFAULT_OUTPUT_DIR
  );
  const label = getStringFlag(flags, "label");
  const dryRun = getBooleanFlag(flags, "dry-run");
  const loop = getBooleanFlag(flags, "loop");

  const retentionRaw = getStringFlag(flags, "retention-days")
    || getEnvString("POSTGRES_SNAPSHOT_RETENTION_DAYS")
    || String(DEFAULT_RETENTION_DAYS);
  const retentionDays = parseNonNegativeIntegerStrict(retentionRaw, "--retention-days");

  const intervalRaw = getStringFlag(flags, "interval-seconds")
    || getEnvString("POSTGRES_SNAPSHOT_INTERVAL_SECONDS")
    || String(DEFAULT_INTERVAL_SECONDS);
  const intervalSeconds = parsePositiveIntegerStrict(intervalRaw, "--interval-seconds");

  if (dryRun && loop) {
    throw new Error("Cannot combine --dry-run and --loop");
  }

  return {
    databaseUrl,
    outputDir,
    label,
    retentionDays,
    dryRun,
    loop,
    intervalSeconds,
  };
}

async function runPgDump(databaseUrl: string, outputPath: string): Promise<void> {
  const credentials = splitDatabaseUrlCredentials(databaseUrl);
  const args = [
    "--format=custom",
    "--compress=9",
    "--no-owner",
    "--no-privileges",
    "--file",
    outputPath,
    "--dbname",
    credentials.connectionUrl,
  ];

  await new Promise<void>((resolvePromise, rejectPromise) => {
    const child = spawn("pg_dump", args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        ...(credentials.password ? { PGPASSWORD: credentials.password } : {}),
      },
    });

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });

    child.once("error", (error) => {
      const withCode = error as NodeJS.ErrnoException;
      if (withCode.code === "ENOENT") {
        rejectPromise(
          new Error(
            "pg_dump was not found in PATH. Install PostgreSQL client tools or run snapshots via Docker Compose."
          )
        );
        return;
      }
      rejectPromise(error);
    });

    child.once("close", (code) => {
      if (code === 0) {
        resolvePromise();
        return;
      }

      const details = stderr.trim();
      rejectPromise(
        new Error(
          details.length > 0
            ? `pg_dump exited with code ${code}: ${details}`
            : `pg_dump exited with code ${code}`
        )
      );
    });
  });
}

async function pruneOldSnapshots(
  outputDir: string,
  retentionDays: number,
  keepPath: string,
  now: Date
): Promise<string[]> {
  if (retentionDays === 0) {
    return [];
  }

  const cutoffMs = now.getTime() - (retentionDays * 24 * 60 * 60 * 1000);
  const entries = await readdir(outputDir, { withFileTypes: true });
  const removed: string[] = [];

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }

    if (!entry.name.startsWith(SNAPSHOT_PREFIX) || !entry.name.endsWith(SNAPSHOT_SUFFIX)) {
      continue;
    }

    const candidatePath = resolve(outputDir, entry.name);
    if (candidatePath === keepPath) {
      continue;
    }

    const info = await stat(candidatePath);
    if (info.mtimeMs >= cutoffMs) {
      continue;
    }

    await rm(candidatePath, { force: true });
    removed.push(candidatePath);
  }

  return removed;
}

async function runSnapshotOnce(config: SnapshotConfig): Promise<SnapshotRunResult> {
  const startedAt = new Date();
  const fileName = buildSnapshotFileName(config.databaseUrl, startedAt, config.label);
  const outputPath = resolve(config.outputDir, fileName);

  if (config.dryRun) {
    console.log("Postgres snapshot dry run:\n");
    console.log(`  output path: ${outputPath}`);
    console.log(`  retention days: ${config.retentionDays}`);
    console.log(`  format: custom (pg_dump -Fc)`);
    return {
      outputPath,
      outputBytes: 0,
      prunedFiles: [],
    };
  }

  await mkdir(config.outputDir, { recursive: true });

  try {
    await runPgDump(config.databaseUrl, outputPath);
  } catch (error) {
    await rm(outputPath, { force: true });
    throw error;
  }

  const outputInfo = await stat(outputPath);
  const prunedFiles = await pruneOldSnapshots(
    config.outputDir,
    config.retentionDays,
    outputPath,
    startedAt
  );

  return {
    outputPath,
    outputBytes: outputInfo.size,
    prunedFiles,
  };
}

function printSnapshotSummary(result: SnapshotRunResult, config: SnapshotConfig): void {
  console.log("\nPostgres snapshot complete.\n");
  console.log(`  file: ${result.outputPath}`);
  console.log(`  size: ${formatBytes(result.outputBytes)}`);
  console.log(`  retention days: ${config.retentionDays}`);
  console.log(`  pruned files: ${result.prunedFiles.length}`);
  if (result.prunedFiles.length > 0) {
    for (const path of result.prunedFiles) {
      console.log(`    - ${path}`);
    }
  }
}

export async function dbSnapshot(flags: CliFlags): Promise<void> {
  const config = parseSnapshotConfig(flags);
  if (!config.loop) {
    const result = await runSnapshotOnce(config);
    if (!config.dryRun) {
      printSnapshotSummary(result, config);
    }
    return;
  }

  console.log("Starting Postgres snapshot scheduler.\n");
  console.log(`  interval seconds: ${config.intervalSeconds}`);
  console.log(`  output dir: ${config.outputDir}`);
  console.log(`  retention days: ${config.retentionDays}`);
  console.log("");

  while (true) {
    const runStartedAt = new Date();
    try {
      const result = await runSnapshotOnce(config);
      printSnapshotSummary(result, config);
    } catch (error) {
      console.error("\nPostgres snapshot failed.");
      console.error(error);
    }

    const nextRunAt = new Date(runStartedAt.getTime() + (config.intervalSeconds * 1000));
    console.log(`\nNext snapshot at: ${nextRunAt.toISOString()}`);
    await sleep(config.intervalSeconds * 1000);
  }
}
