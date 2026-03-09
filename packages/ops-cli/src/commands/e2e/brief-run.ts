import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { REPO_ROOT } from "@rising-intelligence/shared/paths";
import type { CliFlags } from "../../lib/args.js";
import { getBooleanFlag, getStringFlag } from "../../lib/flags.js";
import { parsePositiveIntegerStrict } from "../../lib/number.js";

interface E2eBriefRunConfig {
  scriptPath: string;
  composeProject: string;
  kafkaHostPort?: number;
  schemaRegistryHostPort?: number;
  briefHostPort?: number;
  redisHostPort?: number;
  postgresHostPort?: number;
  mockLlmHostPort?: number;
  waitTimeoutMs?: number;
  keepUp: boolean;
  dryRun: boolean;
}

function parseOptionalInteger(flags: CliFlags, name: string): number | undefined {
  const raw = getStringFlag(flags, name);
  if (raw === undefined) {
    return undefined;
  }
  return parsePositiveIntegerStrict(raw, `--${name}`);
}

export function resolveE2eBriefRunConfig(flags: CliFlags): E2eBriefRunConfig {
  return {
    scriptPath:
      getStringFlag(flags, "script-path") || resolve(REPO_ROOT, "scripts", "test-e2e-brief.sh"),
    composeProject: getStringFlag(flags, "compose-project") || "ri-brief-e2e",
    kafkaHostPort: parseOptionalInteger(flags, "kafka-host-port"),
    schemaRegistryHostPort: parseOptionalInteger(flags, "schema-registry-host-port"),
    briefHostPort: parseOptionalInteger(flags, "brief-host-port"),
    redisHostPort: parseOptionalInteger(flags, "redis-host-port"),
    postgresHostPort: parseOptionalInteger(flags, "postgres-host-port"),
    mockLlmHostPort: parseOptionalInteger(flags, "mock-llm-host-port"),
    waitTimeoutMs: parseOptionalInteger(flags, "wait-timeout-ms"),
    keepUp: getBooleanFlag(flags, "keep-up"),
    dryRun: getBooleanFlag(flags, "dry-run"),
  };
}

async function runScript(config: E2eBriefRunConfig): Promise<void> {
  await new Promise<void>((resolvePromise, rejectPromise) => {
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      E2E_BRIEF_COMPOSE_PROJECT: config.composeProject,
    };
    if (config.kafkaHostPort !== undefined) {
      env.E2E_KAFKA_HOST_PORT = String(config.kafkaHostPort);
    }
    if (config.schemaRegistryHostPort !== undefined) {
      env.E2E_SCHEMA_REGISTRY_HOST_PORT = String(config.schemaRegistryHostPort);
    }
    if (config.briefHostPort !== undefined) {
      env.E2E_BRIEF_HOST_PORT = String(config.briefHostPort);
    }
    if (config.redisHostPort !== undefined) {
      env.E2E_REDIS_HOST_PORT = String(config.redisHostPort);
    }
    if (config.postgresHostPort !== undefined) {
      env.E2E_POSTGRES_HOST_PORT = String(config.postgresHostPort);
    }
    if (config.mockLlmHostPort !== undefined) {
      env.E2E_MOCK_LLM_HOST_PORT = String(config.mockLlmHostPort);
    }
    if (config.waitTimeoutMs !== undefined) {
      env.E2E_WAIT_TIMEOUT_MS = String(config.waitTimeoutMs);
    }
    if (config.keepUp) {
      env.E2E_KEEP_UP = "true";
    }

    const child = spawn("/bin/bash", [config.scriptPath], {
      cwd: REPO_ROOT,
      env,
      stdio: "inherit",
    });

    child.once("error", (error) => {
      rejectPromise(error);
    });

    child.once("exit", (code, signal) => {
      if (signal) {
        rejectPromise(new Error(`e2e script terminated by signal: ${signal}`));
        return;
      }
      if (code !== 0) {
        rejectPromise(new Error(`e2e script exited with code ${code ?? "unknown"}`));
        return;
      }
      resolvePromise();
    });
  });
}

export async function e2eBriefRun(flags: CliFlags): Promise<void> {
  const config = resolveE2eBriefRunConfig(flags);
  if (!existsSync(config.scriptPath)) {
    throw new Error(`E2E script not found: ${config.scriptPath}`);
  }

  if (config.dryRun) {
    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(
        {
          scriptPath: config.scriptPath,
          composeProject: config.composeProject,
          keepUp: config.keepUp,
          kafkaHostPort: config.kafkaHostPort,
          schemaRegistryHostPort: config.schemaRegistryHostPort,
          briefHostPort: config.briefHostPort,
          redisHostPort: config.redisHostPort,
          postgresHostPort: config.postgresHostPort,
          mockLlmHostPort: config.mockLlmHostPort,
          waitTimeoutMs: config.waitTimeoutMs,
        },
        null,
        2
      )
    );
    return;
  }

  await runScript(config);
}
