import { readFile } from "node:fs/promises";

import {
  CONTRACT_REFERENCE_NAME,
  CONTRACTS_PROTO_PATH,
  CONTRACTS_SUBJECT,
  GRPC_SUBJECT,
  KAFKA_VALUE_SUBJECTS,
  SERVICES_PROTO_PATH,
  getEnvString,
} from "@rising-intelligence/shared";

type SchemaReference = {
  name: string;
  subject: string;
  version: number;
};

type SchemaRegistryConfig = {
  schemaRegistryUrl: string;
  compatibility: string;
  dryRun: boolean;
  retries: number;
  retryInitialDelayMs: number;
  retryMaxDelayMs: number;
  timeoutMs: number;
};

class SchemaRegistryHttpError extends Error {
  status: number;
  body: string;

  constructor(status: number, body: string) {
    super(`Schema Registry error ${status}: ${body}`);
    this.status = status;
    this.body = body;
  }
}

function normalizeUrl(url: string) {
  return url.endsWith("/") ? url.slice(0, -1) : url;
}

function getStringFlag(
  flags: Record<string, string | boolean>,
  name: string,
): string | undefined {
  const value = flags[name];
  if (typeof value === "string") {
    return value;
  }
  return undefined;
}

function getNumberFlag(
  flags: Record<string, string | boolean>,
  name: string,
): number | undefined {
  const value = getStringFlag(flags, name);
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric flag: --${name}=${value}`);
  }
  return parsed;
}

function getBooleanFlag(flags: Record<string, string | boolean>, name: string): boolean {
  return flags[name] === true;
}

function parseNumberEnv(name: string, rawValue: string | undefined, defaultValue: number): number {
  if (rawValue === undefined) {
    return defaultValue;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric env var: ${name}=${rawValue}`);
  }

  return parsed;
}

function resolveConfig(flags: Record<string, string | boolean>): SchemaRegistryConfig {
  const schemaRegistryUrl =
    getStringFlag(flags, "schema-registry-url") ||
    getEnvString("SCHEMA_REGISTRY_URL") ||
    "http://localhost:8081";

  const compatibility =
    getStringFlag(flags, "compatibility") || getEnvString("SCHEMA_COMPATIBILITY") || "BACKWARD";

  const dryRun = getBooleanFlag(flags, "dry-run");

  const retries =
    getNumberFlag(flags, "retries") ??
    parseNumberEnv("SCHEMA_REGISTRY_RETRIES", getEnvString("SCHEMA_REGISTRY_RETRIES"), 10);
  const retryInitialDelayMs =
    getNumberFlag(flags, "retry-initial-ms") ??
    parseNumberEnv(
      "SCHEMA_REGISTRY_RETRY_INITIAL_MS",
      getEnvString("SCHEMA_REGISTRY_RETRY_INITIAL_MS"),
      250,
    );
  const retryMaxDelayMs =
    getNumberFlag(flags, "retry-max-ms") ??
    parseNumberEnv(
      "SCHEMA_REGISTRY_RETRY_MAX_MS",
      getEnvString("SCHEMA_REGISTRY_RETRY_MAX_MS"),
      5000,
    );
  const timeoutMs =
    getNumberFlag(flags, "timeout-ms") ??
    parseNumberEnv("SCHEMA_REGISTRY_TIMEOUT_MS", getEnvString("SCHEMA_REGISTRY_TIMEOUT_MS"), 8000);

  return {
    schemaRegistryUrl: normalizeUrl(schemaRegistryUrl),
    compatibility,
    dryRun,
    retries,
    retryInitialDelayMs,
    retryMaxDelayMs,
    timeoutMs,
  };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableError(error: unknown): boolean {
  if (error instanceof SchemaRegistryHttpError) {
    return (
      error.status === 408 ||
      error.status === 429 ||
      (error.status >= 500 && error.status <= 599)
    );
  }

  if (error instanceof Error) {
    // Network errors from fetch are typically TypeError("fetch failed") or similar.
    return true;
  }

  return false;
}

async function withRetry<T>(
  config: SchemaRegistryConfig,
  opName: string,
  fn: () => Promise<T>,
): Promise<T> {
  let delayMs = config.retryInitialDelayMs;

  for (let attempt = 0; attempt <= config.retries; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      const isLastAttempt = attempt >= config.retries;
      const retryable = isRetryableError(error);

      if (isLastAttempt || !retryable) {
        throw error;
      }

      const jitter = Math.floor(Math.random() * 100);
      const waitMs = Math.min(config.retryMaxDelayMs, delayMs) + jitter;

      // eslint-disable-next-line no-console
      console.warn(
        `${opName} failed (attempt ${attempt + 1}/${config.retries + 1}); retrying in ${waitMs}ms`,
      );

      await sleep(waitMs);
      delayMs = Math.min(config.retryMaxDelayMs, delayMs * 2);
    }
  }

  // Unreachable
  throw new Error(`${opName} failed after retries`);
}

async function srRequest(
  config: SchemaRegistryConfig,
  path: string,
  init?: RequestInit,
): Promise<Response> {
  const url = `${config.schemaRegistryUrl}${path}`;
  const headers = new Headers(init?.headers);
  headers.set("Content-Type", "application/vnd.schemaregistry.v1+json");

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.timeoutMs);
  try {
    return await fetch(url, { ...init, headers, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function srJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new SchemaRegistryHttpError(response.status, body);
  }
  return (await response.json()) as T;
}

async function setGlobalCompatibility(config: SchemaRegistryConfig) {
  if (config.dryRun) {
    // eslint-disable-next-line no-console
    console.log(`[dry-run] PUT /config compatibility=${config.compatibility}`);
    return;
  }

  await withRetry(config, "schema-registry:set-compatibility", async () => {
    const response = await srRequest(config, "/config", {
      method: "PUT",
      body: JSON.stringify({ compatibility: config.compatibility }),
    });
    await srJson(response);
  });
}

type LatestSchemaResponse = {
  subject: string;
  version: number;
  id: number;
  schemaType?: string;
  references?: SchemaReference[];
  schema: string;
};

function normalizeReferences(references: SchemaReference[] | undefined): SchemaReference[] {
  if (!references) {
    return [];
  }

  return [...references].sort((a, b) => {
    const nameCompare = a.name.localeCompare(b.name);
    if (nameCompare !== 0) {
      return nameCompare;
    }
    const subjectCompare = a.subject.localeCompare(b.subject);
    if (subjectCompare !== 0) {
      return subjectCompare;
    }
    return a.version - b.version;
  });
}

function schemasMatch(
  latest: LatestSchemaResponse,
  schema: string,
  references: SchemaReference[] | undefined,
): boolean {
  const latestSchemaType = latest.schemaType ?? "PROTOBUF";
  if (latestSchemaType !== "PROTOBUF") {
    return false;
  }

  if (latest.schema !== schema) {
    return false;
  }

  const a = normalizeReferences(latest.references);
  const b = normalizeReferences(references);
  if (a.length !== b.length) {
    return false;
  }

  for (let i = 0; i < a.length; i += 1) {
    if (a[i].name !== b[i].name || a[i].subject !== b[i].subject || a[i].version !== b[i].version) {
      return false;
    }
  }

  return true;
}

async function getLatestSchema(
  config: SchemaRegistryConfig,
  subject: string,
) {
  if (config.dryRun) {
    return null;
  }

  return withRetry(config, `schema-registry:get-latest:${subject}`, async () => {
    const response = await srRequest(config, `/subjects/${subject}/versions/latest`, {
      method: "GET",
    });

    if (response.status === 404) {
      return null;
    }

    return await srJson<LatestSchemaResponse>(response);
  });
}

async function registerSubject(
  config: SchemaRegistryConfig,
  subject: string,
  schema: string,
  references?: SchemaReference[],
) {
  if (config.dryRun) {
    // eslint-disable-next-line no-console
    console.log(
      `[dry-run] POST /subjects/${subject}/versions schemaType=PROTOBUF refs=${references?.length ?? 0}`,
    );
    return;
  }

  const payload: Record<string, unknown> = {
    schemaType: "PROTOBUF",
    schema,
  };
  if (references && references.length > 0) {
    payload.references = references;
  }

  await withRetry(config, `schema-registry:register:${subject}`, async () => {
    const response = await srRequest(config, `/subjects/${subject}/versions`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    await srJson<{ id: number }>(response);
  });
}

async function latestVersion(config: SchemaRegistryConfig, subject: string): Promise<number> {
  if (config.dryRun) {
    return 1;
  }

  return await withRetry(config, `schema-registry:latest-version:${subject}`, async () => {
    const latest = await getLatestSchema(config, subject);
    if (!latest) {
      throw new Error(`Subject not found: ${subject}`);
    }
    return latest.version;
  });
}

async function ensureSubject(
  config: SchemaRegistryConfig,
  subject: string,
  schema: string,
  references?: SchemaReference[],
): Promise<{ changed: boolean; version: number }> {
  if (config.dryRun) {
    // eslint-disable-next-line no-console
    console.log(
      `[dry-run] ensure subject=${subject} schemaType=PROTOBUF refs=${references?.length ?? 0}`,
    );
    return { changed: true, version: 1 };
  }

  const latest = await getLatestSchema(config, subject);
  if (latest && schemasMatch(latest, schema, references)) {
    // eslint-disable-next-line no-console
    console.log(`Subject up-to-date: ${subject} (v${latest.version})`);
    return { changed: false, version: latest.version };
  }

  await registerSubject(config, subject, schema, references);
  const version = await latestVersion(config, subject);
  // eslint-disable-next-line no-console
  console.log(`Subject updated: ${subject} (v${version})`);
  return { changed: true, version };
}

export async function schemaRegistryPublishProtos(flags: Record<string, string | boolean>) {
  const config = resolveConfig(flags);

  // eslint-disable-next-line no-console
  console.log(`Schema Registry: ${config.schemaRegistryUrl}`);

  await setGlobalCompatibility(config);

  const contractsSchema = await readFile(CONTRACTS_PROTO_PATH, "utf-8");
  const contractsResult = await ensureSubject(config, CONTRACTS_SUBJECT, contractsSchema);

  const servicesSchema = await readFile(SERVICES_PROTO_PATH, "utf-8");
  await ensureSubject(config, GRPC_SUBJECT, servicesSchema, [
    {
      name: CONTRACT_REFERENCE_NAME,
      subject: CONTRACTS_SUBJECT,
      version: contractsResult.version,
    },
  ]);

  for (const subject of KAFKA_VALUE_SUBJECTS) {
    await ensureSubject(config, subject, contractsSchema);
  }

  // eslint-disable-next-line no-console
  console.log("Done.");
}
