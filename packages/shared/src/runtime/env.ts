import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";

import { REPO_ROOT } from "./paths.js";

export type LoadDotEnvOptions = {
  path?: string;
  override?: boolean;
};

function parseDotEnv(contents: string): Record<string, string> {
  const result: Record<string, string> = {};

  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line.length === 0 || line.startsWith("#")) {
      continue;
    }

    const withoutExport = line.startsWith("export ") ? line.slice("export ".length) : line;
    const equalsIndex = withoutExport.indexOf("=");
    if (equalsIndex <= 0) {
      continue;
    }

    const key = withoutExport.slice(0, equalsIndex).trim();
    if (!key) {
      continue;
    }

    let value = withoutExport.slice(equalsIndex + 1).trim();

    const isDoubleQuoted = value.startsWith("\"") && value.endsWith("\"") && value.length >= 2;
    const isSingleQuoted = value.startsWith("'") && value.endsWith("'") && value.length >= 2;
    if (isDoubleQuoted || isSingleQuoted) {
      value = value.slice(1, -1);
    }

    if (isDoubleQuoted) {
      value = value
        .replaceAll("\\n", "\n")
        .replaceAll("\\r", "\r")
        .replaceAll("\\t", "\t")
        .replaceAll("\\\"", "\"")
        .replaceAll("\\\\", "\\");
    }

    result[key] = value;
  }

  return result;
}

export function loadDotEnv(options: LoadDotEnvOptions = {}) {
  const envPath = options.path || process.env.RI_ENV_PATH || resolve(REPO_ROOT, ".env");

  if (!existsSync(envPath)) {
    return { loaded: false as const, path: envPath };
  }

  const contents = readFileSync(envPath, "utf-8");
  const parsed = parseDotEnv(contents);

  for (const [key, value] of Object.entries(parsed)) {
    if (!options.override && process.env[key] !== undefined) {
      continue;
    }
    process.env[key] = value;
  }

  return { loaded: true as const, path: envPath, keys: Object.keys(parsed) };
}

export type EnvStringOptions = {
  defaultValue?: string;
  required?: boolean;
  allowEmpty?: boolean;
};

export function getEnvString(name: string, options: EnvStringOptions = {}): string | undefined {
  const raw = process.env[name];
  const value = raw ?? options.defaultValue;

  if (value === undefined) {
    if (options.required) {
      throw new Error(`Missing required env var: ${name}`);
    }
    return undefined;
  }

  if (!options.allowEmpty && value.trim().length === 0) {
    if (options.required) {
      throw new Error(`Empty required env var: ${name}`);
    }
    return undefined;
  }

  return value;
}

export function requireEnvString(name: string, options: Omit<EnvStringOptions, "required"> = {}) {
  return getEnvString(name, { ...options, required: true });
}

