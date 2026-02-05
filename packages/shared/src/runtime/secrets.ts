import { readFileSync } from "node:fs";

export type Secret = {
  name: string;
  value: string;
  source: "env" | "file";
};

export type SecretOptions = {
  required?: boolean;
  allowEmpty?: boolean;
};

function readSecretFile(path: string): string {
  // Common convention for secret files is a trailing newline.
  return readFileSync(path, "utf-8").trimEnd();
}

export function getSecret(name: string, options: SecretOptions = {}): Secret | undefined {
  const envValue = process.env[name];
  if (envValue !== undefined) {
    if (!options.allowEmpty && envValue.trim().length === 0) {
      if (options.required) {
        throw new Error(`Empty required secret env var: ${name}`);
      }
      return undefined;
    }

    return { name, value: envValue, source: "env" };
  }

  const fileVar = `${name}_FILE`;
  const filePath = process.env[fileVar];
  if (filePath !== undefined) {
    const fileValue = readSecretFile(filePath);
    if (!options.allowEmpty && fileValue.trim().length === 0) {
      if (options.required) {
        throw new Error(`Empty required secret file var: ${fileVar} (${filePath})`);
      }
      return undefined;
    }

    return { name, value: fileValue, source: "file" };
  }

  if (options.required) {
    throw new Error(`Missing required secret: ${name} (or ${fileVar})`);
  }

  return undefined;
}

export function requireSecret(name: string, options: Omit<SecretOptions, "required"> = {}) {
  return getSecret(name, { ...options, required: true });
}

export function getSecretValue(name: string, options: SecretOptions = {}) {
  return getSecret(name, options)?.value;
}

