import type { CliFlags } from "./args.js";

export function getStringFlag(flags: CliFlags, name: string): string | undefined {
  const value = flags[name];
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    const last = value[value.length - 1];
    return typeof last === "string" ? last : undefined;
  }
  return undefined;
}

export function getBooleanFlag(flags: CliFlags, name: string): boolean {
  return flags[name] === true;
}

export function parseKafkaBrokers(rawValue: string): string[] {
  const brokers = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS resolved to an empty value");
  }

  return brokers;
}
