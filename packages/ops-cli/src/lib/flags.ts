import type { CliFlags } from "./args.js";

export function getStringFlag(flags: CliFlags, name: string): string | undefined {
  const value = flags[name];
  return typeof value === "string" ? value : undefined;
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

