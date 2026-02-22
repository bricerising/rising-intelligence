import { parseCanonicalSource } from "@rising-intelligence/shared";
import type { CollectorHeartbeatState } from "./health.js";

const COLLECTOR_STATUS_BY_NUMBER = new Map<number, CollectorHeartbeatState["status"]>([
  [1, "healthy"],
  [2, "degraded"],
  [3, "error"],
]);

const COLLECTOR_STATUS_BY_STRING = new Map<string, CollectorHeartbeatState["status"]>([
  ["healthy", "healthy"],
  ["collector_status_healthy", "healthy"],
  ["degraded", "degraded"],
  ["collector_status_degraded", "degraded"],
  ["error", "error"],
  ["collector_status_error", "error"],
]);

function parseIsoDate(value: unknown, field: string): Date {
  if (typeof value !== "string") {
    throw new Error(`collector heartbeat ${field} must be a string`);
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`collector heartbeat ${field} is invalid: ${value}`);
  }
  return parsed;
}

function parseCollectorStatusByNumber(
  value: number
): CollectorHeartbeatState["status"] | null {
  return COLLECTOR_STATUS_BY_NUMBER.get(value) ?? null;
}

function parseCollectorStatus(value: unknown): CollectorHeartbeatState["status"] {
  if (typeof value === "number") {
    const status = parseCollectorStatusByNumber(value);
    if (status) {
      return status;
    }
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    const statusByName = COLLECTOR_STATUS_BY_STRING.get(normalized);
    if (statusByName) {
      return statusByName;
    }

    const asNumber = Number.parseInt(normalized, 10);
    if (Number.isInteger(asNumber) && `${asNumber}` === normalized) {
      const statusByNumber = parseCollectorStatusByNumber(asNumber);
      if (statusByNumber) {
        return statusByNumber;
      }
    }
  }

  throw new Error(`Unsupported collector heartbeat status: ${String(value)}`);
}

function parseNonNegativeInteger(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.floor(value));
  }

  if (typeof value === "string") {
    const normalized = value.trim();
    if (/^\d+$/.test(normalized)) {
      return Number.parseInt(normalized, 10);
    }
  }

  return fallback;
}

function parseCollectorSource(value: unknown): string {
  if (typeof value !== "number" && typeof value !== "string") {
    throw new Error("collector heartbeat source must be a string or number");
  }

  return parseCanonicalSource(value);
}

function parseCollectorHeartbeatRecord(decoded: unknown): CollectorHeartbeatState {
  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
    throw new Error("Collector heartbeat payload must be an object");
  }

  const heartbeat = decoded as Record<string, unknown>;
  const source = parseCollectorSource(heartbeat.source);
  const status = parseCollectorStatus(heartbeat.status);
  const timestamp = parseIsoDate(heartbeat.timestamp, "timestamp");
  const lastFetchAt = parseIsoDate(heartbeat.last_fetch_at, "last_fetch_at");
  const itemsFetched = parseNonNegativeInteger(heartbeat.items_fetched, 0);
  const errorMessage =
    typeof heartbeat.error_message === "string" && heartbeat.error_message.trim().length > 0
      ? heartbeat.error_message
      : undefined;

  return {
    source,
    status,
    timestamp,
    lastFetchAt,
    itemsFetched,
    errorMessage,
  };
}

export function deserializeCollectorHeartbeat(messageValue: Buffer): CollectorHeartbeatState {
  try {
    return parseCollectorHeartbeatRecord(JSON.parse(messageValue.toString("utf-8")));
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error(`Invalid collector heartbeat JSON: ${error.message}`);
    }
    throw error;
  }
}
