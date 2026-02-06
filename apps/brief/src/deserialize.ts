import { z } from "zod";
import type { ParsedSummaryRequest, SummaryRequestType } from "./types.js";

const SummaryRequestWireSchema = z.object({
  request_id: z.string().min(1),
  requested_at: z.string().min(1),
  type: z.union([z.number().int(), z.string().min(1)]),
  windows: z.array(z.union([z.number().int(), z.string()])).optional(),
  topics: z
    .array(
      z.object({
        topic: z.string().min(1),
        evidence: z.array(z.unknown()).optional(),
      })
    )
    .optional(),
});

function parseRequestedAt(value: string): Date {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid requested_at: ${value}`);
  }
  return parsed;
}

function parseTrendWindow(value: number | string): number {
  const SUPPORTED_TREND_WINDOWS = new Set([1, 2, 3]);

  if (typeof value === "number") {
    if (Number.isInteger(value) && SUPPORTED_TREND_WINDOWS.has(value)) {
      return value;
    }
    throw new Error(`Unsupported trend window enum: ${value}`);
  }

  const normalized = value.trim().toLowerCase();
  switch (normalized) {
    case "trend_window_15m":
      return 1;
    case "trend_window_60m":
      return 2;
    case "trend_window_24h":
      return 3;
    default:
      break;
  }

  const asNumber = Number.parseInt(normalized, 10);
  if (
    !Number.isNaN(asNumber) &&
    `${asNumber}` === normalized &&
    SUPPORTED_TREND_WINDOWS.has(asNumber)
  ) {
    return asNumber;
  }

  throw new Error(`Unsupported trend window value: ${value}`);
}

export function parseSummaryRequestType(value: number | string): SummaryRequestType {
  if (typeof value === "number") {
    if (value === 1) {
      return "daily";
    }
    if (value === 2) {
      return "threshold";
    }
    throw new Error(`Unsupported summary request type enum: ${value}`);
  }

  const normalized = value.trim().toLowerCase();
  switch (normalized) {
    case "summary_request_type_daily":
    case "daily":
      return "daily";
    case "summary_request_type_threshold":
    case "threshold":
      return "threshold";
    default:
      throw new Error(`Unsupported summary request type value: ${value}`);
  }
}

export function deserializeSummaryRequest(messageValue: Buffer): ParsedSummaryRequest {
  let decoded: unknown;
  try {
    decoded = JSON.parse(messageValue.toString("utf-8"));
  } catch (error) {
    throw new Error(`Invalid JSON payload: ${(error as Error).message}`);
  }

  const wire = SummaryRequestWireSchema.parse(decoded);

  return {
    requestId: wire.request_id,
    requestedAt: parseRequestedAt(wire.requested_at),
    type: parseSummaryRequestType(wire.type),
    windows: (wire.windows ?? []).map((window) => parseTrendWindow(window)),
    topics: (wire.topics ?? []).map((topic) => ({
      topic: topic.topic,
      evidenceCount: topic.evidence?.length ?? 0,
    })),
  };
}
