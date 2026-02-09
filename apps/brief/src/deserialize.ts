import { z } from "zod";
import { parseCanonicalSource } from "@rising-intelligence/shared";
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
        metrics: z
          .array(
            z.object({
              topic: z.string().min(1).optional(),
              window: z.union([z.number().int(), z.string()]).optional(),
              score: z.number().optional(),
              volume: z.number().optional(),
              acceleration: z.number().optional(),
            })
          )
          .optional(),
        evidence: z
          .array(
            z.object({
              event_id: z.string().min(1).optional(),
              source: z.union([z.number().int(), z.string().min(1)]).optional(),
              url: z.string().optional(),
              title: z.string().optional(),
              published_at: z.string().optional(),
              fetched_at: z.string().optional(),
              text_excerpt: z.string().optional(),
            })
          )
          .optional(),
      })
    )
    .optional(),
  budget: z
    .object({
      daily_budget_usd: z.number().nonnegative().optional(),
      max_topics: z.number().int().positive().optional(),
      max_evidence_per_topic: z.number().int().positive().optional(),
      max_output_tokens: z.number().int().positive().optional(),
    })
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

function parseOptionalDate(value: string | undefined, field: string): Date | null {
  if (!value || value.trim().length === 0) {
    return null;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ${field}: ${value}`);
  }
  return parsed;
}

function parseEvidenceSource(value: number | string | undefined): string {
  if (value === undefined) {
    throw new Error("Missing evidence source");
  }

  return parseCanonicalSource(value);
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
    budget: wire.budget
      ? {
          dailyBudgetUsd: wire.budget.daily_budget_usd,
          maxTopics: wire.budget.max_topics,
          maxEvidencePerTopic: wire.budget.max_evidence_per_topic,
          maxOutputTokens: wire.budget.max_output_tokens,
        }
      : null,
    topics: (wire.topics ?? []).map((topic) => {
      const parsedTopic = topic.topic.trim();
      return {
        topic: parsedTopic,
        metrics: (topic.metrics ?? []).map((metric) => ({
          topic: metric.topic?.trim() || parsedTopic,
          window: metric.window ? parseTrendWindow(metric.window) : 0,
          score: metric.score ?? 0,
          volume: metric.volume ?? 0,
          acceleration: metric.acceleration ?? 0,
        })),
        evidence: (topic.evidence ?? []).map((evidence) => ({
          eventId: evidence.event_id?.trim() ?? "",
          source: parseEvidenceSource(evidence.source),
          url: evidence.url?.trim() || null,
          title: evidence.title?.trim() || null,
          publishedAt: parseOptionalDate(evidence.published_at, "published_at"),
          fetchedAt: parseOptionalDate(evidence.fetched_at, "fetched_at"),
          textExcerpt: evidence.text_excerpt?.trim() || null,
        })),
      };
    }),
  };
}
