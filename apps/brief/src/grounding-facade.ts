import { createUrlSafetyFacade } from "@rising-intelligence/shared";
import type pino from "pino";
import { incrementSuspiciousContent, type HealthContext } from "./health.js";
import type { ParsedSummaryRequest } from "./types.js";

const NOTES_URL_PATTERN = /https?:\/\/[^\s<>"'`]+/g;
export const EVIDENCE_EXCERPT_MAX_LENGTH = 2000;
const TITLE_MAX_LENGTH = 200;
const CONTROL_CHAR_PATTERN = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g;
const INSTRUCTION_MARKER_PATTERN = /\[INST\]|\[\/INST\]|\[SYSTEM\]|<<SYS>>|<<\/SYS>>|<\/SYS>>/gi;
const urlSafetyFacade = createUrlSafetyFacade();
const SUSPICIOUS_CONTENT_PATTERNS = [
  /ignore\s+(all\s+)?(previous|prior|above)\s+instructions/i,
  /you\s+are\s+(now\s+)?a\s+(different|new)/i,
  /\[INST\]/i,
  /<<SYS>>/i,
  /system\s*:\s*$/im,
];

export interface SummaryRequestPayloadOptions {
  logger?: pino.Logger;
  healthContext?: HealthContext;
}

export interface SummaryRequestGroundingFacade {
  dedupeCanonicalUrls(values: Array<string | null | undefined>): string[];
  createEvidenceUrlSet(request: ParsedSummaryRequest): Set<string>;
  filterGroundedCitations(citations: string[], evidenceUrls: Set<string>): string[];
  enforceGroundedNotes(
    request: ParsedSummaryRequest,
    notes: string,
    createError: (message: string) => Error
  ): string;
  buildSummaryRequestPayload(
    request: ParsedSummaryRequest,
    options?: SummaryRequestPayloadOptions
  ): Record<string, unknown>;
}

function canonicalizeUrl(value: string): string | null {
  return urlSafetyFacade.canonicalizeHttpUrl(value);
}

function dedupeCanonicalUrls(values: Array<string | null | undefined>): string[] {
  const unique = new Set<string>();
  for (const value of values) {
    if (!value) {
      continue;
    }
    const canonical = canonicalizeUrl(value);
    if (!canonical) {
      continue;
    }
    unique.add(canonical);
  }
  return [...unique];
}

function extractCanonicalUrlsFromText(text: string): string[] {
  const matches = text.match(NOTES_URL_PATTERN);
  if (!matches || matches.length === 0) {
    return [];
  }

  const cleaned = matches.map((value) => value.replace(/[),.;!?]+$/g, ""));
  return dedupeCanonicalUrls(cleaned);
}

function createEvidenceUrlSet(request: ParsedSummaryRequest): Set<string> {
  const urls = request.topics.flatMap((topic) => topic.evidence.map((evidence) => evidence.url));
  return new Set(dedupeCanonicalUrls(urls));
}

function filterGroundedCitations(citations: string[], evidenceUrls: Set<string>): string[] {
  const filtered = dedupeCanonicalUrls(citations);
  return filtered.filter((citation) => evidenceUrls.has(citation));
}

function sanitizeEvidenceTitle(value: string | null): string {
  if (!value || value.trim().length === 0) {
    return "";
  }

  const sanitized = value
    .replace(/[\x00-\x1F\x7F]/g, "")
    .slice(0, TITLE_MAX_LENGTH)
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(INSTRUCTION_MARKER_PATTERN, "")
    .trim();
  return sanitized.length > 0 ? sanitized : "[No title]";
}

function sanitizeEvidenceExcerpt(value: string | null): string {
  if (!value || value.trim().length === 0) {
    return "";
  }

  const sanitized = value
    .replace(CONTROL_CHAR_PATTERN, "")
    .replace(/\s{3,}/g, "  ")
    .slice(0, EVIDENCE_EXCERPT_MAX_LENGTH)
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(INSTRUCTION_MARKER_PATTERN, "")
    .trim();
  return sanitized.length > 0 ? sanitized : "[Content removed]";
}

function findSuspiciousContentPattern(title: string | null, excerpt: string | null): string | null {
  const text = [title ?? "", excerpt ?? ""].join("\n").trim();
  if (text.length === 0) {
    return null;
  }

  for (const pattern of SUSPICIOUS_CONTENT_PATTERNS) {
    if (pattern.test(text)) {
      return pattern.source;
    }
  }
  return null;
}

export function createSummaryRequestGroundingFacade(): SummaryRequestGroundingFacade {
  return {
    dedupeCanonicalUrls,
    createEvidenceUrlSet,
    filterGroundedCitations,
    enforceGroundedNotes(request, notes, createError) {
      const noteUrls = extractCanonicalUrlsFromText(notes);
      if (noteUrls.length === 0) {
        return notes;
      }

      const evidenceUrls = createEvidenceUrlSet(request);
      if (evidenceUrls.size === 0) {
        throw createError("No evidence URLs were provided in the summary request");
      }

      const ungrounded = noteUrls.filter((url) => !evidenceUrls.has(url));
      if (ungrounded.length > 0) {
        throw createError(
          `Brief notes contained ungrounded URL citations: ${ungrounded.slice(0, 3).join(", ")}`
        );
      }

      return notes;
    },
    buildSummaryRequestPayload(request, options = {}) {
      const { logger, healthContext } = options;

      return {
        request_id: request.requestId,
        requested_at: request.requestedAt.toISOString(),
        type: request.type,
        windows: request.windows,
        topics: request.topics.map((topic) => ({
          topic: topic.topic,
          metrics: topic.metrics.map((metric) => ({
            topic: metric.topic,
            window: metric.window,
            score: metric.score,
            volume: metric.volume,
            acceleration: metric.acceleration,
          })),
          evidence: topic.evidence.map((evidence) => {
            const suspiciousPattern = findSuspiciousContentPattern(evidence.title, evidence.textExcerpt);
            if (suspiciousPattern) {
              logger?.warn(
                {
                  requestId: request.requestId,
                  topic: topic.topic,
                  eventId: evidence.eventId,
                  pattern: suspiciousPattern,
                },
                "Suspicious prompt-like content detected in evidence"
              );
              if (healthContext) {
                incrementSuspiciousContent(healthContext);
              }
            }

            return {
              event_id: evidence.eventId,
              source: evidence.source,
              url: evidence.url ? canonicalizeUrl(evidence.url) ?? "" : "",
              title: sanitizeEvidenceTitle(evidence.title),
              published_at: evidence.publishedAt ? evidence.publishedAt.toISOString() : "",
              fetched_at: evidence.fetchedAt ? evidence.fetchedAt.toISOString() : "",
              text_excerpt: sanitizeEvidenceExcerpt(evidence.textExcerpt),
            };
          }),
        })),
        budget: request.budget
          ? {
              daily_budget_usd: request.budget.dailyBudgetUsd,
              max_topics: request.budget.maxTopics,
              max_evidence_per_topic: request.budget.maxEvidencePerTopic,
              max_output_tokens: request.budget.maxOutputTokens,
            }
          : null,
        query: request.query
          ? {
              lookback_days: request.query.lookbackDays,
              topic_globs: request.query.topicGlobs,
              max_events_per_topic: request.query.maxEventsPerTopic,
              evidence_strategy: request.query.evidenceStrategy,
            }
          : null,
        report: request.report
          ? {
              timezone: request.report.timezone,
              start_at: request.report.startAt ? request.report.startAt.toISOString() : undefined,
              end_at: request.report.endAt ? request.report.endAt.toISOString() : undefined,
            }
          : null,
      };
    },
  };
}
