import { isIP } from "node:net";
import type pino from "pino";
import { incrementSuspiciousContent, type HealthContext } from "./health.js";
import type { ParsedSummaryRequest } from "./types.js";

const NOTES_URL_PATTERN = /https?:\/\/[^\s<>"'`]+/g;
export const EVIDENCE_EXCERPT_MAX_LENGTH = 2000;
const TITLE_MAX_LENGTH = 200;
const CONTROL_CHAR_PATTERN = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g;
const INSTRUCTION_MARKER_PATTERN = /\[INST\]|\[\/INST\]|\[SYSTEM\]|<<SYS>>|<\/SYS>>/gi;
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

function isPrivateOrLoopbackIpv4(hostname: string): boolean {
  const parts = hostname.split(".");
  if (parts.length !== 4) {
    return false;
  }

  const octets = parts.map((part) => Number.parseInt(part, 10));
  if (octets.some((octet) => Number.isNaN(octet) || octet < 0 || octet > 255)) {
    return false;
  }

  return (
    octets[0] === 10 ||
    octets[0] === 127 ||
    (octets[0] === 169 && octets[1] === 254) ||
    (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) ||
    (octets[0] === 192 && octets[1] === 168)
  );
}

function isPrivateOrLoopbackIpv6(hostname: string): boolean {
  const normalized = hostname.toLowerCase();
  if (normalized === "::1") {
    return true;
  }
  if (normalized.startsWith("::ffff:")) {
    return isPrivateOrLoopbackIpv4(normalized.slice("::ffff:".length));
  }
  if (normalized.startsWith("fc") || normalized.startsWith("fd")) {
    return true;
  }

  const firstHextet = normalized.split(":")[0];
  return /^fe[89ab][0-9a-f]{0,2}$/i.test(firstHextet);
}

function isDisallowedHostname(hostname: string): boolean {
  const normalized = hostname.toLowerCase();
  if (
    normalized === "localhost" ||
    normalized.endsWith(".localhost") ||
    normalized === "0.0.0.0"
  ) {
    return true;
  }

  const ipVersion = isIP(normalized);
  if (ipVersion === 4) {
    return isPrivateOrLoopbackIpv4(normalized);
  }
  if (ipVersion === 6) {
    return isPrivateOrLoopbackIpv6(normalized);
  }
  return false;
}

function canonicalizeUrl(value: string): string | null {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }

  try {
    const url = new URL(trimmed);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return null;
    }
    if (isDisallowedHostname(url.hostname)) {
      return null;
    }
    url.hash = "";
    url.hostname = url.hostname.toLowerCase();
    if (
      (url.protocol === "http:" && url.port === "80") ||
      (url.protocol === "https:" && url.port === "443")
    ) {
      url.port = "";
    }
    if (url.pathname.length > 1 && url.pathname.endsWith("/")) {
      url.pathname = url.pathname.slice(0, -1);
    }
    return url.toString();
  } catch {
    return null;
  }
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
