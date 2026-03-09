import { createUrlSafetyFacade } from "@rising-intelligence/shared/http";
import type pino from "pino";
import { incrementSuspiciousContent, type HealthContext } from "./health.js";
import type {
  ParsedSummaryEvidence,
  ParsedSummaryRequest,
  ParsedSummaryTopic,
} from "./types.js";

const NOTES_URL_PATTERN = /https?:\/\/[^\s<>"'`]+/g;
export const EVIDENCE_EXCERPT_MAX_LENGTH = 2000;
const TARGET_TOTAL_EVIDENCE_EXCERPT_CHARS = 10_000;
const MIN_EVIDENCE_EXCERPT_LENGTH = 160;
const TITLE_MAX_LENGTH = 200;
const EXCERPT_CONTROL_CHAR_PATTERN = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g;
const TITLE_CONTROL_CHAR_PATTERN = /[\x00-\x1F\x7F]/g;
const INSTRUCTION_MARKER_PATTERN = /\[INST\]|\[\/INST\]|\[SYSTEM\]|<<SYS>>|<<\/SYS>>|<\/SYS>>/gi;
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

interface CitationGroundingAdapter {
  canonicalizeUrl(value: string): string | null;
  dedupeCanonicalUrls(values: Array<string | null | undefined>): string[];
  createEvidenceUrlSet(request: ParsedSummaryRequest): Set<string>;
  filterGroundedCitations(citations: string[], evidenceUrls: Set<string>): string[];
  enforceGroundedNotes(
    request: ParsedSummaryRequest,
    notes: string,
    createError: (message: string) => Error
  ): string;
}

interface EvidenceSanitizerAdapter {
  sanitizeTitle(value: string | null): string;
  sanitizeExcerpt(value: string | null, maxLength?: number): string;
  findSuspiciousPattern(title: string | null, excerpt: string | null): string | null;
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

function createCitationGroundingAdapter(): CitationGroundingAdapter {
  const urlSafetyFacade = createUrlSafetyFacade();

  const canonicalizeUrl = (value: string): string | null =>
    urlSafetyFacade.canonicalizeHttpUrl(value);

  const dedupeCanonicalUrls = (values: Array<string | null | undefined>): string[] => {
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
  };

  const extractCanonicalUrlsFromText = (text: string): string[] => {
    const matches = text.match(NOTES_URL_PATTERN);
    if (!matches || matches.length === 0) {
      return [];
    }

    const cleaned = matches.map((value) => value.replace(/[),.;!?]+$/g, ""));
    return dedupeCanonicalUrls(cleaned);
  };

  const createEvidenceUrlSet = (request: ParsedSummaryRequest): Set<string> => {
    const urls = request.topics.flatMap((topic) =>
      topic.evidence.map((evidence) => evidence.url)
    );
    return new Set(dedupeCanonicalUrls(urls));
  };

  return {
    canonicalizeUrl,
    dedupeCanonicalUrls,
    createEvidenceUrlSet,
    filterGroundedCitations(citations, evidenceUrls) {
      const filtered = dedupeCanonicalUrls(citations);
      return filtered.filter((citation) => evidenceUrls.has(citation));
    },
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
  };
}

function createEvidenceSanitizerAdapter(): EvidenceSanitizerAdapter {
  return {
    sanitizeTitle(value) {
      if (!value || value.trim().length === 0) {
        return "";
      }

      const sanitized = value
        .replace(TITLE_CONTROL_CHAR_PATTERN, "")
        .slice(0, TITLE_MAX_LENGTH)
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(INSTRUCTION_MARKER_PATTERN, "")
        .trim();
      return sanitized.length > 0 ? sanitized : "[No title]";
    },
    sanitizeExcerpt(value, maxLength = EVIDENCE_EXCERPT_MAX_LENGTH) {
      if (!value || value.trim().length === 0) {
        return "";
      }

      const sanitized = value
        .replace(EXCERPT_CONTROL_CHAR_PATTERN, "")
        .replace(/\s{3,}/g, "  ")
        .slice(0, maxLength)
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(INSTRUCTION_MARKER_PATTERN, "")
        .trim();
      return sanitized.length > 0 ? sanitized : "[Content removed]";
    },
    findSuspiciousPattern(title, excerpt) {
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
    },
  };
}

interface SummaryPayloadAdapters {
  citationGrounding: CitationGroundingAdapter;
  evidenceSanitizer: EvidenceSanitizerAdapter;
}

function countEvidenceItems(request: ParsedSummaryRequest): number {
  return request.topics.reduce((count, topic) => count + topic.evidence.length, 0);
}

function resolvePayloadExcerptMaxLength(request: ParsedSummaryRequest): number {
  const evidenceCount = countEvidenceItems(request);
  if (evidenceCount <= 0) {
    return EVIDENCE_EXCERPT_MAX_LENGTH;
  }

  const budgetBasedLimit = Math.floor(
    TARGET_TOTAL_EVIDENCE_EXCERPT_CHARS / evidenceCount
  );

  return Math.max(
    MIN_EVIDENCE_EXCERPT_LENGTH,
    Math.min(EVIDENCE_EXCERPT_MAX_LENGTH, budgetBasedLimit)
  );
}

function mapEvidenceToPayload(
  request: ParsedSummaryRequest,
  topic: ParsedSummaryTopic,
  evidence: ParsedSummaryEvidence,
  excerptMaxLength: number,
  options: SummaryRequestPayloadOptions,
  adapters: SummaryPayloadAdapters
): Record<string, unknown> {
  const suspiciousPattern = adapters.evidenceSanitizer.findSuspiciousPattern(
    evidence.title,
    evidence.textExcerpt
  );
  if (suspiciousPattern) {
    options.logger?.warn(
      {
        requestId: request.requestId,
        topic: topic.topic,
        eventId: evidence.eventId,
        pattern: suspiciousPattern,
      },
      "Suspicious prompt-like content detected in evidence"
    );
    if (options.healthContext) {
      incrementSuspiciousContent(options.healthContext);
    }
  }

  return {
    event_id: evidence.eventId,
    source: evidence.source,
    url: evidence.url ? adapters.citationGrounding.canonicalizeUrl(evidence.url) ?? "" : "",
    title: adapters.evidenceSanitizer.sanitizeTitle(evidence.title),
    published_at: evidence.publishedAt ? evidence.publishedAt.toISOString() : "",
    fetched_at: evidence.fetchedAt ? evidence.fetchedAt.toISOString() : "",
    text_excerpt: adapters.evidenceSanitizer.sanitizeExcerpt(
      evidence.textExcerpt,
      excerptMaxLength
    ),
  };
}

function mapTopicToPayload(
  request: ParsedSummaryRequest,
  topic: ParsedSummaryTopic,
  options: SummaryRequestPayloadOptions,
  adapters: SummaryPayloadAdapters,
  excerptMaxLength: number
): Record<string, unknown> {
  return {
    topic: topic.topic,
    metrics: topic.metrics.map((metric) => ({
      topic: metric.topic,
      window: metric.window,
      score: metric.score,
      volume: metric.volume,
      acceleration: metric.acceleration,
    })),
    evidence: topic.evidence.map((evidence) =>
      mapEvidenceToPayload(request, topic, evidence, excerptMaxLength, options, adapters)
    ),
  };
}

function buildSummaryRequestPayload(
  request: ParsedSummaryRequest,
  options: SummaryRequestPayloadOptions,
  adapters: SummaryPayloadAdapters
): Record<string, unknown> {
  const excerptMaxLength = resolvePayloadExcerptMaxLength(request);

  return {
    request_id: request.requestId,
    requested_at: request.requestedAt.toISOString(),
    type: request.type,
    windows: request.windows,
    topics: request.topics.map((topic) =>
      mapTopicToPayload(request, topic, options, adapters, excerptMaxLength)
    ),
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
}

export function createSummaryRequestGroundingFacade(): SummaryRequestGroundingFacade {
  const citationGrounding = createCitationGroundingAdapter();
  const evidenceSanitizer = createEvidenceSanitizerAdapter();

  return {
    dedupeCanonicalUrls: citationGrounding.dedupeCanonicalUrls,
    createEvidenceUrlSet: citationGrounding.createEvidenceUrlSet,
    filterGroundedCitations: citationGrounding.filterGroundedCitations,
    enforceGroundedNotes(request, notes, createError) {
      return citationGrounding.enforceGroundedNotes(request, notes, createError);
    },
    buildSummaryRequestPayload(request, options = {}) {
      return buildSummaryRequestPayload(request, options, {
        citationGrounding,
        evidenceSanitizer,
      });
    },
  };
}
