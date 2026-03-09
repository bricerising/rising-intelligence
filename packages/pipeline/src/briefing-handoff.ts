import { isIP } from "node:net";
import {
  normalizeOptionalString,
  parseDate,
} from "./raw-event.js";
import {
  parseCanonicalSource,
  type CanonicalSource,
} from "./source.js";

const TITLE_MAX_LENGTH = 200;
export const BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH = 2_000;
const TARGET_TOTAL_EVIDENCE_EXCERPT_CHARS = 10_000;
const MIN_EVIDENCE_EXCERPT_LENGTH = 160;
const EXCERPT_CONTROL_CHAR_PATTERN = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g;
const TITLE_CONTROL_CHAR_PATTERN = /[\x00-\x1F\x7F]/g;
const INSTRUCTION_MARKER_PATTERN =
  /\[INST\]|\[\/INST\]|\[SYSTEM\]|<<SYS>>|<<\/SYS>>|<\/SYS>>/gi;
const SUSPICIOUS_CONTENT_PATTERNS = [
  /ignore\s+(all\s+)?(previous|prior|above)\s+instructions/i,
  /you\s+are\s+(now\s+)?a\s+(different|new)/i,
  /\[INST\]/i,
  /<<SYS>>/i,
  /system\s*:\s*$/im,
] as const;

export interface BriefEvidenceRecord {
  eventId: string;
  source: CanonicalSource;
  url: string | null;
  title: string | null;
  publishedAt: Date | null;
  fetchedAt: Date | null;
  textExcerpt: string | null;
  suspiciousPattern?: string | null;
}

export interface BriefEvidenceRecordInput {
  eventId: string;
  source: CanonicalSource | string | number;
  url?: string | null;
  title?: string | null;
  publishedAt?: Date | string | null;
  fetchedAt?: Date | string | null;
  textExcerpt?: string | null;
  suspiciousPattern?: string | null;
}

export interface CollectedContentBriefingInput {
  eventId: string;
  source: CanonicalSource | string | number;
  url?: string | null;
  title?: string | null;
  publishedAt?: Date | string | null;
  fetchedAt?: Date | string | null;
  text?: string | null;
}

export interface RawEventBriefingInput {
  event_id: string;
  source: CanonicalSource | string | number;
  url?: string | null;
  title?: string | null;
  published_at?: Date | string | null;
  fetched_at?: Date | string | null;
  text?: string | null;
}

export interface SummaryRequestPayloadMetricInput {
  topic: string;
  window: number;
  score: number;
  volume: number;
  acceleration: number;
}

export interface SummaryRequestPayloadTopicInput<
  TEvidence extends BriefEvidenceRecordInput = BriefEvidenceRecordInput,
> {
  topic: string;
  metrics: SummaryRequestPayloadMetricInput[];
  evidence: TEvidence[];
}

export interface SummaryRequestPayloadInput<
  TEvidence extends BriefEvidenceRecordInput = BriefEvidenceRecordInput,
> {
  requestId: string;
  requestedAt: Date | string;
  type: string;
  windows: number[];
  topics: Array<SummaryRequestPayloadTopicInput<TEvidence>>;
  budget:
    | {
        dailyBudgetUsd?: number;
        maxTopics?: number;
        maxEvidencePerTopic?: number;
        maxOutputTokens?: number;
      }
    | null;
  query:
    | {
        lookbackDays?: number;
        topicGlobs?: string[];
        maxEventsPerTopic?: number;
        evidenceStrategy?: string;
      }
    | null;
  report:
    | {
        timezone?: string;
        startAt?: Date | string;
        endAt?: Date | string;
      }
    | null;
}

export interface SummaryRequestPayload {
  request_id: string;
  requested_at: string;
  type: string;
  windows: number[];
  topics: Array<{
    topic: string;
    metrics: Array<{
      topic: string;
      window: number;
      score: number;
      volume: number;
      acceleration: number;
    }>;
    evidence: Array<{
      event_id: string;
      source: CanonicalSource;
      url: string;
      title: string;
      published_at: string;
      fetched_at: string;
      text_excerpt: string;
    }>;
  }>;
  budget:
    | {
        daily_budget_usd?: number;
        max_topics?: number;
        max_evidence_per_topic?: number;
        max_output_tokens?: number;
      }
    | null;
  query:
    | {
        lookback_days?: number;
        topic_globs?: string[];
        max_events_per_topic?: number;
        evidence_strategy?: string;
      }
    | null;
  report:
    | {
        timezone?: string;
        start_at?: string;
        end_at?: string;
      }
    | null;
}

export interface BuildSummaryRequestPayloadOptions {
  onSuspiciousEvidence?: (input: {
    topic: string;
    eventId: string;
    pattern: string;
  }) => void;
}

function normalizeHostname(hostname: string): string {
  const normalized = hostname.trim().toLowerCase();
  if (normalized.startsWith("[") && normalized.endsWith("]")) {
    return normalized.slice(1, -1);
  }
  return normalized;
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
  if (hostname.startsWith("::")) {
    return true;
  }
  if (hostname.startsWith("fc") || hostname.startsWith("fd")) {
    return true;
  }

  const firstHextet = hostname.split(":")[0];
  return /^fe[89ab][0-9a-f]{0,2}$/i.test(firstHextet);
}

function isDisallowedHostname(hostname: string): boolean {
  const normalized = normalizeHostname(hostname);
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

function parseHttpUrl(value: string): URL | null {
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function normalizeHttpUrl(parsed: URL): string {
  parsed.hash = "";
  parsed.hostname = normalizeHostname(parsed.hostname);
  if (
    (parsed.protocol === "http:" && parsed.port === "80") ||
    (parsed.protocol === "https:" && parsed.port === "443")
  ) {
    parsed.port = "";
  }
  if (parsed.pathname.length > 1 && parsed.pathname.endsWith("/")) {
    parsed.pathname = parsed.pathname.slice(0, -1);
  }
  return parsed.toString();
}

function parseOptionalDateValue(
  value: Date | string | null | undefined,
  field: string
): Date | null {
  if (!value) {
    return null;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  const normalized = normalizeOptionalString(value);
  if (!normalized) {
    return null;
  }
  return parseDate(normalized, field);
}

export function canonicalizeBriefEvidenceUrl(value: string): string | null {
  const normalized = normalizeOptionalString(value);
  if (!normalized) {
    return null;
  }

  const parsed = parseHttpUrl(normalized);
  if (!parsed || isDisallowedHostname(parsed.hostname)) {
    return null;
  }

  return normalizeHttpUrl(parsed);
}

export function dedupeCanonicalBriefEvidenceUrls(
  values: Array<string | null | undefined>
): string[] {
  const unique = new Set<string>();

  for (const value of values) {
    if (!value) {
      continue;
    }

    const canonical = canonicalizeBriefEvidenceUrl(value);
    if (!canonical) {
      continue;
    }
    unique.add(canonical);
  }

  return [...unique];
}

export function normalizeBriefEvidenceTitle(
  value: string | null | undefined
): string | null {
  const normalized = normalizeOptionalString(value ?? undefined);
  if (!normalized) {
    return null;
  }

  const sanitized = normalized
    .replace(TITLE_CONTROL_CHAR_PATTERN, "")
    .slice(0, TITLE_MAX_LENGTH)
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(INSTRUCTION_MARKER_PATTERN, "")
    .trim();

  if (sanitized.length === 0) {
    return "[No title]";
  }

  return sanitized;
}

export function normalizeBriefEvidenceExcerpt(
  value: string | null | undefined,
  maxLength = BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH
): string | null {
  const normalized = normalizeOptionalString(value ?? undefined);
  if (!normalized) {
    return null;
  }

  const sanitized = normalized
    .replace(EXCERPT_CONTROL_CHAR_PATTERN, "")
    .replace(/\s{3,}/g, "  ")
    .slice(0, maxLength)
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(INSTRUCTION_MARKER_PATTERN, "")
    .trim();

  if (sanitized.length === 0) {
    return "[Content removed]";
  }

  return sanitized;
}

export function detectSuspiciousBriefEvidencePattern(
  title: string | null | undefined,
  excerpt: string | null | undefined
): string | null {
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

export function buildBriefEvidenceRecord(
  input: BriefEvidenceRecordInput,
  options: { excerptMaxLength?: number } = {}
): BriefEvidenceRecord {
  const suspiciousPattern =
    input.suspiciousPattern ??
    detectSuspiciousBriefEvidencePattern(input.title, input.textExcerpt);

  return {
    eventId: input.eventId.trim(),
    source: parseCanonicalSource(input.source),
    url: input.url ? canonicalizeBriefEvidenceUrl(input.url) : null,
    title: normalizeBriefEvidenceTitle(input.title),
    publishedAt: parseOptionalDateValue(input.publishedAt, "published_at"),
    fetchedAt: parseOptionalDateValue(input.fetchedAt, "fetched_at"),
    textExcerpt: normalizeBriefEvidenceExcerpt(
      input.textExcerpt,
      options.excerptMaxLength
    ),
    suspiciousPattern,
  };
}

export function buildBriefEvidenceRecordFromRawEvent(
  input: RawEventBriefingInput,
  options: { excerptMaxLength?: number } = {}
): BriefEvidenceRecord {
  return buildBriefEvidenceRecordFromCollectedContent(
    {
      eventId: input.event_id,
      source: input.source,
      url: input.url,
      title: input.title,
      publishedAt: input.published_at,
      fetchedAt: input.fetched_at,
      text: input.text,
    },
    options
  );
}

export function buildBriefEvidenceRecordFromCollectedContent(
  input: CollectedContentBriefingInput,
  options: { excerptMaxLength?: number } = {}
): BriefEvidenceRecord {
  return buildBriefEvidenceRecord(
    {
      eventId: input.eventId,
      source: input.source,
      url: input.url,
      title: input.title,
      publishedAt: input.publishedAt,
      fetchedAt: input.fetchedAt,
      textExcerpt: input.text,
    },
    options
  );
}

function countEvidenceItems(
  request: SummaryRequestPayloadInput
): number {
  return request.topics.reduce(
    (count, topic) => count + topic.evidence.length,
    0
  );
}

export function resolveBriefEvidenceExcerptMaxLength(
  request: SummaryRequestPayloadInput
): number {
  const evidenceCount = countEvidenceItems(request);
  if (evidenceCount <= 0) {
    return BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH;
  }

  const budgetBasedLimit = Math.floor(
    TARGET_TOTAL_EVIDENCE_EXCERPT_CHARS / evidenceCount
  );

  return Math.max(
    MIN_EVIDENCE_EXCERPT_LENGTH,
    Math.min(BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH, budgetBasedLimit)
  );
}

function mapEvidenceToPayload(
  topic: SummaryRequestPayloadTopicInput,
  evidence: BriefEvidenceRecordInput,
  excerptMaxLength: number,
  options: BuildSummaryRequestPayloadOptions
): SummaryRequestPayload["topics"][number]["evidence"][number] {
  const normalizedEvidence = buildBriefEvidenceRecord(evidence, {
    excerptMaxLength,
  });

  const suspiciousPattern =
    normalizedEvidence.suspiciousPattern ??
    detectSuspiciousBriefEvidencePattern(evidence.title, evidence.textExcerpt);
  if (suspiciousPattern) {
    options.onSuspiciousEvidence?.({
      topic: topic.topic,
      eventId: normalizedEvidence.eventId,
      pattern: suspiciousPattern,
    });
  }

  return {
    event_id: normalizedEvidence.eventId,
    source: normalizedEvidence.source,
    url: normalizedEvidence.url ?? "",
    title: normalizedEvidence.title ?? "",
    published_at: normalizedEvidence.publishedAt
      ? normalizedEvidence.publishedAt.toISOString()
      : "",
    fetched_at: normalizedEvidence.fetchedAt
      ? normalizedEvidence.fetchedAt.toISOString()
      : "",
    text_excerpt: normalizedEvidence.textExcerpt ?? "",
  };
}

function toIsoString(
  value: Date | string | null | undefined,
  field: string
): string | undefined {
  const parsed = parseOptionalDateValue(value, field);
  return parsed ? parsed.toISOString() : undefined;
}

export function buildSummaryRequestPayload(
  request: SummaryRequestPayloadInput,
  options: BuildSummaryRequestPayloadOptions = {}
): SummaryRequestPayload {
  const excerptMaxLength = resolveBriefEvidenceExcerptMaxLength(request);
  const requestedAt = parseOptionalDateValue(request.requestedAt, "requested_at");
  if (!requestedAt) {
    throw new Error("Invalid requested_at");
  }

  return {
    request_id: request.requestId,
    requested_at: requestedAt.toISOString(),
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
      evidence: topic.evidence.map((evidence) =>
        mapEvidenceToPayload(topic, evidence, excerptMaxLength, options)
      ),
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
          start_at: toIsoString(request.report.startAt, "report.start_at"),
          end_at: toIsoString(request.report.endAt, "report.end_at"),
        }
      : null,
  };
}
