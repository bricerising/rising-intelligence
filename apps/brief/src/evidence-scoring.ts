/**
 * Evidence scoring and insight extraction.
 *
 * Scores individual evidence items by signal strength, recency, source
 * authority, and topic relevance so that the orchestrator can rank and
 * select the most informative items for a brief highlight.
 */

import type { SummaryRequestGroundingFacade } from "./grounding-facade.js";
import {
  countTopicRelevanceTermMatches,
} from "./query-mode-selection.js";
import {
  detectSignalCategories,
  buildInternalWhyItMatters,
  buildInternalSuggestedAction,
  type SignalCategory,
} from "./internal-highlight-strategy.js";
import type {
  ParsedSummaryEvidence,
  ParsedSummaryTopic,
} from "./types.js";

// ── Text normalization helpers ──────────────────────────────────────────────

const DATE_ONLY_TITLE_REGEX =
  /^(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2},\s+\d{4}$/i;

const GENERIC_EVIDENCE_SNIPPET_PATTERNS: RegExp[] = [
  /^the following release notes cover/i,
  /^for a comprehensive list/i,
  /^you can also see and filter all release notes/i,
  /^get the latest updates on azure/i,
  /^subscribe to notifications to stay informed/i,
  /^skip to main content/i,
  /^overview guides reference samples resources/i,
  /^welcome to /i,
  /^reading time:/i,
  /^table of contents/i,
  /^transcript$/i,
];

export function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

export function truncateText(value: string, maxChars: number): string {
  if (value.length <= maxChars) {
    return value;
  }
  return `${value.slice(0, maxChars - 1).trimEnd()}…`;
}

export function normalizeTopicKey(topic: string): string {
  return topic.trim().toLowerCase();
}

export function normalizeTextFingerprint(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

export function ensureSentenceEnding(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return trimmed;
  }
  if (/[.!?]$/.test(trimmed)) {
    return trimmed;
  }
  return `${trimmed}.`;
}

export function isLowSignalTitle(value: string): boolean {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return true;
  }
  if (DATE_ONLY_TITLE_REGEX.test(normalized)) {
    return true;
  }
  if (normalized.length < 12) {
    return true;
  }
  return GENERIC_EVIDENCE_SNIPPET_PATTERNS.some((pattern) => pattern.test(normalized));
}

export function extractFirstMeaningfulSentence(value: string, maxChars: number): string | null {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  const candidates = normalized
    .split(/(?<=[.!?])\s+|\s*\n+\s*/)
    .map((candidate) => candidate.trim())
    .filter((candidate) => candidate.length >= 24)
    .filter((candidate) => !GENERIC_EVIDENCE_SNIPPET_PATTERNS.some((pattern) => pattern.test(candidate)));
  if (candidates.length === 0) {
    return null;
  }

  return truncateText(candidates[0], maxChars);
}

// ── Cloud-host authority rules ──────────────────────────────────────────────

interface PreferredCloudHostRule {
  topicPrefix: string;
  preferredHostSuffixes: readonly string[];
}

const PREFERRED_CLOUD_HOST_RULES: ReadonlyArray<PreferredCloudHostRule> = [
  {
    topicPrefix: "aws.",
    preferredHostSuffixes: ["aws.amazon.com", "docs.aws.amazon.com"],
  },
  {
    topicPrefix: "cloud.gcp",
    preferredHostSuffixes: [
      "cloud.google.com",
      "docs.cloud.google.com",
      "status.cloud.google.com",
    ],
  },
  {
    topicPrefix: "cloud.azure",
    preferredHostSuffixes: ["azure.microsoft.com", "learn.microsoft.com"],
  },
  {
    topicPrefix: "cloud.terraform",
    preferredHostSuffixes: [
      "hashicorp.com",
      "terraform.io",
      "aws.amazon.com",
      "cloud.google.com",
      "azure.microsoft.com",
    ],
  },
];

function resolvePreferredCloudHostRule(topic: string): PreferredCloudHostRule | null {
  const normalizedTopic = normalizeTopicKey(topic);
  for (const rule of PREFERRED_CLOUD_HOST_RULES) {
    if (normalizedTopic.startsWith(rule.topicPrefix)) {
      return rule;
    }
  }
  return null;
}

function hostnameMatchesAnySuffix(hostname: string, suffixes: readonly string[]): boolean {
  return suffixes.some((suffix) => hostname.endsWith(suffix));
}

function isPreferredCloudHost(topic: string, hostname: string | null): boolean {
  if (!hostname) {
    return false;
  }

  const rule = resolvePreferredCloudHostRule(topic);
  if (!rule) {
    return false;
  }

  return hostnameMatchesAnySuffix(hostname, rule.preferredHostSuffixes);
}

function hasPreferredCloudHostRule(topic: string): boolean {
  return resolvePreferredCloudHostRule(topic) !== null;
}

function getEvidenceRecencyMs(evidence: ParsedSummaryEvidence): number {
  return (evidence.publishedAt ?? evidence.fetchedAt ?? new Date(0)).getTime();
}

function getEvidenceHostname(url: string | null): string | null {
  if (!url) {
    return null;
  }
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }
}

function countTopicEvidenceTermMatches(topic: string, value: string): number {
  return countTopicRelevanceTermMatches(topic, value, 1);
}

// ── Evidence insight building ───────────────────────────────────────────────

export interface EvidenceInsight {
  summary: string;
  citation: string | null;
  categories: Set<SignalCategory>;
  score: number;
  recencyMs: number;
}

export function buildEvidenceInsight(
  groundingFacade: SummaryRequestGroundingFacade,
  topic: ParsedSummaryTopic,
  evidence: ParsedSummaryEvidence
): EvidenceInsight | null {
  const title = normalizeWhitespace(evidence.title ?? "");
  const excerptSentence = extractFirstMeaningfulSentence(evidence.textExcerpt ?? "", 170);
  const citation = evidence.url
    ? groundingFacade.dedupeCanonicalUrls([evidence.url])[0] ?? null
    : null;

  let summary = "";
  if (!isLowSignalTitle(title)) {
    summary = truncateText(title, 140);
    if (excerptSentence) {
      const titleFingerprint = normalizeTextFingerprint(summary);
      const excerptFingerprint = normalizeTextFingerprint(excerptSentence);
      if (excerptFingerprint.length > 0 && !excerptFingerprint.includes(titleFingerprint)) {
        summary = truncateText(`${summary} — ${excerptSentence}`, 220);
      }
    }
  } else if (excerptSentence) {
    summary = excerptSentence;
  } else {
    const hostname = getEvidenceHostname(evidence.url);
    if (hostname) {
      summary = `Update reported by ${hostname}`;
    }
  }

  if (!summary) {
    return null;
  }

  const categories = detectSignalCategories([title, evidence.textExcerpt ?? "", evidence.url ?? ""].join(" "));
  const hostname = getEvidenceHostname(evidence.url);
  const preferredHost = isPreferredCloudHost(topic.topic, hostname);
  const topicTermMatches = countTopicEvidenceTermMatches(
    topic.topic,
    `${title} ${evidence.textExcerpt ?? ""} ${evidence.url ?? ""}`
  );
  let score = 0;
  score += isLowSignalTitle(title) ? -2 : 3;
  score += excerptSentence ? 2 : 0;
  score += categories.size;
  score += preferredHost ? 4 : 0;
  score += Math.min(topicTermMatches, 2);
  if (topicTermMatches === 0 && !preferredHost) {
    score -= 4;
  }
  if (hasPreferredCloudHostRule(topic.topic) && !preferredHost) {
    score -= 2;
  }
  score += evidence.publishedAt ? 1 : 0;

  return {
    summary,
    citation,
    categories,
    score,
    recencyMs: getEvidenceRecencyMs(evidence),
  };
}

export function collectTopEvidenceInsights(
  groundingFacade: SummaryRequestGroundingFacade,
  topic: ParsedSummaryTopic,
  limit: number
): EvidenceInsight[] {
  const seen = new Set<string>();
  const insights = topic.evidence
    .map((evidence) => buildEvidenceInsight(groundingFacade, topic, evidence))
    .filter((insight): insight is EvidenceInsight => insight !== null)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return right.recencyMs - left.recencyMs;
    })
    .filter((insight) => {
      const key = normalizeTextFingerprint(insight.summary);
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });

  return insights.slice(0, limit);
}

// ── Internal highlight building ─────────────────────────────────────────────

export interface NormalizedHighlight {
  topic: string;
  what_happened: string;
  why_it_matters: string;
  suggested_action: string;
  citations: string[];
}

export function buildInternalHighlight(
  groundingFacade: SummaryRequestGroundingFacade,
  topic: ParsedSummaryTopic
): NormalizedHighlight {
  const fallbackCitations = groundingFacade.dedupeCanonicalUrls(topic.evidence.map((evidence) => evidence.url));
  const insights = collectTopEvidenceInsights(groundingFacade, topic, 2);
  const citations = groundingFacade.dedupeCanonicalUrls(
    insights.map((insight) => insight.citation).filter((citation): citation is string => Boolean(citation))
  );
  const categories = new Set<SignalCategory>();
  for (const insight of insights) {
    for (const category of insight.categories) {
      categories.add(category);
    }
  }

  const whatHappened =
    insights.length > 0
      ? ensureSentenceEnding(insights.map((insight) => insight.summary).join("; "))
      : `Recent updates were detected for ${topic.topic}.`;

  return {
    topic: topic.topic,
    what_happened: whatHappened,
    why_it_matters: buildInternalWhyItMatters(topic.topic, categories),
    suggested_action: buildInternalSuggestedAction(categories),
    citations: citations.length > 0 ? citations : fallbackCitations,
  };
}
