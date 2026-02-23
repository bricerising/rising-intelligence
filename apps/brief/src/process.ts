import { BriefStatus, type PrismaClient } from "@rising-intelligence/db";
import type { Producer } from "kafkajs";
import type { Redis } from "ioredis";
import {
  buildFunctionDependencies,
  serializeError,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared";
import type pino from "pino";
import { z } from "zod";
import {
  createBriefBudgetLedger,
  type BriefBudgetLedger,
  type CreateBriefBudgetLedgerInput,
} from "./budget-ledger.js";
import type { Config } from "./config.js";
import {
  incrementBudgetExceeded,
  incrementDuplicatesSkipped,
  incrementError,
  incrementGeneration,
  incrementLlmCostUsd,
  incrementLlmTokens,
  observeCitationsCount,
  observeHighlightsCount,
  setBudgetRemainingUsd,
  type HealthContext,
} from "./health.js";
import {
  createSummaryRequestGroundingFacade,
} from "./grounding-facade.js";
import { executeCodexCli } from "./llm/codex-cli.js";
import {
  classifyRetryableFailureCode,
  LlmGenerationError,
  NonRetryableProcessingError,
  toGroundingError,
} from "./processing-errors.js";
import {
  createBriefResultPublisher,
  type CreateBriefResultPublisherInput,
  type BriefResultPublisher,
} from "./publishing-facade.js";
import {
  createQueryModeRequestResolver,
  type QueryModeRequestResolver,
} from "./query-mode-request-facade.js";
import {
  buildFailureBriefResultPayload,
  type BriefResultPayload,
} from "./result-payload-adapter.js";
import {
  createBriefResultStore,
  type BriefResultStore,
  type StoredBriefResult,
} from "./result-store-facade.js";
import type {
  LlmProvider,
  ParsedSummaryEvidence,
  ParsedSummaryRequest,
  ParsedSummaryTopic,
} from "./types.js";
import {
  countTopicRelevanceTermMatches,
  getTopLevelTopicGroup,
} from "./query-mode-selection.js";
import {
  buildInternalSuggestedAction,
  buildInternalWhyItMatters,
  detectSignalCategories,
  type SignalCategory,
} from "./internal-highlight-strategy.js";

export interface ProcessContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  prisma: PrismaClient;
  redis: Redis;
  producer: Producer;
}

const LlmHighlightSchema = z.object({
  topic: z.string().min(1),
  what_happened: z.string().min(1),
  why_it_matters: z.string().min(1),
  suggested_action: z.string().min(1),
  citations: z.array(z.string().url()).min(1),
});

const LlmResponseSchema = z.object({
  title: z.string().min(1),
  highlights: z.array(LlmHighlightSchema).default([]),
  notes: z.string().optional(),
  usage: z
    .object({
      prompt_tokens: z.number().int().nonnegative(),
      completion_tokens: z.number().int().nonnegative(),
    })
    .optional(),
  meta: z
    .object({
      provider: z.string().min(1).optional(),
      model: z.string().min(1).optional(),
      estimated_cost_usd: z.number().nonnegative().optional(),
    })
    .optional(),
});

type NormalizedHighlight = z.infer<typeof LlmHighlightSchema>;
type ParsedLlmResponse = z.infer<typeof LlmResponseSchema>;
const groundingFacade = createSummaryRequestGroundingFacade();

interface SuccessResult {
  payload: {
    request_id: string;
    produced_at: string;
    brief: {
      brief_id: string;
      generated_at: string;
      window: number;
      title: string;
      highlights: NormalizedHighlight[];
      notes: string;
      meta: {
        provider: string;
        model: string;
        input_tokens: number;
        output_tokens: number;
        estimated_cost_usd: number;
      };
    };
  };
  metrics: {
    highlightsCount: number;
    citationsCount: number;
    inputTokens: number;
    outputTokens: number;
    costUsd: number;
  };
}

export interface SummaryRequestProcessor {
  processSummaryRequest(ctx: ProcessContext, request: ParsedSummaryRequest): Promise<void>;
}

interface SummaryRequestProcessorDependencies {
  createBriefBudgetLedger(input: CreateBriefBudgetLedgerInput): BriefBudgetLedger;
  createBriefResultPublisher(
    input: CreateBriefResultPublisherInput
  ): BriefResultPublisher<BriefResultPayload>;
  createBriefResultStore(
    prisma: PrismaClient,
    healthContext: HealthContext
  ): BriefResultStore;
  createQueryModeRequestResolver(): QueryModeRequestResolver;
}

type SummaryRequestProcessorDependencyOverrides = FunctionDependencyOverrides<
  SummaryRequestProcessorDependencies
>;

interface SummaryRequestRuntime {
  logger: pino.Logger;
  budgetLedger: BriefBudgetLedger;
  publisher: BriefResultPublisher<BriefResultPayload>;
  resultStore: BriefResultStore;
  queryModeRequestResolver: QueryModeRequestResolver;
  producedAt: Date;
}

const DEFAULT_SUMMARY_REQUEST_PROCESSOR_DEPENDENCIES: SummaryRequestProcessorDependencies = {
  createBriefBudgetLedger,
  createBriefResultPublisher(input): BriefResultPublisher<BriefResultPayload> {
    return createBriefResultPublisher<BriefResultPayload>(input);
  },
  createBriefResultStore,
  createQueryModeRequestResolver,
};

class SummaryRequestRuntimeFactory {
  private readonly queryModeRequestResolver: QueryModeRequestResolver;

  constructor(
    private readonly dependencies: SummaryRequestProcessorDependencies
  ) {
    this.queryModeRequestResolver = dependencies.createQueryModeRequestResolver();
  }

  create(ctx: ProcessContext, request: ParsedSummaryRequest): SummaryRequestRuntime {
    const logger = ctx.logger.child({ requestId: request.requestId });
    return {
      logger,
      budgetLedger: this.dependencies.createBriefBudgetLedger({
        prisma: ctx.prisma,
        redis: ctx.redis,
        logger,
      }),
      publisher: this.dependencies.createBriefResultPublisher({
        producer: ctx.producer,
        logger,
        topic: ctx.config.KAFKA_TOPIC_SUMMARY_RESULTS,
      }),
      resultStore: this.dependencies.createBriefResultStore(
        ctx.prisma,
        ctx.healthContext
      ),
      queryModeRequestResolver: this.queryModeRequestResolver,
      producedAt: new Date(),
    };
  }
}

function getBudgetDateKey(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function estimateTokenCount(text: string): number {
  return Math.max(1, Math.ceil(text.length / 4));
}

function estimateRequestCostUsd(request: ParsedSummaryRequest): number {
  const evidenceCount = request.topics.reduce((sum, topic) => sum + topic.evidence.length, 0);
  return Number((0.01 + request.topics.length * 0.002 + evidenceCount * 0.0005).toFixed(4));
}

function normalizeUsd(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  if (value <= 0) {
    return 0;
  }

  return Number(value.toFixed(6));
}

function normalizeUsdDelta(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }

  const rounded = Number(value.toFixed(6));
  return rounded === 0 ? 0 : rounded;
}

interface EvidenceInsight {
  summary: string;
  citation: string | null;
  categories: Set<SignalCategory>;
  score: number;
  recencyMs: number;
}

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

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function truncateText(value: string, maxChars: number): string {
  if (value.length <= maxChars) {
    return value;
  }
  return `${value.slice(0, maxChars - 1).trimEnd()}…`;
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

function isLowSignalTitle(value: string): boolean {
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

function extractFirstMeaningfulSentence(value: string, maxChars: number): string | null {
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

function countTopicEvidenceTermMatches(topic: string, value: string): number {
  return countTopicRelevanceTermMatches(topic, value, 1);
}

function buildEvidenceInsight(topic: ParsedSummaryTopic, evidence: ParsedSummaryEvidence): EvidenceInsight | null {
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

function collectTopEvidenceInsights(topic: ParsedSummaryTopic, limit: number): EvidenceInsight[] {
  const seen = new Set<string>();
  const insights = topic.evidence
    .map((evidence) => buildEvidenceInsight(topic, evidence))
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

function buildInternalHighlight(topic: ParsedSummaryTopic): NormalizedHighlight {
  const fallbackCitations = groundingFacade.dedupeCanonicalUrls(topic.evidence.map((evidence) => evidence.url));
  const insights = collectTopEvidenceInsights(topic, 2);
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

function normalizeLlmHighlight(highlight: NormalizedHighlight): NormalizedHighlight {
  return {
    topic: highlight.topic.trim(),
    what_happened: highlight.what_happened.trim(),
    why_it_matters: highlight.why_it_matters.trim(),
    suggested_action: highlight.suggested_action.trim(),
    citations: groundingFacade.dedupeCanonicalUrls(highlight.citations),
  };
}

function normalizeTopicKey(topic: string): string {
  return topic.trim().toLowerCase();
}

function normalizeTextFingerprint(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function ensureSentenceEnding(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return trimmed;
  }
  if (/[.!?]$/.test(trimmed)) {
    return trimmed;
  }
  return `${trimmed}.`;
}

function mergeNarrativeFields(values: string[]): string {
  const merged: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed) {
      continue;
    }

    const fingerprint = normalizeTextFingerprint(trimmed);
    if (seen.has(fingerprint)) {
      continue;
    }
    seen.add(fingerprint);
    merged.push(ensureSentenceEnding(trimmed));
  }

  return merged.join(" ");
}

function mergeHighlightsByTopic(highlights: NormalizedHighlight[]): NormalizedHighlight[] {
  const merged: NormalizedHighlight[] = [];
  const indexByTopic = new Map<string, number>();

  for (const highlight of highlights) {
    const topicKey = normalizeTopicKey(highlight.topic);
    const existingIndex = indexByTopic.get(topicKey);
    if (existingIndex === undefined) {
      merged.push({
        ...highlight,
        citations: groundingFacade.dedupeCanonicalUrls(highlight.citations),
      });
      indexByTopic.set(topicKey, merged.length - 1);
      continue;
    }

    const existing = merged[existingIndex];
    merged[existingIndex] = {
      topic: existing.topic,
      what_happened: mergeNarrativeFields([existing.what_happened, highlight.what_happened]),
      why_it_matters: mergeNarrativeFields([existing.why_it_matters, highlight.why_it_matters]),
      suggested_action: mergeNarrativeFields([existing.suggested_action, highlight.suggested_action]),
      citations: groundingFacade.dedupeCanonicalUrls([...existing.citations, ...highlight.citations]),
    };
  }

  return merged;
}

interface TopicEvidenceScope {
  canonicalTopic: string;
  normalizedTopic: string;
  topLevelGroup: string;
  evidenceUrls: Set<string>;
}

function buildTopicEvidenceScopes(request: ParsedSummaryRequest): Map<string, TopicEvidenceScope> {
  const scopes = new Map<string, TopicEvidenceScope>();
  for (const topic of request.topics) {
    const canonicalTopic = topic.topic.trim();
    const topicKey = normalizeTopicKey(canonicalTopic);
    scopes.set(topicKey, {
      canonicalTopic,
      normalizedTopic: topicKey,
      topLevelGroup: getTopLevelTopicGroup(topicKey),
      evidenceUrls: new Set(groundingFacade.dedupeCanonicalUrls(topic.evidence.map((evidence) => evidence.url))),
    });
  }
  return scopes;
}

function countScopeCitationOverlap(scope: TopicEvidenceScope, citations: string[]): number {
  return citations.reduce((count, citation) => count + (scope.evidenceUrls.has(citation) ? 1 : 0), 0);
}

function resolveGroundedTopicScope(
  topicEvidenceScopes: Map<string, TopicEvidenceScope>,
  requestedTopicKey: string,
  groundedCitations: string[]
): TopicEvidenceScope | null {
  const declaredScope = topicEvidenceScopes.get(requestedTopicKey);
  if (declaredScope && countScopeCitationOverlap(declaredScope, groundedCitations) > 0) {
    return declaredScope;
  }

  const requestedTopLevelGroup = getTopLevelTopicGroup(requestedTopicKey);
  let bestScope: TopicEvidenceScope | null = null;
  let bestOverlap = 0;
  let bestSameTopLevelGroup = false;

  for (const scope of topicEvidenceScopes.values()) {
    const overlap = countScopeCitationOverlap(scope, groundedCitations);
    if (overlap <= 0) {
      continue;
    }

    const sameTopLevelGroup =
      requestedTopLevelGroup.length > 0 && scope.topLevelGroup === requestedTopLevelGroup;
    if (
      overlap > bestOverlap ||
      (overlap === bestOverlap && sameTopLevelGroup && !bestSameTopLevelGroup)
    ) {
      bestScope = scope;
      bestOverlap = overlap;
      bestSameTopLevelGroup = sameTopLevelGroup;
    }
  }

  return bestScope;
}

function formatReportDate(value: Date | undefined, timezone: string | undefined): string | null {
  if (!value) {
    return null;
  }

  try {
    return new Intl.DateTimeFormat("en-US", {
      timeZone: timezone ?? "UTC",
      year: "numeric",
      month: "long",
      day: "numeric",
    }).format(value);
  } catch {
    return value.toISOString();
  }
}

function deriveStructuredNotes(
  request: ParsedSummaryRequest,
  highlights: NormalizedHighlight[]
): string {
  const timezone = request.report?.timezone;
  const startAt = request.report?.startAt;
  const endAt = request.report?.endAt ?? request.requestedAt;
  const startText = formatReportDate(startAt, timezone);
  const endText = formatReportDate(endAt, timezone) ?? request.requestedAt.toISOString();
  const lookbackDays = request.query?.lookbackDays;
  const timeframe =
    startText !== null
      ? `${startText} through ${endText}`
      : lookbackDays && lookbackDays > 0
        ? `the last ${lookbackDays} day(s), ending ${endText}`
        : `up to ${endText}`;

  const topTopics = highlights.slice(0, 3).map((highlight) => highlight.topic);
  const topTopicSentence =
    topTopics.length > 0 ? topTopics.join(", ") : "No dominant topics were confidently grounded";
  const distinctSentences = (
    values: string[],
    limit: number,
    fallback: string
  ): string => {
    const sentenceCandidates = values
      .map((value) => normalizeWhitespace(value))
      .flatMap((value) => value.split(/(?<=[.!?])\s+/))
      .map((value) => value.trim())
      .filter((value) => value.length > 0)
      .map((value) => ensureSentenceEnding(value));
    const deduped = [...new Set(sentenceCandidates)];
    if (deduped.length === 0) {
      return fallback;
    }
    return deduped.slice(0, limit).join(" ");
  };
  const topicLandscapeLines =
    highlights.length > 0
      ? highlights
          .slice(0, 3)
          .map((highlight) => `- **${highlight.topic}**: ${highlight.why_it_matters}`)
      : ["- Limited coverage: no grounded highlights were produced for this request."];
  const riskAndGovernance = distinctSentences(
    highlights.map((highlight) => highlight.why_it_matters),
    2,
    "No grounded risk signals were available in this run."
  );
  const executionAndEconomics = distinctSentences(
    highlights.map((highlight) => highlight.suggested_action),
    2,
    "No grounded execution actions were available in this run."
  );
  const outlookText =
    topTopics.length > 0
      ? `Near-term execution focus is likely to remain on ${topTopicSentence} as teams operationalize the cited changes.`
      : "Collect additional grounded evidence before setting near-term outlook assumptions.";

  return [
    "# State of Signals and Where They're Going",
    "",
    "## Method and scope",
    `This report summarizes topic-level evidence gathered over ${timeframe}${timezone ? ` (${timezone})` : ""}.`,
    "",
    "## Dominant shifts",
    `Current signals are clustering around ${topTopicSentence}, with emphasis on concrete operational and platform updates.`,
    "",
    "## Topic landscape",
    ...topicLandscapeLines,
    "",
    "## Risk and governance",
    riskAndGovernance,
    "",
    "## Execution and economics",
    executionAndEconomics,
    "",
    "## Outlook",
    outlookText,
  ].join("\n");
}

function deriveDefaultNotes(request: ParsedSummaryRequest, highlights: NormalizedHighlight[]): string {
  return deriveStructuredNotes(request, highlights);
}

function buildCodexCliPrompt(
  request: ParsedSummaryRequest,
  logger?: pino.Logger,
  healthContext?: HealthContext
): string {
  const payload = groundingFacade.buildSummaryRequestPayload(request, {
    logger,
    healthContext,
  });
  const maxTopics = resolveHighlightLimit(request, request.topics.length);
  const maxEvidencePerTopic =
    request.budget?.maxEvidencePerTopic ??
    Math.max(...request.topics.map((topic) => topic.evidence.length), 0);
  const maxOutputTokens = request.budget?.maxOutputTokens ?? 1200;

  const promptSections = [
    "You are generating a human-readable engineering intelligence brief from a structured summary request.",
    "Use only the evidence included in SUMMARY_REQUEST_JSON. Do not invent facts or URLs.",
    "Trend metrics (score, volume, acceleration) are ranking inputs only. Do not repeat these numbers in highlights.",
    "Do not write phrases like '<topic> reached score ...'. Summarize concrete events from evidence (releases, incidents, CVEs, deprecations, region/feature launches, policy/pricing changes).",
    "Each highlight must include concrete what_happened, why_it_matters, suggested_action, and citations.",
    "Do not emit duplicate topics in highlights. If multiple points map to the same topic, combine them into one highlight.",
    `Keep output concise and practical. Limit highlights to at most ${maxTopics} and per-topic evidence references to at most ${maxEvidencePerTopic}.`,
    `Target no more than ${maxOutputTokens} tokens in total output.`,
    "Return valid JSON only with this shape:",
    '{ "title": string, "highlights": [{ "topic": string, "what_happened": string, "why_it_matters": string, "suggested_action": string, "citations": string[] }], "notes": string, "usage": { "prompt_tokens": number, "completion_tokens": number }, "meta": { "provider": string, "model": string, "estimated_cost_usd": number } }',
    "If usage or cost are unknown, set them to 0.",
  ];

  promptSections.push(
    "STANDARD NOTES FORMAT (always required): Render notes as markdown using this structure in order:",
    "# State of Signals and Where They're Going",
    "## Method and scope",
    "## Dominant shifts",
    "## Topic landscape",
    "## Risk and governance",
    "## Execution and economics",
    "## Outlook",
    "If notes include URLs, every URL MUST come from the evidence in SUMMARY_REQUEST_JSON."
  );

  promptSections.push(`SUMMARY_REQUEST_JSON:\n${JSON.stringify(payload, null, 2)}`);
  return promptSections.join("\n\n");
}

async function callHttpLlm(
  config: Config,
  request: ParsedSummaryRequest,
  logger: pino.Logger,
  healthContext?: HealthContext
): Promise<ParsedLlmResponse> {
  let response: Response;
  try {
    response = await fetch(config.LLM_ENDPOINT_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(
        groundingFacade.buildSummaryRequestPayload(request, {
          logger,
          healthContext,
        })
      ),
      signal: AbortSignal.timeout(config.LLM_TIMEOUT_MS),
    });
  } catch (error) {
    throw new LlmGenerationError(
      `LLM endpoint request failed: ${error instanceof Error ? error.message : "unknown error"}`
    );
  }

  if (!response.ok) {
    const responseBody = await response.text();
    logger.warn(
      {
        status: response.status,
        body: responseBody.slice(0, 512),
      },
      "LLM endpoint returned non-2xx status"
    );
    throw new LlmGenerationError(`LLM endpoint returned HTTP ${response.status}`);
  }

  let decoded: unknown;
  try {
    decoded = await response.json();
  } catch (error) {
    throw new LlmGenerationError(
      `LLM endpoint returned invalid JSON: ${error instanceof Error ? error.message : "unknown error"}`
    );
  }

  const parsed = LlmResponseSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new LlmGenerationError(`LLM endpoint response validation failed: ${parsed.error.issues[0]?.message}`);
  }

  return parsed.data;
}

async function callCodexCliLlm(
  config: Config,
  request: ParsedSummaryRequest,
  logger: pino.Logger,
  healthContext?: HealthContext
): Promise<ParsedLlmResponse> {
  const prompt = buildCodexCliPrompt(request, logger, healthContext);
  let decoded: unknown;
  try {
    decoded = await executeCodexCli(config, prompt, logger);
  } catch (error) {
    throw new LlmGenerationError(
      `Codex CLI request failed: ${error instanceof Error ? error.message : "unknown error"}`
    );
  }

  const parsed = LlmResponseSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new LlmGenerationError(
      `Codex CLI response validation failed: ${parsed.error.issues[0]?.message}`
    );
  }

  return parsed.data;
}

function buildSuccessPayload(
  request: ParsedSummaryRequest,
  producedAt: Date,
  title: string,
  highlights: NormalizedHighlight[],
  notes: string,
  provider: string,
  model: string,
  inputTokens: number,
  outputTokens: number,
  costUsd: number
): SuccessResult {
  const window = request.type === "daily" ? 1 : 2;
  const totalCitations = highlights.reduce((sum, highlight) => sum + highlight.citations.length, 0);

  return {
    payload: {
      request_id: request.requestId,
      produced_at: producedAt.toISOString(),
      brief: {
        brief_id: `brief:${request.requestId}`,
        generated_at: producedAt.toISOString(),
        window,
        title,
        highlights,
        notes,
        meta: {
          provider,
          model,
          input_tokens: inputTokens,
          output_tokens: outputTokens,
          estimated_cost_usd: costUsd,
        },
      },
    },
    metrics: {
      highlightsCount: highlights.length,
      citationsCount: totalCitations,
      inputTokens,
      outputTokens,
      costUsd,
    },
  };
}

function enforceGroundedHighlights(
  request: ParsedSummaryRequest,
  highlights: NormalizedHighlight[]
): NormalizedHighlight[] {
  const evidenceUrls = groundingFacade.createEvidenceUrlSet(request);
  const topicEvidenceScopes = buildTopicEvidenceScopes(request);
  if (evidenceUrls.size === 0) {
    throw new NonRetryableProcessingError("No evidence URLs were provided in the summary request");
  }

  const groundedHighlights = highlights
    .map((highlight) => {
      const globallyGroundedCitations = groundingFacade.filterGroundedCitations(
        highlight.citations,
        evidenceUrls
      );
      if (globallyGroundedCitations.length === 0) {
        return null;
      }

      const topicScope = resolveGroundedTopicScope(
        topicEvidenceScopes,
        normalizeTopicKey(highlight.topic),
        globallyGroundedCitations
      );
      if (!topicScope) {
        return null;
      }

      const topicScopedCitations = globallyGroundedCitations.filter((citation) =>
        topicScope.evidenceUrls.has(citation)
      );
      if (topicScopedCitations.length === 0) {
        return null;
      }

      return {
        ...highlight,
        topic: topicScope.canonicalTopic,
        citations: topicScopedCitations,
      };
    })
    .filter((highlight): highlight is NormalizedHighlight => highlight !== null)
    .filter((highlight) => highlight.citations.length > 0);

  const mergedHighlights = mergeHighlightsByTopic(groundedHighlights);

  if (mergedHighlights.length === 0) {
    throw new NonRetryableProcessingError(
      "Brief generation produced no grounded highlights with valid evidence citations"
    );
  }

  return mergedHighlights;
}

interface BuildSuccessResultInput {
  ctx: ProcessContext;
  request: ParsedSummaryRequest;
  producedAt: Date;
  estimatedCostUsd: number;
}

interface LlmProviderStrategy {
  readonly provider: LlmProvider;
  build(input: BuildSuccessResultInput): Promise<SuccessResult>;
}

function isNoGroundedHighlightError(error: unknown): boolean {
  return (
    error instanceof NonRetryableProcessingError &&
    error.message === "Brief generation produced no grounded highlights with valid evidence citations"
  );
}

function isCodexOutputArtifactError(error: unknown): boolean {
  if (!(error instanceof LlmGenerationError)) {
    return false;
  }
  const message = error.message.toLowerCase();
  const isArtifactMissing =
    (message.includes("last-message.txt") || message.includes("output file missing")) &&
    message.includes("enoent");
  const isTempStorageFailure =
    (message.includes("enospc") ||
      message.includes("no space left on device") ||
      message.includes("eacces") ||
      message.includes("permission denied")) &&
    (message.includes("mkdtemp") ||
      message.includes("mkdir") ||
      message.includes("codex-tmp") ||
      message.includes(".tmp"));

  return (
    message.includes("codex cli request failed") &&
    (isArtifactMissing || isTempStorageFailure)
  );
}

function resolveHighlightLimit(request: ParsedSummaryRequest, availableCount: number): number {
  if (request.query) {
    return availableCount;
  }

  const maxTopics = request.budget?.maxTopics;
  if (typeof maxTopics !== "number" || !Number.isInteger(maxTopics) || maxTopics <= 0) {
    return availableCount;
  }

  return Math.min(maxTopics, availableCount);
}

function selectInternalFallbackTopics(
  request: ParsedSummaryRequest,
  preferredCount?: number,
  conciseQueryMode = false
): ParsedSummaryTopic[] {
  let limit = resolveHighlightLimit(request, request.topics.length);

  // Query-mode can include many subtopics for evidence selection; apply concise cap only for fallback paths.
  if (conciseQueryMode && request.query) {
    const maxTopics = request.budget?.maxTopics;
    if (typeof maxTopics === "number" && Number.isInteger(maxTopics) && maxTopics > 0) {
      limit = Math.min(limit, maxTopics);
    }
  }

  if (typeof preferredCount === "number" && Number.isInteger(preferredCount) && preferredCount > 0) {
    limit = Math.min(limit, preferredCount);
  }

  const boundedLimit = Math.max(1, Math.min(limit, request.topics.length));
  return request.topics.slice(0, boundedLimit);
}

function appendCoverageWarnings(notes: string, request: ParsedSummaryRequest): string {
  if (!request.coverageWarnings || request.coverageWarnings.length === 0) {
    return notes;
  }

  const warningsText = request.coverageWarnings.join(" ");
  return notes ? `${notes}\n\nCoverage Note: ${warningsText}` : `Coverage Note: ${warningsText}`;
}

function buildInternalSuccessResult(
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number,
  preferredTopicCount?: number,
  conciseQueryMode = false,
  provider = "internal",
  model = "rule-based-v1"
): SuccessResult {
  const topics = selectInternalFallbackTopics(request, preferredTopicCount, conciseQueryMode);
  const highlights = enforceGroundedHighlights(
    request,
    topics.map((topic) => buildInternalHighlight(topic))
  );
  const inputTokens = estimateTokenCount(JSON.stringify(request));
  const outputTokens = estimateTokenCount(JSON.stringify(highlights));
  let notes = deriveDefaultNotes(request, highlights);
  notes = appendCoverageWarnings(notes, request);
  notes = groundingFacade.enforceGroundedNotes(request, notes, toGroundingError);

  return buildSuccessPayload(
    request,
    producedAt,
    `Trend Brief ${producedAt.toISOString().slice(0, 10)}`,
    highlights,
    notes,
    provider,
    model,
    inputTokens,
    outputTokens,
    normalizeUsd(estimatedCostUsd)
  );
}

function buildLlmBackedSuccessResult(
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number,
  llmResponse: ParsedLlmResponse,
  defaultProvider: string,
  defaultModel: string,
  logger: pino.Logger
): SuccessResult {
  const maxTopics = resolveHighlightLimit(request, llmResponse.highlights.length);
  const llmHighlights = llmResponse.highlights.slice(0, maxTopics).map(normalizeLlmHighlight);
  let highlights: NormalizedHighlight[];
  let usedInternalFallback = false;

  try {
    highlights = enforceGroundedHighlights(request, llmHighlights);
  } catch (error) {
    if (!isNoGroundedHighlightError(error)) {
      throw error;
    }

    const fallbackTopics = selectInternalFallbackTopics(request, llmHighlights.length, true);
    highlights = enforceGroundedHighlights(
      request,
      fallbackTopics.map((topic) => buildInternalHighlight(topic))
    );
    usedInternalFallback = true;
    logger.warn(
      {
        requestId: request.requestId,
        llmHighlightCount: llmHighlights.length,
        fallbackHighlightCount: highlights.length,
      },
      "LLM highlights failed grounding; using internal grounded fallback highlights"
    );
  }
  const inputTokens = llmResponse.usage?.prompt_tokens ?? estimateTokenCount(JSON.stringify(request));
  const outputTokens =
    llmResponse.usage?.completion_tokens ?? estimateTokenCount(JSON.stringify(highlights));

  let notes = usedInternalFallback
    ? deriveDefaultNotes(request, highlights)
    : llmResponse.notes?.trim() || deriveDefaultNotes(request, highlights);
  notes = appendCoverageWarnings(notes, request);
  notes = groundingFacade.enforceGroundedNotes(request, notes, toGroundingError);

  return buildSuccessPayload(
    request,
    producedAt,
    llmResponse.title,
    highlights,
    notes,
    usedInternalFallback ? "internal" : llmResponse.meta?.provider?.trim() || defaultProvider,
    usedInternalFallback ? "rule-based-fallback-v1" : llmResponse.meta?.model?.trim() || defaultModel,
    inputTokens,
    outputTokens,
    normalizeUsd(llmResponse.meta?.estimated_cost_usd ?? estimatedCostUsd)
  );
}

async function buildHttpSuccessResult(input: BuildSuccessResultInput): Promise<SuccessResult> {
  const { ctx, request, producedAt, estimatedCostUsd } = input;
  const llmResponse = await callHttpLlm(ctx.config, request, ctx.logger, ctx.healthContext);
  return buildLlmBackedSuccessResult(
    request,
    producedAt,
    estimatedCostUsd,
    llmResponse,
    "http",
    "http-v1",
    ctx.logger
  );
}

async function buildCodexCliSuccessResult(input: BuildSuccessResultInput): Promise<SuccessResult> {
  const { ctx, request, producedAt, estimatedCostUsd } = input;
  try {
    const llmResponse = await callCodexCliLlm(ctx.config, request, ctx.logger, ctx.healthContext);
    const defaultModel = ctx.config.LLM_CODEX_MODEL.trim() || "codex-cli";

    return buildLlmBackedSuccessResult(
      request,
      producedAt,
      estimatedCostUsd,
      llmResponse,
      "codex-cli",
      defaultModel,
      ctx.logger
    );
  } catch (error) {
    if (!isCodexOutputArtifactError(error)) {
      throw error;
    }

    ctx.logger.warn(
      {
        requestId: request.requestId,
        error: serializeError(error),
      },
      "Codex CLI execution unavailable; using internal grounded fallback brief"
    );
    return buildInternalSuccessResult(
      request,
      producedAt,
      estimatedCostUsd,
      request.budget?.maxTopics,
      true,
      "internal",
      "rule-based-fallback-v1"
    );
  }
}

const LLM_PROVIDER_STRATEGIES: Record<LlmProvider, LlmProviderStrategy> = {
  internal: {
    provider: "internal",
    async build({ request, producedAt, estimatedCostUsd }) {
      return buildInternalSuccessResult(request, producedAt, estimatedCostUsd);
    },
  },
  http: {
    provider: "http",
    build: buildHttpSuccessResult,
  },
  "codex-cli": {
    provider: "codex-cli",
    build: buildCodexCliSuccessResult,
  },
};

function resolveLlmProvider(ctx: ProcessContext, request: ParsedSummaryRequest): LlmProvider {
  return request.llmProvider ?? ctx.config.LLM_PROVIDER;
}

async function buildSuccessResult(
  ctx: ProcessContext,
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number
): Promise<SuccessResult> {
  const llmProvider = resolveLlmProvider(ctx, request);
  const strategy = LLM_PROVIDER_STRATEGIES[llmProvider];
  if (!strategy) {
    throw new LlmGenerationError(`Unsupported LLM provider: ${llmProvider}`);
  }

  return strategy.build({
    ctx,
    request,
    producedAt,
    estimatedCostUsd,
  });
}

async function republishPersistedResult(
  resultStore: BriefResultStore,
  requestId: string,
  publisher: BriefResultPublisher<BriefResultPayload>
): Promise<BriefStatus | null> {
  const existing = await resultStore.load(requestId);
  if (!existing) {
    return null;
  }

  await publisher.publishResult(requestId, existing.payload);
  return existing.status;
}

async function rollbackBudgetReservation(
  ctx: ProcessContext,
  budgetLedger: BriefBudgetLedger,
  logger: pino.Logger,
  dateKey: string,
  reservedAmountUsd: number,
  dailyBudgetUsd: number
): Promise<void> {
  const spentBudgetUsd = await budgetLedger.release({
    dateKey,
    amountUsd: reservedAmountUsd,
  });
  ctx.healthContext.redisHealthy = true;
  setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
  logger.info({ spentBudgetUsd }, "Rolled back brief budget reservation");
}

async function emitFailureResult(
  ctx: ProcessContext,
  resultStore: BriefResultStore,
  publisher: BriefResultPublisher<BriefResultPayload>,
  requestId: string,
  producedAt: Date,
  code: string,
  message: string,
  retryable: boolean
): Promise<void> {
  const failureResult = buildFailureBriefResultPayload(
    requestId,
    producedAt,
    code,
    message,
    retryable
  );
  const persisted = await resultStore.persist(failureResult, BriefStatus.failure);
  if (persisted === "duplicate") {
    incrementDuplicatesSkipped(ctx.healthContext);
    const republishedStatus = await republishPersistedResult(
      resultStore,
      requestId,
      publisher
    );
    if (!republishedStatus) {
      throw new Error(`Unable to republish existing failure result for request ${requestId}`);
    }
    return;
  }

  await publisher.publishResult(requestId, failureResult);
}

function mapNonRetryableFailureMetric(
  error: NonRetryableProcessingError
): "grounding_error" | "generation_error" {
  return error.code === "grounding_error" ? "grounding_error" : "generation_error";
}

interface HandleNonRetryableFailureInput {
  ctx: ProcessContext;
  resultStore: BriefResultStore;
  publisher: BriefResultPublisher<BriefResultPayload>;
  requestId: string;
  producedAt: Date;
  error: NonRetryableProcessingError;
  logger: pino.Logger;
  logMessage: string;
}

async function handleNonRetryableFailure(
  input: HandleNonRetryableFailureInput
): Promise<void> {
  const {
    ctx,
    resultStore,
    publisher,
    requestId,
    producedAt,
    error,
    logger,
    logMessage,
  } = input;

  incrementError(ctx.healthContext, mapNonRetryableFailureMetric(error));
  incrementGeneration(ctx.healthContext, "failure");
  logger.warn({ error: serializeError(error) }, logMessage);
  await emitFailureResult(
    ctx,
    resultStore,
    publisher,
    requestId,
    producedAt,
    error.code,
    error.message,
    false
  );
}

type SummaryRequestFailureHandlerOutcome = "handled" | "rethrow";

interface SummaryRequestFailureHandlingContext {
  ctx: ProcessContext;
  resultStore: BriefResultStore;
  publisher: BriefResultPublisher<BriefResultPayload>;
  requestId: string;
  producedAt: Date;
  logger: pino.Logger;
  persistedCreated: boolean;
}

interface SummaryRequestFailureHandler {
  readonly name: string;
  canHandle(
    error: unknown,
    context: SummaryRequestFailureHandlingContext
  ): boolean;
  handle(
    error: unknown,
    context: SummaryRequestFailureHandlingContext
  ): Promise<SummaryRequestFailureHandlerOutcome>;
}

const NON_RETRYABLE_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "non-retryable",
  canHandle(error): boolean {
    return error instanceof NonRetryableProcessingError;
  },
  async handle(error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    if (!(error instanceof NonRetryableProcessingError)) {
      return "rethrow";
    }

    await handleNonRetryableFailure({
      ctx: context.ctx,
      resultStore: context.resultStore,
      publisher: context.publisher,
      requestId: context.requestId,
      producedAt: context.producedAt,
      error,
      logger: context.logger,
      logMessage: "Brief request failed non-retryable validation",
    });
    return "handled";
  },
};

const PERSISTED_RESULT_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "persisted-result-publish",
  canHandle(_error, context): boolean {
    return context.persistedCreated;
  },
  async handle(error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    incrementError(context.ctx.healthContext, "publish_error");
    context.logger.error(
      { error: serializeError(error) },
      "Persisted brief result but failed to publish; will retry from Kafka"
    );
    return "rethrow";
  },
};

const RETRYABLE_LLM_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "retryable-llm",
  canHandle(error): boolean {
    return error instanceof LlmGenerationError;
  },
  async handle(error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    if (!(error instanceof LlmGenerationError)) {
      return "rethrow";
    }

    const failureCode = classifyRetryableFailureCode(error);
    incrementError(context.ctx.healthContext, failureCode);
    incrementGeneration(context.ctx.healthContext, "failure");
    context.logger.error(
      { error: serializeError(error), failureCode },
      "Failed to process summary request due to retryable LLM error"
    );
    await emitFailureResult(
      context.ctx,
      context.resultStore,
      context.publisher,
      context.requestId,
      context.producedAt,
      failureCode,
      error.message,
      true
    );
    return "handled";
  },
};

const UNKNOWN_SUMMARY_REQUEST_FAILURE_HANDLER: SummaryRequestFailureHandler = {
  name: "unknown",
  canHandle(): boolean {
    return true;
  },
  async handle(_error, context): Promise<SummaryRequestFailureHandlerOutcome> {
    incrementError(
      context.ctx.healthContext,
      "generation_error"
    );
    incrementGeneration(context.ctx.healthContext, "failure");
    context.logger.error({ error: serializeError(_error) }, "Failed to process summary request");
    return "rethrow";
  },
};

const SUMMARY_REQUEST_FAILURE_HANDLERS: readonly SummaryRequestFailureHandler[] = [
  NON_RETRYABLE_SUMMARY_REQUEST_FAILURE_HANDLER,
  PERSISTED_RESULT_SUMMARY_REQUEST_FAILURE_HANDLER,
  RETRYABLE_LLM_SUMMARY_REQUEST_FAILURE_HANDLER,
  UNKNOWN_SUMMARY_REQUEST_FAILURE_HANDLER,
];

async function handleSummaryRequestFailure(
  error: unknown,
  context: SummaryRequestFailureHandlingContext
): Promise<SummaryRequestFailureHandlerOutcome> {
  for (const handler of SUMMARY_REQUEST_FAILURE_HANDLERS) {
    if (!handler.canHandle(error, context)) {
      continue;
    }
    return handler.handle(error, context);
  }

  return "rethrow";
}

class DefaultSummaryRequestProcessor implements SummaryRequestProcessor {
  private readonly runtimeFactory: SummaryRequestRuntimeFactory;

  constructor(dependencies: SummaryRequestProcessorDependencies) {
    this.runtimeFactory = new SummaryRequestRuntimeFactory(dependencies);
  }

  async processSummaryRequest(
    ctx: ProcessContext,
    request: ParsedSummaryRequest
  ): Promise<void> {
    const runtime = this.runtimeFactory.create(ctx, request);
    const {
      logger,
      budgetLedger,
      publisher,
      resultStore,
      queryModeRequestResolver,
      producedAt,
    } = runtime;
    let existingResult: StoredBriefResult | null = null;

    try {
      existingResult = await resultStore.load(request.requestId);
    } catch (error) {
      incrementError(ctx.healthContext, "idempotency_error");
      logger.error({ error: serializeError(error) }, "Failed to load persisted brief result");
      throw error;
    }

    if (existingResult) {
      incrementDuplicatesSkipped(ctx.healthContext);
      incrementGeneration(ctx.healthContext, "skipped");
      try {
        await publisher.publishResult(request.requestId, existingResult.payload);
        logger.info({ status: existingResult.status }, "Republished persisted brief result for duplicate request");
        return;
      } catch (error) {
        incrementError(ctx.healthContext, "publish_error");
        logger.error(
          { error: serializeError(error) },
          "Failed to republish persisted brief result for duplicate request"
        );
        throw error;
      }
    }

    let requestForGeneration: ParsedSummaryRequest;
    try {
      requestForGeneration = await queryModeRequestResolver.resolve(ctx, request, logger);
    } catch (error) {
      if (error instanceof NonRetryableProcessingError) {
        await handleNonRetryableFailure({
          ctx,
          resultStore,
          publisher,
          requestId: request.requestId,
          producedAt,
          error,
          logger,
          logMessage: "Summary request failed non-retryable pre-processing",
        });
        return;
      }

      incrementError(ctx.healthContext, "generation_error");
      incrementGeneration(ctx.healthContext, "failure");
      logger.error({ error: serializeError(error) }, "Failed to resolve summary request");
      throw error;
    }

    const dateKey = getBudgetDateKey(producedAt);
    const dailyBudgetUsd = requestForGeneration.budget?.dailyBudgetUsd ?? ctx.config.LLM_DAILY_BUDGET_USD;
    const estimatedCostUsd = normalizeUsd(estimateRequestCostUsd(requestForGeneration));
    let budgetReserved = false;
    let spentBudgetUsd = 0;
    let reservedCostUsd = estimatedCostUsd;

    try {
      const reservation = await budgetLedger.reserve({
        dateKey,
        dailyBudgetUsd,
        amountUsd: reservedCostUsd,
      });
      budgetReserved = reservation.reserved;
      spentBudgetUsd = reservation.spentUsd;
      ctx.healthContext.redisHealthy = true;
      setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
    } catch (error) {
      ctx.healthContext.redisHealthy = false;
      incrementError(ctx.healthContext, "redis_error");
      logger.error({ error: serializeError(error) }, "Failed to reserve daily budget");
      throw error;
    }

    if (!budgetReserved) {
      incrementBudgetExceeded(ctx.healthContext);
      incrementGeneration(ctx.healthContext, "skipped");
      await emitFailureResult(
        ctx,
        resultStore,
        publisher,
        request.requestId,
        producedAt,
        "budget_exceeded",
        "Daily brief budget exceeded",
        false
      );
      logger.info(
        {
          spentBudgetUsd,
          reservedCostUsd,
          dailyBudgetUsd,
        },
        "Skipped summary request due to budget limit"
      );
      return;
    }

    let persistedCreated = false;
    try {
      const successResult = await buildSuccessResult(
        ctx,
        requestForGeneration,
        producedAt,
        estimatedCostUsd
      );
      const persisted = await resultStore.persist(successResult.payload, BriefStatus.success);
      if (persisted === "duplicate") {
        await rollbackBudgetReservation(
          ctx,
          budgetLedger,
          logger,
          dateKey,
          reservedCostUsd,
          dailyBudgetUsd
        );
        budgetReserved = false;
        incrementDuplicatesSkipped(ctx.healthContext);
        incrementGeneration(ctx.healthContext, "skipped");
        const republishedStatus = await republishPersistedResult(
          resultStore,
          request.requestId,
          publisher
        );
        if (!republishedStatus) {
          throw new Error(`Persisted result missing after duplicate insert for request ${request.requestId}`);
        }
        logger.info({ status: republishedStatus }, "Detected duplicate during persist and republished stored result");
        return;
      }
      persistedCreated = true;

      const costDeltaUsd = normalizeUsdDelta(successResult.metrics.costUsd - reservedCostUsd);
      if (costDeltaUsd !== 0) {
        try {
          spentBudgetUsd = await budgetLedger.settle({
            dateKey,
            deltaUsd: costDeltaUsd,
          });
          reservedCostUsd = normalizeUsd(successResult.metrics.costUsd);
          ctx.healthContext.redisHealthy = true;
        } catch (error) {
          ctx.healthContext.redisHealthy = false;
          incrementError(ctx.healthContext, "redis_error");
          logger.warn(
            {
              costDeltaUsd,
              error: serializeError(error),
            },
            "Failed to settle reserved budget to final brief cost"
          );
        }
      }

      await publisher.publishResult(request.requestId, successResult.payload);

      incrementGeneration(ctx.healthContext, "success");
      incrementLlmCostUsd(ctx.healthContext, successResult.metrics.costUsd);
      incrementLlmTokens(ctx.healthContext, "input", successResult.metrics.inputTokens);
      incrementLlmTokens(ctx.healthContext, "output", successResult.metrics.outputTokens);
      observeHighlightsCount(ctx.healthContext, successResult.metrics.highlightsCount);
      observeCitationsCount(ctx.healthContext, successResult.metrics.citationsCount);

      setBudgetRemainingUsd(ctx.healthContext, Math.max(0, dailyBudgetUsd - spentBudgetUsd));
      logger.info(
        {
          topicCount: successResult.metrics.highlightsCount,
          citationsCount: successResult.metrics.citationsCount,
          costUsd: successResult.metrics.costUsd,
        },
        "Summary request processed"
      );
    } catch (error) {
      if (budgetReserved && !persistedCreated) {
        try {
          await rollbackBudgetReservation(
            ctx,
            budgetLedger,
            logger,
            dateKey,
            reservedCostUsd,
            dailyBudgetUsd
          );
          budgetReserved = false;
        } catch (rollbackError) {
          ctx.healthContext.redisHealthy = false;
          incrementError(ctx.healthContext, "redis_error");
          logger.error(
            { error: serializeError(rollbackError) },
            "Failed to roll back reserved brief budget"
          );
          throw rollbackError;
        }
      }

      const failureOutcome = await handleSummaryRequestFailure(error, {
        ctx,
        resultStore,
        publisher,
        requestId: request.requestId,
        producedAt,
        logger,
        persistedCreated,
      });
      if (failureOutcome === "handled") {
        return;
      }

      throw error;
    }
  }
}

export function createSummaryRequestProcessor(
  overrides: SummaryRequestProcessorDependencyOverrides = {}
): SummaryRequestProcessor {
  const dependencies = buildFunctionDependencies(
    "Summary request processor dependency",
    DEFAULT_SUMMARY_REQUEST_PROCESSOR_DEPENDENCIES,
    overrides
  );
  return new DefaultSummaryRequestProcessor(dependencies);
}

const DEFAULT_SUMMARY_REQUEST_PROCESSOR = createSummaryRequestProcessor();

export async function processSummaryRequest(
  ctx: ProcessContext,
  request: ParsedSummaryRequest
): Promise<void> {
  await DEFAULT_SUMMARY_REQUEST_PROCESSOR.processSummaryRequest(ctx, request);
}
