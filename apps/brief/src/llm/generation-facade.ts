/**
 * LLM generation facade.
 *
 * Encapsulates the provider-selection strategy (internal / http / codex-cli)
 * and the mapping from raw LLM responses to grounded brief results.
 */

import { serializeError } from "@rising-intelligence/shared/errors";
import type pino from "pino";
import { z } from "zod";
import type { SummaryRequestGroundingFacade } from "../grounding-facade.js";
import {
  LlmGenerationError,
  NonRetryableProcessingError,
  toGroundingError,
} from "../processing-errors.js";
import type { HealthContext } from "../health.js";
import { executeCodexCli } from "./codex-cli.js";
import type { Config } from "../config.js";
import type {
  LlmProvider,
  ParsedSummaryRequest,
  ParsedSummaryTopic,
} from "../types.js";
import {
  ensureSentenceEnding,
  normalizeWhitespace,
  normalizeTopicKey,
  buildInternalHighlight,
  type NormalizedHighlight,
} from "../evidence-scoring.js";
import {
  enforceGroundedHighlights,
  normalizeLlmHighlight,
} from "../grounding-enforcement.js";

// ── LLM response schema ────────────────────────────────────────────────────

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

type ParsedLlmResponse = z.infer<typeof LlmResponseSchema>;

// ── Cost and token estimation ───────────────────────────────────────────────

export function estimateTokenCount(text: string): number {
  return Math.max(1, Math.ceil(text.length / 4));
}

export function estimateRequestCostUsd(request: ParsedSummaryRequest): number {
  const evidenceCount = request.topics.reduce((sum, topic) => sum + topic.evidence.length, 0);
  return Number((0.01 + request.topics.length * 0.002 + evidenceCount * 0.0005).toFixed(4));
}

export function normalizeUsd(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  if (value <= 0) {
    return 0;
  }

  return Number(value.toFixed(6));
}

export function normalizeUsdDelta(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }

  const rounded = Number(value.toFixed(6));
  return rounded === 0 ? 0 : rounded;
}

// ── Success result types ────────────────────────────────────────────────────

export interface SuccessResult {
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

export interface BuildSuccessResultInput {
  ctx: BuildSuccessResultContext;
  request: ParsedSummaryRequest;
  producedAt: Date;
  estimatedCostUsd: number;
}

export interface BuildSuccessResultContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
}

// ── Shared builders ─────────────────────────────────────────────────────────

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

// ── Notes derivation ────────────────────────────────────────────────────────

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

function appendCoverageWarnings(notes: string, request: ParsedSummaryRequest): string {
  if (!request.coverageWarnings || request.coverageWarnings.length === 0) {
    return notes;
  }

  const warningsText = request.coverageWarnings.join(" ");
  return notes ? `${notes}\n\nCoverage Note: ${warningsText}` : `Coverage Note: ${warningsText}`;
}

// ── Internal (rule-based) generation ────────────────────────────────────────

function buildInternalSuccessResult(
  groundingFacade: SummaryRequestGroundingFacade,
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
    groundingFacade,
    request,
    topics.map((topic) => buildInternalHighlight(groundingFacade, topic))
  );
  const inputTokens = estimateTokenCount(JSON.stringify(request));
  const outputTokens = estimateTokenCount(JSON.stringify(highlights));
  let notes = deriveStructuredNotes(request, highlights);
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

// ── LLM-backed generation helpers ───────────────────────────────────────────

function normalizeLlmMetaString(value: string | undefined): string | null {
  const normalized = value?.trim();
  if (!normalized) {
    return null;
  }

  const lowerCased = normalized.toLowerCase();
  if (
    lowerCased === "unknown" ||
    lowerCased === "n/a" ||
    lowerCased === "na" ||
    lowerCased === "none" ||
    lowerCased === "null" ||
    lowerCased === "unspecified"
  ) {
    return null;
  }

  return normalized;
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

function buildLlmBackedSuccessResult(
  groundingFacade: SummaryRequestGroundingFacade,
  request: ParsedSummaryRequest,
  producedAt: Date,
  estimatedCostUsd: number,
  llmResponse: ParsedLlmResponse,
  defaultProvider: string,
  defaultModel: string,
  logger: pino.Logger
): SuccessResult {
  const maxTopics = resolveHighlightLimit(request, llmResponse.highlights.length);
  const llmHighlights = llmResponse.highlights.slice(0, maxTopics).map(
    (h) => normalizeLlmHighlight(groundingFacade, h)
  );
  let highlights: NormalizedHighlight[];
  let usedInternalFallback = false;

  try {
    highlights = enforceGroundedHighlights(groundingFacade, request, llmHighlights);
  } catch (error) {
    if (!isNoGroundedHighlightError(error)) {
      throw error;
    }

    const fallbackTopics = selectInternalFallbackTopics(request, llmHighlights.length, true);
    highlights = enforceGroundedHighlights(
      groundingFacade,
      request,
      fallbackTopics.map((topic) => buildInternalHighlight(groundingFacade, topic))
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
    ? deriveStructuredNotes(request, highlights)
    : llmResponse.notes?.trim() || deriveStructuredNotes(request, highlights);
  notes = appendCoverageWarnings(notes, request);
  notes = groundingFacade.enforceGroundedNotes(request, notes, toGroundingError);
  const provider = usedInternalFallback
    ? "internal"
    : normalizeLlmMetaString(llmResponse.meta?.provider) ?? defaultProvider;
  const model = usedInternalFallback
    ? "rule-based-fallback-v1"
    : normalizeLlmMetaString(llmResponse.meta?.model) ?? defaultModel;

  return buildSuccessPayload(
    request,
    producedAt,
    llmResponse.title,
    highlights,
    notes,
    provider,
    model,
    inputTokens,
    outputTokens,
    normalizeUsd(llmResponse.meta?.estimated_cost_usd ?? estimatedCostUsd)
  );
}

// ── Codex CLI prompt builder ────────────────────────────────────────────────

function buildCodexCliPrompt(
  groundingFacade: SummaryRequestGroundingFacade,
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

// ── HTTP LLM provider ───────────────────────────────────────────────────────

async function callHttpLlm(
  groundingFacade: SummaryRequestGroundingFacade,
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

// ── Codex CLI LLM provider ──────────────────────────────────────────────────

async function callCodexCliLlm(
  groundingFacade: SummaryRequestGroundingFacade,
  config: Config,
  request: ParsedSummaryRequest,
  logger: pino.Logger,
  healthContext?: HealthContext
): Promise<ParsedLlmResponse> {
  const prompt = buildCodexCliPrompt(groundingFacade, request, logger, healthContext);
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

// ── Provider strategy dispatch ──────────────────────────────────────────────

interface LlmProviderStrategy {
  readonly provider: LlmProvider;
  build(input: BuildSuccessResultInput): Promise<SuccessResult>;
}

function createLlmProviderStrategies(
  groundingFacade: SummaryRequestGroundingFacade
): Record<LlmProvider, LlmProviderStrategy> {
  return {
    internal: {
      provider: "internal",
      async build({ request, producedAt, estimatedCostUsd }) {
        return buildInternalSuccessResult(groundingFacade, request, producedAt, estimatedCostUsd);
      },
    },
    http: {
      provider: "http",
      async build({ ctx, request, producedAt, estimatedCostUsd }) {
        const llmResponse = await callHttpLlm(groundingFacade, ctx.config, request, ctx.logger, ctx.healthContext);
        return buildLlmBackedSuccessResult(
          groundingFacade,
          request,
          producedAt,
          estimatedCostUsd,
          llmResponse,
          "http",
          "http-v1",
          ctx.logger
        );
      },
    },
    "codex-cli": {
      provider: "codex-cli",
      async build({ ctx, request, producedAt, estimatedCostUsd }) {
        try {
          const llmResponse = await callCodexCliLlm(groundingFacade, ctx.config, request, ctx.logger, ctx.healthContext);
          const defaultModel = ctx.config.LLM_CODEX_MODEL.trim() || "codex-cli";

          return buildLlmBackedSuccessResult(
            groundingFacade,
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
            groundingFacade,
            request,
            producedAt,
            estimatedCostUsd,
            request.budget?.maxTopics,
            true,
            "internal",
            "rule-based-fallback-v1"
          );
        }
      },
    },
  };
}

// ── Public facade ───────────────────────────────────────────────────────────

export interface SummaryRequestGenerationFacade {
  buildSuccessResult(input: BuildSuccessResultInput): Promise<SuccessResult>;
}

export function createSummaryRequestGenerationFacade(
  groundingFacade: SummaryRequestGroundingFacade
): SummaryRequestGenerationFacade {
  const strategies = createLlmProviderStrategies(groundingFacade);

  return {
    async buildSuccessResult(input) {
      const llmProvider: LlmProvider = input.request.llmProvider ?? input.ctx.config.LLM_PROVIDER;
      const strategy = strategies[llmProvider];
      if (!strategy) {
        throw new LlmGenerationError(`Unsupported LLM provider: ${llmProvider}`);
      }

      return strategy.build(input);
    },
  };
}
