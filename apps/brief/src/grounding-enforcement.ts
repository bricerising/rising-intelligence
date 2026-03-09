/**
 * Grounding enforcement for brief highlights and notes.
 *
 * Ensures every citation in a generated brief traces back to evidence
 * provided in the original summary request.  Highlights with no grounded
 * citations are discarded; duplicate topic highlights are merged.
 */

import type { SummaryRequestGroundingFacade } from "./grounding-facade.js";
import { NonRetryableProcessingError } from "./processing-errors.js";
import type { ParsedSummaryRequest } from "./types.js";
import {
  normalizeTopicKey,
  normalizeTextFingerprint,
  ensureSentenceEnding,
  type NormalizedHighlight,
} from "./evidence-scoring.js";
import { getTopLevelTopicGroup } from "./query-mode-selection.js";

// ── Topic evidence scoping ──────────────────────────────────────────────────

interface TopicEvidenceScope {
  canonicalTopic: string;
  normalizedTopic: string;
  topLevelGroup: string;
  evidenceUrls: Set<string>;
}

function buildTopicEvidenceScopes(
  groundingFacade: SummaryRequestGroundingFacade,
  request: ParsedSummaryRequest
): Map<string, TopicEvidenceScope> {
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

// ── Highlight merging ───────────────────────────────────────────────────────

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

function mergeHighlightsByTopic(
  groundingFacade: SummaryRequestGroundingFacade,
  highlights: NormalizedHighlight[]
): NormalizedHighlight[] {
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

// ── Grounding enforcement ───────────────────────────────────────────────────

export function enforceGroundedHighlights(
  groundingFacade: SummaryRequestGroundingFacade,
  request: ParsedSummaryRequest,
  highlights: NormalizedHighlight[]
): NormalizedHighlight[] {
  const evidenceUrls = groundingFacade.createEvidenceUrlSet(request);
  const topicEvidenceScopes = buildTopicEvidenceScopes(groundingFacade, request);
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

  const mergedHighlights = mergeHighlightsByTopic(groundingFacade, groundedHighlights);

  if (mergedHighlights.length === 0) {
    throw new NonRetryableProcessingError(
      "Brief generation produced no grounded highlights with valid evidence citations"
    );
  }

  return mergedHighlights;
}

export function normalizeLlmHighlight(
  groundingFacade: SummaryRequestGroundingFacade,
  highlight: NormalizedHighlight
): NormalizedHighlight {
  return {
    topic: highlight.topic.trim(),
    what_happened: highlight.what_happened.trim(),
    why_it_matters: highlight.why_it_matters.trim(),
    suggested_action: highlight.suggested_action.trim(),
    citations: groundingFacade.dedupeCanonicalUrls(highlight.citations),
  };
}
