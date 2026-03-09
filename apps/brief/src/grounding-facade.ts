import {
  BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH,
  dedupeCanonicalBriefEvidenceUrls,
} from "@rising-intelligence/pipeline";
import type { ParsedSummaryRequest } from "./types.js";

const NOTES_URL_PATTERN = /https?:\/\/[^\s<>"'`]+/g;

interface CitationGroundingAdapter {
  dedupeCanonicalUrls(values: Array<string | null | undefined>): string[];
  createEvidenceUrlSet(request: ParsedSummaryRequest): Set<string>;
  filterGroundedCitations(citations: string[], evidenceUrls: Set<string>): string[];
  enforceGroundedNotes(
    request: ParsedSummaryRequest,
    notes: string,
    createError: (message: string) => Error
  ): string;
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
}

export {
  BRIEF_EVIDENCE_EXCERPT_MAX_LENGTH as EVIDENCE_EXCERPT_MAX_LENGTH,
};

function createCitationGroundingAdapter(): CitationGroundingAdapter {
  const dedupeCanonicalUrls = (values: Array<string | null | undefined>): string[] =>
    dedupeCanonicalBriefEvidenceUrls(values);

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

export function createSummaryRequestGroundingFacade(): SummaryRequestGroundingFacade {
  const citationGrounding = createCitationGroundingAdapter();

  return {
    dedupeCanonicalUrls: citationGrounding.dedupeCanonicalUrls,
    createEvidenceUrlSet: citationGrounding.createEvidenceUrlSet,
    filterGroundedCitations: citationGrounding.filterGroundedCitations,
    enforceGroundedNotes(request, notes, createError) {
      return citationGrounding.enforceGroundedNotes(request, notes, createError);
    },
  };
}
