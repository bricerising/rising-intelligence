import { describe, expect, it } from "vitest";
import {
  createSummaryRequestGroundingFacade,
  type ParsedSummaryRequest,
} from "../src/testing.js";

function makeRequest(evidenceUrl: string | null): ParsedSummaryRequest {
  return {
    requestId: "req-grounding",
    requestedAt: new Date("2026-02-10T00:00:00.000Z"),
    type: "daily",
    windows: [2],
    budget: null,
    query: null,
    report: null,
    topics: [
      {
        topic: "aws.bedrock",
        metrics: [],
        evidence: [
          {
            eventId: "evt-1",
            source: "rss",
            url: evidenceUrl,
            title: "Test evidence",
            publishedAt: null,
            fetchedAt: null,
            textExcerpt: "Test excerpt",
          },
        ],
      },
    ],
  };
}

describe("summary request grounding facade", () => {
  it("canonicalizes and deduplicates evidence URLs while filtering unsafe hosts", () => {
    const facade = createSummaryRequestGroundingFacade();

    const urls = facade.dedupeCanonicalUrls([
      "https://example.com/path/#fragment",
      "https://EXAMPLE.com/path",
      "http://[::1]/internal",
      "http://localhost/internal",
      "https://example.com/other",
    ]);

    expect(urls).toEqual([
      "https://example.com/path",
      "https://example.com/other",
    ]);
  });

  it("rejects notes that cite URLs outside the grounded evidence set", () => {
    const facade = createSummaryRequestGroundingFacade();
    const request = makeRequest("https://example.com/evidence");
    expect(() =>
      facade.enforceGroundedNotes(
        request,
        "Reference https://malicious.example.com/out-of-band",
        (message) => new Error(message)
      )
    ).toThrow("ungrounded URL citations");
  });
});
