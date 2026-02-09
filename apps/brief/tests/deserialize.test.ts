import { describe, expect, it } from "vitest";
import {
  deserializeSummaryRequest,
  parseSummaryRequestType,
} from "../src/deserialize.js";

function makeBuffer(payload: unknown): Buffer {
  return Buffer.from(JSON.stringify(payload), "utf-8");
}

describe("brief deserializeSummaryRequest", () => {
  it("deserializes valid requests", () => {
    const payload = {
      request_id: "req-1",
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
      windows: [1, "2"],
      budget: {
        daily_budget_usd: 5,
        max_topics: 3,
        max_evidence_per_topic: 4,
        max_output_tokens: 1200,
      },
      topics: [
        {
          topic: "aws.bedrock",
          metrics: [
            {
              topic: "aws.bedrock",
              window: 2,
              score: 12.5,
              volume: 21,
              acceleration: 0.8,
            },
          ],
          evidence: [
            {
              event_id: "evt-1",
              source: 3,
              url: "https://example.com/1",
              title: "Bedrock update",
              published_at: "2026-02-06T09:00:00.000Z",
              fetched_at: "2026-02-06T09:30:00.000Z",
              text_excerpt: "Details",
            },
          ],
        },
      ],
    };

    const parsed = deserializeSummaryRequest(makeBuffer(payload));

    expect(parsed.requestId).toBe("req-1");
    expect(parsed.type).toBe("daily");
    expect(parsed.windows).toEqual([1, 2]);
    expect(parsed.budget).toEqual({
      dailyBudgetUsd: 5,
      maxTopics: 3,
      maxEvidencePerTopic: 4,
      maxOutputTokens: 1200,
    });
    expect(parsed.topics[0].topic).toBe("aws.bedrock");
    expect(parsed.topics[0].metrics).toHaveLength(1);
    expect(parsed.topics[0].metrics[0]).toMatchObject({
      topic: "aws.bedrock",
      window: 2,
      score: 12.5,
      volume: 21,
      acceleration: 0.8,
    });
    expect(parsed.topics[0].evidence).toHaveLength(1);
    expect(parsed.topics[0].evidence[0]).toMatchObject({
      eventId: "evt-1",
      source: "hackernews",
      url: "https://example.com/1",
      title: "Bedrock update",
      textExcerpt: "Details",
    });
  });

  it("parses protobuf trend window enum names", () => {
    const payload = {
      request_id: "req-2",
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
      windows: ["TREND_WINDOW_15M", "TREND_WINDOW_60M", "TREND_WINDOW_24H", "2"],
    };

    const parsed = deserializeSummaryRequest(makeBuffer(payload));
    expect(parsed.windows).toEqual([1, 2, 3, 2]);
  });

  it("preserves missing budget fields as undefined", () => {
    const payload = {
      request_id: "req-budget",
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
      budget: {
        daily_budget_usd: 5,
      },
    };

    const parsed = deserializeSummaryRequest(makeBuffer(payload));
    expect(parsed.budget).toEqual({
      dailyBudgetUsd: 5,
      maxTopics: undefined,
      maxEvidencePerTopic: undefined,
      maxOutputTokens: undefined,
    });
  });

  it("supports string summary request types", () => {
    expect(parseSummaryRequestType("daily")).toBe("daily");
    expect(parseSummaryRequestType("SUMMARY_REQUEST_TYPE_DAILY")).toBe("daily");
    expect(parseSummaryRequestType("threshold")).toBe("threshold");
    expect(parseSummaryRequestType("SUMMARY_REQUEST_TYPE_THRESHOLD")).toBe("threshold");
  });

  it("throws for unsupported summary request type", () => {
    expect(() => parseSummaryRequestType(999)).toThrow("Unsupported summary request type enum");
    expect(() => parseSummaryRequestType("unknown")).toThrow(
      "Unsupported summary request type value"
    );
  });

  it("throws for invalid JSON", () => {
    expect(() => deserializeSummaryRequest(Buffer.from("{bad", "utf-8"))).toThrow(
      "Invalid JSON payload"
    );
  });

  it("throws for invalid requested_at", () => {
    const payload = {
      request_id: "req-1",
      requested_at: "not-a-date",
      type: 1,
    };

    expect(() => deserializeSummaryRequest(makeBuffer(payload))).toThrow("Invalid requested_at");
  });

  it("rejects malformed window values", () => {
    const payload = {
      request_id: "req-1",
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
      windows: ["2oops"],
    };

    expect(() => deserializeSummaryRequest(makeBuffer(payload))).toThrow(
      "Unsupported trend window value"
    );
  });

  it("rejects non-positive window enums", () => {
    const payload = {
      request_id: "req-1",
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
      windows: [0],
    };

    expect(() => deserializeSummaryRequest(makeBuffer(payload))).toThrow(
      "Unsupported trend window enum"
    );
  });

  it("rejects unknown positive window enums", () => {
    const payload = {
      request_id: "req-1",
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
      windows: [99],
    };

    expect(() => deserializeSummaryRequest(makeBuffer(payload))).toThrow(
      "Unsupported trend window enum"
    );
  });

  it("rejects missing required fields", () => {
    const payload = {
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
    };

    expect(() => deserializeSummaryRequest(makeBuffer(payload))).toThrow();
  });

  it("rejects unknown evidence source values", () => {
    const payload = {
      request_id: "req-source",
      requested_at: "2026-02-06T10:00:00.000Z",
      type: 1,
      topics: [
        {
          topic: "aws.bedrock",
          evidence: [
            {
              event_id: "evt-1",
              source: "totally-unknown-source",
            },
          ],
        },
      ],
    };

    expect(() => deserializeSummaryRequest(makeBuffer(payload))).toThrow(
      "Unsupported source value"
    );
  });
});
