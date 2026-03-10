import { describe, expect, it } from "vitest";
import {
  BRIEF_KAFKA_TOPICS,
  BRIEF_OPERATION_DEFAULTS,
  BRIEF_QUERY_MODE_WINDOWS,
  BRIEF_SUPPORTED_WINDOWS,
  BRIEF_TRIGGER_DEFAULTS,
  createBriefSummaryRequest,
} from "../src/operations.js";

describe("brief operations contract", () => {
  it("publishes the trigger and kafka defaults needed by operational tooling", () => {
    expect(BRIEF_KAFKA_TOPICS).toEqual({
      summaryRequests: "summary.requests",
      summaryResults: "summary.results",
      trendSnapshots: "trends.snapshots",
    });
    expect(BRIEF_OPERATION_DEFAULTS).toMatchObject({
      kafkaBrokers: "localhost:9092",
      llmProvider: "codex-cli",
      defaultLookbackDays: 7,
      maxLookbackDays: 30,
      maxQueryEventsPerTopic: 25,
    });
    expect(BRIEF_SUPPORTED_WINDOWS).toEqual([1, 2, 3]);
    expect(BRIEF_QUERY_MODE_WINDOWS).toEqual([2]);
    expect(BRIEF_TRIGGER_DEFAULTS.windows).toEqual([2]);
  });

  it("creates a stable query-mode summary request payload", () => {
    const payload = createBriefSummaryRequest({
      requestId: "req-ops-1",
      requestedAt: "2026-03-09T12:00:00.000Z",
      type: "daily",
      windows: [...BRIEF_QUERY_MODE_WINDOWS],
      budget: {
        dailyBudgetUsd: 5,
        maxTopics: 5,
        maxEvidencePerTopic: 3,
        maxOutputTokens: 1200,
      },
      query: {
        lookbackDays: 7,
        topicGlobs: ["aws.*"],
        maxEventsPerTopic: 3,
        evidenceStrategy: "diversity",
      },
      llmProvider: "codex-cli",
      topics: [],
    });

    expect(payload).toMatchObject({
      request_id: "req-ops-1",
      requested_at: "2026-03-09T12:00:00.000Z",
      type: "daily",
      windows: [2],
      llm_provider: "codex-cli",
      query: {
        lookback_days: 7,
        topic_globs: ["aws.*"],
        max_events_per_topic: 3,
        evidence_strategy: "diversity",
      },
      topics: [],
    });
  });
});
