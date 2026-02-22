import { Source } from "@rising-intelligence/db";
import { describe, expect, it } from "vitest";
import {
  getTopLevelTopicGroup,
  isEventRelevantToTopic,
  rankTopicsFromSnapshots,
  selectEvidence,
  selectTopLevelTopicGroups,
  type QueryModeRawEvent,
} from "../src/query-mode-selection.js";

function makeEvent(overrides: Partial<QueryModeRawEvent> = {}): QueryModeRawEvent {
  return {
    eventId: "evt-default",
    source: Source.news,
    url: "https://example.com/default",
    title: "Default title",
    publishedAt: new Date("2026-02-20T00:00:00.000Z"),
    fetchedAt: new Date("2026-02-20T00:00:00.000Z"),
    text: "default text",
    topics: ["aws.bedrock"],
    engagementScore: 0,
    ...overrides,
  };
}

describe("query-mode selection strategies", () => {
  it("selects recency strategy in source order", () => {
    const events = [
      makeEvent({ eventId: "evt-1" }),
      makeEvent({ eventId: "evt-2" }),
      makeEvent({ eventId: "evt-3" }),
    ];

    const selected = selectEvidence(events, "recency", 2);

    expect(selected.map((event) => event.eventId)).toEqual(["evt-1", "evt-2"]);
  });

  it("sorts engagement strategy by score and then recency", () => {
    const events = [
      makeEvent({
        eventId: "evt-old-high",
        engagementScore: 90,
        publishedAt: new Date("2026-02-18T00:00:00.000Z"),
      }),
      makeEvent({
        eventId: "evt-new-high",
        engagementScore: 90,
        publishedAt: new Date("2026-02-19T00:00:00.000Z"),
      }),
      makeEvent({
        eventId: "evt-low",
        engagementScore: 40,
        publishedAt: new Date("2026-02-21T00:00:00.000Z"),
      }),
    ];

    const selected = selectEvidence(events, "engagement", 2);

    expect(selected.map((event) => event.eventId)).toEqual([
      "evt-new-high",
      "evt-old-high",
    ]);
  });

  it("applies diversity strategy by seeding curated and discussion sources first", () => {
    const events = [
      makeEvent({
        eventId: "evt-curated-first",
        source: Source.news,
        engagementScore: 5,
      }),
      makeEvent({
        eventId: "evt-discussion-first",
        source: Source.reddit,
        engagementScore: 1,
      }),
      makeEvent({
        eventId: "evt-curated-second",
        source: Source.github,
        engagementScore: 60,
      }),
      makeEvent({
        eventId: "evt-discussion-second",
        source: Source.hackernews,
        engagementScore: 50,
      }),
    ];

    const selected = selectEvidence(events, "diversity", 3);

    expect(selected.map((event) => event.eventId)).toEqual([
      "evt-curated-first",
      "evt-discussion-first",
      "evt-curated-second",
    ]);
  });
});

describe("query-mode topic relevance", () => {
  it("requires at least two body matches for topic-only relevance checks", () => {
    const weakBodyEvent = makeEvent({
      eventId: "evt-weak",
      title: "Model updates",
      url: "https://example.com/model-updates",
      text: "this text only mentions kafka once",
    });
    const strongBodyEvent = makeEvent({
      eventId: "evt-strong",
      title: "Model updates",
      url: "https://example.com/model-updates",
      text: "kafka pipelines and kafka consumers in production",
    });

    expect(isEventRelevantToTopic(weakBodyEvent, "data.kafka")).toBe(false);
    expect(isEventRelevantToTopic(strongBodyEvent, "data.kafka")).toBe(true);
  });

  it("accepts topic alias hits from title or url immediately", () => {
    const aliasEvent = makeEvent({
      eventId: "evt-alias",
      title: "Redpanda throughput tuning guide",
      text: "single mention only",
    });

    expect(isEventRelevantToTopic(aliasEvent, "data.kafka")).toBe(true);
  });
});

describe("query-mode ranking", () => {
  it("selects top-level groups by aggregate score and recency tie-breaker", () => {
    const selected = selectTopLevelTopicGroups(
      [
        {
          topic: "aws.bedrock",
          score: 5,
          volume: 10,
          acceleration: 1,
          latestGeneratedAtMs: 100,
        },
        {
          topic: "aws.s3",
          score: 1,
          volume: 4,
          acceleration: 1,
          latestGeneratedAtMs: 80,
        },
        {
          topic: "data.kafka",
          score: 4,
          volume: 7,
          acceleration: 1,
          latestGeneratedAtMs: 90,
        },
        {
          topic: "observability.opentelemetry",
          score: 4,
          volume: 6,
          acceleration: 1,
          latestGeneratedAtMs: 110,
        },
      ],
      2
    );

    expect([...selected]).toEqual(["aws", "observability"]);
  });

  it("ranks topics from snapshots and applies topic glob matchers", () => {
    const requestedAt = new Date("2026-02-21T12:00:00.000Z");
    const ranked = rankTopicsFromSnapshots(
      [
        {
          generatedAt: new Date("2026-02-21T11:00:00.000Z"),
          snapshot: {
            topics: [
              { topic: "aws.bedrock", score: 9, volume: 8, acceleration: 1 },
              { topic: "data.kafka", score: 2, volume: 5, acceleration: 0.5 },
            ],
          },
        },
        {
          generatedAt: new Date("2026-02-21T06:00:00.000Z"),
          snapshot: {
            topics: [
              { topic: "aws.bedrock", score: 18, volume: 16, acceleration: 1.5 },
              { topic: "data.kafka", score: 8, volume: 7, acceleration: 1.2 },
            ],
          },
        },
      ],
      requestedAt,
      [/^aws\./i]
    );

    expect(ranked).toHaveLength(1);
    expect(ranked[0]?.topic).toBe("aws.bedrock");
    expect(ranked[0]?.score).toBeGreaterThan(9);
    expect(ranked[0]?.score).toBeLessThan(18);
  });
});

describe("topic helpers", () => {
  it("normalizes top-level topic groups", () => {
    expect(getTopLevelTopicGroup("  OBSERVABILITY.OpenTelemetry ")).toBe("observability");
    expect(getTopLevelTopicGroup("aws")).toBe("aws");
    expect(getTopLevelTopicGroup(" ")).toBe("");
  });
});
