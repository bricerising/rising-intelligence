import { describe, expect, it } from "vitest";
import { REQUIRED_TOPICS } from "../src/commands/kafka/required-topics.js";
import { resolveEnsureTopicsConfig } from "../src/commands/kafka/ensure-topics.js";

describe("resolveEnsureTopicsConfig", () => {
  it("uses required topics by default", () => {
    const config = resolveEnsureTopicsConfig({}, {});
    expect(config.brokers).toEqual(["localhost:9092"]);
    expect(config.topics).toEqual([...REQUIRED_TOPICS]);
    expect(config.waitTimeoutMs).toBe(15000);
    expect(config.pollIntervalMs).toBe(250);
  });

  it("uses custom topics with explicit partition/replication overrides", () => {
    const config = resolveEnsureTopicsConfig(
      {
        "kafka-brokers": "a:9092,b:9092",
        topics: "summary.requests,summary.results",
        partitions: "3",
        "replication-factor": "1",
        "wait-timeout-ms": "5000",
        "poll-interval-ms": "100",
      },
      {}
    );

    expect(config.brokers).toEqual(["a:9092", "b:9092"]);
    expect(config.topics).toEqual([
      { topic: "summary.requests", numPartitions: 3, replicationFactor: 1 },
      { topic: "summary.results", numPartitions: 3, replicationFactor: 1 },
    ]);
    expect(config.waitTimeoutMs).toBe(5000);
    expect(config.pollIntervalMs).toBe(100);
  });

  it("throws when topics csv resolves to empty", () => {
    expect(() =>
      resolveEnsureTopicsConfig(
        {
          topics: " , , ",
        },
        {}
      )
    ).toThrow(/empty list/i);
  });
});
