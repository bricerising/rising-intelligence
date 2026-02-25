import { describe, expect, it } from "vitest";
import { resolveKafkaBrokers } from "../src/commands/kafka/create-topics.js";

describe("resolveKafkaBrokers", () => {
  it("prefers the --kafka-brokers flag over env", () => {
    const brokers = resolveKafkaBrokers(
      { "kafka-brokers": "flag-a:9092, flag-b:9092" },
      { KAFKA_BROKERS: "env-a:9092" }
    );

    expect(brokers).toEqual(["flag-a:9092", "flag-b:9092"]);
  });

  it("uses KAFKA_BROKERS from env when no flag is provided", () => {
    const brokers = resolveKafkaBrokers({}, { KAFKA_BROKERS: "env-a:9092, env-b:9092" });
    expect(brokers).toEqual(["env-a:9092", "env-b:9092"]);
  });

  it("falls back to localhost when flag and env are unset", () => {
    expect(resolveKafkaBrokers({}, {})).toEqual(["localhost:9092"]);
  });

  it("throws when --kafka-brokers resolves to an empty broker list", () => {
    expect(() =>
      resolveKafkaBrokers({ "kafka-brokers": " ,  , " }, {})
    ).toThrow(/empty value/i);
  });

  it("throws when KAFKA_BROKERS env resolves to an empty broker list", () => {
    expect(() => resolveKafkaBrokers({}, { KAFKA_BROKERS: "   " })).toThrow(/empty value/i);
  });
});
