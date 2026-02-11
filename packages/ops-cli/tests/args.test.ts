import { describe, expect, it } from "vitest";
import { parseArgs } from "../src/lib/args.js";

describe("parseArgs", () => {
  it("returns help when no args are provided", () => {
    expect(parseArgs([])).toEqual({ kind: "help" });
  });

  it("parses command and mixed flag formats", () => {
    expect(
      parseArgs([
        "kafka",
        "topics",
        "--kafka-brokers",
        "localhost:9092",
        "--dry-run",
        "--mode=fast",
      ])
    ).toEqual({
      kind: "command",
      command: ["kafka", "topics"],
      flags: {
        "kafka-brokers": "localhost:9092",
        "dry-run": true,
        mode: "fast",
      },
    });
  });
});

