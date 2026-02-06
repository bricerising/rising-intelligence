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
      topics: [
        {
          topic: "aws.bedrock",
          evidence: [{ id: "one" }, { id: "two" }],
        },
      ],
    };

    const parsed = deserializeSummaryRequest(makeBuffer(payload));

    expect(parsed.requestId).toBe("req-1");
    expect(parsed.type).toBe("daily");
    expect(parsed.windows).toEqual([1, 2]);
    expect(parsed.topics).toEqual([{ topic: "aws.bedrock", evidenceCount: 2 }]);
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
});
