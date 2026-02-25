import { describe, expect, it } from "vitest";
import {
  buildFailureBriefResultPayload,
  parseBriefResultPayload,
} from "../src/result-payload-adapter.js";

describe("result payload adapter", () => {
  it("parses persisted success payloads with passthrough fields", () => {
    const parsed = parseBriefResultPayload({
      request_id: "req-1",
      produced_at: "2026-02-11T00:00:00.000Z",
      brief: {
        brief_id: "brief:req-1",
      },
      custom_meta: { model: "test" },
    });

    expect(parsed.request_id).toBe("req-1");
    expect(parsed.produced_at).toBe("2026-02-11T00:00:00.000Z");
    expect(parsed.custom_meta).toEqual({ model: "test" });
  });

  it("parses persisted failure payloads", () => {
    const parsed = parseBriefResultPayload({
      request_id: "req-2",
      produced_at: "2026-02-11T00:00:00.000Z",
      failure: {
        error_code: "budget_exceeded",
        error_message: "Daily budget reached",
        retryable: false,
      },
    });

    expect(parsed.failure).toEqual({
      error_code: "budget_exceeded",
      error_message: "Daily budget reached",
      retryable: false,
    });
  });

  it("throws when persisted payload is missing required identity fields", () => {
    expect(() =>
      parseBriefResultPayload({
        produced_at: "2026-02-11T00:00:00.000Z",
      })
    ).toThrow(/request_id/);
  });

  it("throws when persisted payload has an invalid produced_at timestamp", () => {
    expect(() =>
      parseBriefResultPayload({
        request_id: "req-4",
        produced_at: "not-a-timestamp",
      })
    ).toThrow(/produced_at/);
  });

  it("builds canonical failure payloads", () => {
    const payload = buildFailureBriefResultPayload(
      "req-3",
      new Date("2026-02-11T12:00:00.000Z"),
      "grounding_error",
      "Missing evidence citations",
      false
    );

    expect(payload).toEqual({
      request_id: "req-3",
      produced_at: "2026-02-11T12:00:00.000Z",
      failure: {
        error_code: "grounding_error",
        error_message: "Missing evidence citations",
        retryable: false,
      },
    });
  });
});
