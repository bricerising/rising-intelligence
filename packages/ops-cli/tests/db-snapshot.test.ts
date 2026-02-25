import { describe, expect, it } from "vitest";
import {
  buildSnapshotFileName,
  formatUtcTimestamp,
  sanitizeSnapshotLabel,
  splitDatabaseUrlCredentials,
} from "../src/commands/db/snapshot.js";

describe("db snapshot helpers", () => {
  it("formats UTC timestamps for filenames", () => {
    const value = new Date("2026-02-15T08:09:10.000Z");
    expect(formatUtcTimestamp(value)).toBe("20260215T080910Z");
  });

  it("sanitizes labels into safe filename segments", () => {
    expect(sanitizeSnapshotLabel("Manual Run / Friday")).toBe("manual-run-friday");
  });

  it("strips password from postgres URL before passing to pg_dump", () => {
    const parsed = splitDatabaseUrlCredentials(
      "postgresql://ri_user:s3cr%40t@db:5432/rising_intelligence?sslmode=disable"
    );

    expect(parsed.password).toBe("s3cr@t");
    expect(parsed.databaseName).toBe("rising_intelligence");
    expect(parsed.connectionUrl).toBe(
      "postgresql://ri_user@db:5432/rising_intelligence?sslmode=disable"
    );
  });

  it("builds deterministic snapshot names with optional label suffix", () => {
    const value = new Date("2026-02-15T08:09:10.000Z");
    const filename = buildSnapshotFileName(
      "postgresql://rising:secret@localhost:5432/rising_intelligence",
      value,
      "Manual Backfill"
    );

    expect(filename).toBe("postgres-rising_intelligence-20260215T080910Z-manual-backfill.dump");
  });
});
