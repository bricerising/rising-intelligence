import { describe, expect, it } from "vitest";
import { resolveTestBootstrapCheckConfig } from "../src/commands/db/test-bootstrap-check.js";

describe("resolveTestBootstrapCheckConfig", () => {
  it("builds defaults when flags are empty", () => {
    const config = resolveTestBootstrapCheckConfig({});
    expect(config.databaseUrl).toContain("postgresql://");
    expect(config.requiredTables).toEqual([
      "raw_events",
      "trend_snapshots",
      "consumer_lag",
      "brief_results",
      "brief_budget_tracking",
    ]);
    expect(config.requiredMigrations).toBeNull();
    expect(config.skipMigrationsCheck).toBe(false);
    expect(config.migrationsDir).toContain("packages/db/prisma/migrations");
  });

  it("honors explicit table and migration overrides", () => {
    const config = resolveTestBootstrapCheckConfig({
      "database-url": "postgresql://test:test@localhost:5433/rising_intelligence_test",
      "required-tables": "brief_results,brief_budget_tracking",
      "required-migrations": "20260210000003_add_brief_tables",
      "skip-migrations-check": true,
      "migrations-dir": "/tmp/migrations",
    });

    expect(config.databaseUrl).toBe(
      "postgresql://test:test@localhost:5433/rising_intelligence_test"
    );
    expect(config.requiredTables).toEqual(["brief_results", "brief_budget_tracking"]);
    expect(config.requiredMigrations).toEqual(["20260210000003_add_brief_tables"]);
    expect(config.skipMigrationsCheck).toBe(true);
    expect(config.migrationsDir).toBe("/tmp/migrations");
  });

  it("throws when required tables resolves to empty", () => {
    expect(() =>
      resolveTestBootstrapCheckConfig({
        "required-tables": " , ",
      })
    ).toThrow(/required-tables resolved to an empty list/i);
  });
});
