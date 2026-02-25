import { describe, expect, it } from "vitest";
import { resolveE2eBriefRunConfig } from "../src/commands/e2e/brief-run.js";

describe("resolveE2eBriefRunConfig", () => {
  it("uses sensible defaults", () => {
    const config = resolveE2eBriefRunConfig({});

    expect(config.composeProject).toBe("ri-brief-e2e");
    expect(config.keepUp).toBe(false);
    expect(config.dryRun).toBe(false);
    expect(config.scriptPath).toContain("scripts/test-e2e-brief.sh");
  });

  it("maps explicit overrides", () => {
    const config = resolveE2eBriefRunConfig({
      "script-path": "/tmp/test-e2e-brief.sh",
      "compose-project": "ri-brief-e2e-alt",
      "kafka-host-port": "19093",
      "schema-registry-host-port": "28082",
      "brief-host-port": "13006",
      "redis-host-port": "16380",
      "postgres-host-port": "15433",
      "mock-llm-host-port": "28080",
      "wait-timeout-ms": "90000",
      "keep-up": true,
      "dry-run": true,
    });

    expect(config.scriptPath).toBe("/tmp/test-e2e-brief.sh");
    expect(config.composeProject).toBe("ri-brief-e2e-alt");
    expect(config.kafkaHostPort).toBe(19093);
    expect(config.schemaRegistryHostPort).toBe(28082);
    expect(config.briefHostPort).toBe(13006);
    expect(config.redisHostPort).toBe(16380);
    expect(config.postgresHostPort).toBe(15433);
    expect(config.mockLlmHostPort).toBe(28080);
    expect(config.waitTimeoutMs).toBe(90000);
    expect(config.keepUp).toBe(true);
    expect(config.dryRun).toBe(true);
  });
});
