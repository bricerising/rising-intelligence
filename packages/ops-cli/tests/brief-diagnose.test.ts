import { describe, expect, it } from "vitest";
import { resolveDiagnoseConfig } from "../src/commands/brief/diagnose.js";

describe("resolveDiagnoseConfig", () => {
  it("applies defaults", () => {
    const config = resolveDiagnoseConfig({});

    expect(config.kafkaBrokers).toEqual(["localhost:9092"]);
    expect(config.summaryRequestsTopic).toBe("summary.requests");
    expect(config.summaryResultsTopic).toBe("summary.results");
    expect(config.timeoutSeconds).toBe(120);
    expect(config.briefHealthUrl).toBe("http://localhost:3005/health");
    expect(config.lookbackDays).toBe(2);
    expect(config.topicGlobs).toEqual(["*"]);
    expect(config.llmProvider).toBe("codex-cli");
    expect(config.dockerComposeProject).toBeUndefined();
  });

  it("parses explicit flags", () => {
    const config = resolveDiagnoseConfig({
      "kafka-brokers": "localhost:9093",
      "summary-requests-topic": "custom.requests",
      "summary-results-topic": "custom.results",
      "request-id": "diag-123",
      timeout: "180",
      "brief-health-url": "http://localhost:4305/health",
      "lookback-days": "7",
      "topic-globs": "aws.*,ai.*",
      "llm-provider": "codex-cli",
      "skip-trigger": true,
      "skip-logs": true,
      "docker-compose-file": "docker-compose.test.yml",
      "docker-compose-project": "ri-brief-e2e-a",
      "docker-service": "brief-test",
      "docker-logs-tail": "500",
      "dry-run": true,
    });

    expect(config.kafkaBrokers).toEqual(["localhost:9093"]);
    expect(config.summaryRequestsTopic).toBe("custom.requests");
    expect(config.summaryResultsTopic).toBe("custom.results");
    expect(config.requestId).toBe("diag-123");
    expect(config.timeoutSeconds).toBe(180);
    expect(config.briefHealthUrl).toBe("http://localhost:4305/health");
    expect(config.lookbackDays).toBe(7);
    expect(config.topicGlobs).toEqual(["aws.*", "ai.*"]);
    expect(config.llmProvider).toBe("codex-cli");
    expect(config.skipTrigger).toBe(true);
    expect(config.skipLogs).toBe(true);
    expect(config.dockerComposeFile).toBe("docker-compose.test.yml");
    expect(config.dockerComposeProject).toBe("ri-brief-e2e-a");
    expect(config.dockerService).toBe("brief-test");
    expect(config.dockerLogsTail).toBe(500);
    expect(config.dryRun).toBe(true);
  });
});
