import { describe, expect, it } from "vitest";
import {
  buildInternalSuggestedAction,
  buildInternalWhyItMatters,
  detectSignalCategories,
} from "../src/internal-highlight-strategy.js";

describe("internal highlight strategy", () => {
  it("detects multiple signal categories from normalized evidence text", () => {
    const categories = detectSignalCategories(
      "Security patch and outage report include IAM policy updates."
    );

    expect(categories).toEqual(
      new Set(["security", "reliability", "governance"])
    );
  });

  it("builds why-it-matters text in strategy order", () => {
    const whyItMatters = buildInternalWhyItMatters(
      "aws.bedrock",
      new Set(["reliability", "security"])
    );

    expect(whyItMatters).toBe(
      "Security-related changes may require immediate remediation to reduce exposure. Reliability and incident signals can impact SLOs if dependency failure paths are untested."
    );
  });

  it("falls back to generic why-it-matters text when no categories are detected", () => {
    const whyItMatters = buildInternalWhyItMatters(
      "cloud.gcp",
      new Set()
    );

    expect(whyItMatters).toBe(
      "Recent cloud.gcp updates include concrete platform changes that may affect near-term delivery plans."
    );
  });

  it("builds suggested actions in strategy order with a default fallback", () => {
    const actions = buildInternalSuggestedAction(
      new Set(["feature", "governance"])
    );
    const fallbackActions = buildInternalSuggestedAction(new Set());

    expect(actions).toBe(
      "Review IAM/trust policy baselines and update policy-as-code checks. Pilot new capabilities in non-production with clear rollback criteria."
    );
    expect(fallbackActions).toBe(
      "Review cited changes, assign owners, and schedule validation work this week."
    );
  });
});
