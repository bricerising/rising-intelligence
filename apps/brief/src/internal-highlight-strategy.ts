export type SignalCategory =
  | "security"
  | "reliability"
  | "lifecycle"
  | "governance"
  | "cost"
  | "feature";

interface SignalCategoryStrategy {
  readonly category: SignalCategory;
  readonly pattern: RegExp;
  readonly whyItMatters: string;
  readonly suggestedAction: string;
}

const SIGNAL_CATEGORY_STRATEGIES: ReadonlyArray<SignalCategoryStrategy> = [
  {
    category: "security",
    pattern:
      /\b(cve-|vulnerability|security|privilege escalation|exploit|patch|xss|rce|authn|authz|jwt|oidc|iam)\b/i,
    whyItMatters: "Security-related changes may require immediate remediation to reduce exposure.",
    suggestedAction: "Prioritize patch validation and configuration audits for affected services.",
  },
  {
    category: "reliability",
    pattern:
      /\b(outage|incident|degradation|latency|error rates?|unavailable|downtime|fail(?:ed|ure)|partition|control plane)\b/i,
    whyItMatters:
      "Reliability and incident signals can impact SLOs if dependency failure paths are untested.",
    suggestedAction: "Run failover and alert drills for impacted dependency paths.",
  },
  {
    category: "lifecycle",
    pattern:
      /\b(deprecat(?:e|ed|ion)|sunset|end of support|eol|removed support|no longer supported|upgrade required)\b/i,
    whyItMatters:
      "Lifecycle/deprecation updates can break runtimes and automation if upgrades are delayed.",
    suggestedAction: "Inventory impacted runtimes/services and stage upgrades before enforcement dates.",
  },
  {
    category: "governance",
    pattern:
      /\b(policy|organization policy|compliance|governance|trust policy|permission|identity provider)\b/i,
    whyItMatters: "Identity and policy shifts can block deploys unless controls and trust policies are updated.",
    suggestedAction: "Review IAM/trust policy baselines and update policy-as-code checks.",
  },
  {
    category: "cost",
    pattern:
      /\b(pricing|cost|finops|optimi[sz]e|idle|throughput|latency reduction|ttlb|storage tier|ssd)\b/i,
    whyItMatters: "Cost and performance changes can materially alter spend and latency assumptions.",
    suggestedAction: "Benchmark cost/latency impact in non-production before broad rollout.",
  },
  {
    category: "feature",
    pattern:
      /\b(generally available|ga|public preview|preview|launched|now available|release update|added support)\b/i,
    whyItMatters: "New GA/preview capabilities may reduce custom platform work once validated.",
    suggestedAction: "Pilot new capabilities in non-production with clear rollback criteria.",
  },
];

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function collectNarratives(
  categories: Set<SignalCategory>,
  selector: (strategy: SignalCategoryStrategy) => string
): string[] {
  const narratives: string[] = [];
  for (const strategy of SIGNAL_CATEGORY_STRATEGIES) {
    if (!categories.has(strategy.category)) {
      continue;
    }
    narratives.push(selector(strategy));
  }
  return narratives;
}

export function detectSignalCategories(value: string): Set<SignalCategory> {
  const categories = new Set<SignalCategory>();
  const normalized = normalizeWhitespace(value);
  for (const strategy of SIGNAL_CATEGORY_STRATEGIES) {
    if (strategy.pattern.test(normalized)) {
      categories.add(strategy.category);
    }
  }
  return categories;
}

export function buildInternalWhyItMatters(topic: string, categories: Set<SignalCategory>): string {
  const narratives = collectNarratives(categories, (strategy) => strategy.whyItMatters);
  if (narratives.length === 0) {
    return `Recent ${topic} updates include concrete platform changes that may affect near-term delivery plans.`;
  }
  return narratives.slice(0, 2).join(" ");
}

export function buildInternalSuggestedAction(categories: Set<SignalCategory>): string {
  const narratives = collectNarratives(categories, (strategy) => strategy.suggestedAction);
  if (narratives.length === 0) {
    return "Review cited changes, assign owners, and schedule validation work this week.";
  }
  return narratives.slice(0, 2).join(" ");
}
