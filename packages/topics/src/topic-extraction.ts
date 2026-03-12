import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";
import { z } from "zod";

/**
 * Compiled topic definition with pre-compiled regex patterns.
 */
export interface CompiledTopic {
  key: string;
  displayName: string;
  priority: number;
  matchers: CompiledMatcher[];
}

export type CompiledMatcher =
  | { type: "keyword"; value: string }
  | { type: "regex"; pattern: RegExp };

/**
 * Compiled allowlist ready for fast matching.
 */
export interface CompiledAllowlist {
  topics: CompiledTopic[];
  maxTopicsPerEvent: number;
  defaultPriority: number;
  mutedTopics: Set<string>;
}

/**
 * Raw YAML structure for the allowlist file.
 */
const RawMatcherSchema = z.object({
  type: z.string().min(1),
  value: z.string().optional(),
  pattern: z.string().optional(),
});

const RawTopicSchema = z.object({
  key: z.string().min(1),
  display_name: z.string().min(1),
  priority: z.number().int().optional(),
  aliases: z.array(z.string()).optional(),
  matchers: z.array(RawMatcherSchema).min(1),
});

const RawAllowlistSchema = z.object({
  defaults: z
    .object({
      max_topics_per_event: z.number().int().positive().optional(),
      regex_case_insensitive: z.boolean().optional(),
      default_priority: z.number().int().optional(),
    })
    .optional(),
  topics: z.array(RawTopicSchema).min(1),
  suppression: z
    .object({
      muted_topics: z.array(z.string()).optional(),
    })
    .optional(),
});

type RawAllowlist = z.infer<typeof RawAllowlistSchema>;
type RawMatcher = z.infer<typeof RawMatcherSchema>;
type MatcherType = CompiledMatcher["type"];

interface MatcherCompileContext {
  topicKey: string;
  matcher: RawMatcher;
  caseInsensitive: boolean;
}

type MatcherCompiler = (context: MatcherCompileContext) => CompiledMatcher;

const MATCHER_COMPILERS: Record<MatcherType, MatcherCompiler> = {
  keyword: ({ topicKey, matcher }) => {
    if (!matcher.value) {
      throw new Error(`Topic ${topicKey}: keyword matcher missing 'value'`);
    }

    return {
      type: "keyword",
      value: matcher.value.toLowerCase(),
    };
  },
  regex: ({ topicKey, matcher, caseInsensitive }) => {
    if (!matcher.pattern) {
      throw new Error(`Topic ${topicKey}: regex matcher missing 'pattern'`);
    }

    try {
      const flags = caseInsensitive ? "i" : "";
      return {
        type: "regex",
        pattern: new RegExp(matcher.pattern, flags),
      };
    } catch (error) {
      throw new Error(
        `Topic ${topicKey}: invalid regex pattern '${matcher.pattern}': ${error}`
      );
    }
  },
};

interface MatcherEvaluationContext {
  content: string;
  lowerContent: string;
  lowerTitle: string;
  lowerText: string;
  lowerUrl: string;
}

const MATCHER_EVALUATORS: {
  [K in MatcherType]: (
    matcher: Extract<CompiledMatcher, { type: K }>,
    context: MatcherEvaluationContext
  ) => boolean;
} = {
  keyword: (matcher, context) => context.lowerContent.includes(matcher.value),
  regex: (matcher, context) => matcher.pattern.test(context.content),
};

const KAFKA_TECHNICAL_PATTERNS = [
  /\bapache\s+kafka\b/,
  /\bredpanda\b/,
  /\bkafka\s+(streams?|connect|brokers?|cluster|consumer|producer|topics?|partitions?)\b/,
  /\b(confluent|schema\s+registry|ksql)\b/,
  /\b(stream(?:ing)?|event\s+stream)\b/,
] as const;

const OTEL_TECHNICAL_PATTERNS = [
  /\b(traces?|tracing|spans?|metrics?|telemetry|instrumentation)\b/,
  /\b(collector|exporter|sdk|semconv|otlp)\b/,
  /\b(prometheus|grafana|tempo|jaeger)\b/,
] as const;

type TopicRelevanceDecision = "allow" | "deny" | "next";

interface TopicRelevanceRule<RuleContext> {
  readonly name: string;
  evaluate(context: RuleContext): TopicRelevanceDecision;
}

function escapeRegexLiteral(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function countWholeWord(content: string, term: string): number {
  const pattern = new RegExp(`(^|[^a-z0-9])${escapeRegexLiteral(term)}([^a-z0-9]|$)`, "g");
  const matches = content.match(pattern);
  return matches ? matches.length : 0;
}

function hasWholeWord(content: string, term: string): boolean {
  return countWholeWord(content, term) > 0;
}

function hasAnyPattern(content: string, patterns: readonly RegExp[]): boolean {
  return patterns.some((pattern) => pattern.test(content));
}

function evaluateTopicRelevanceRules<RuleContext>(
  rules: readonly TopicRelevanceRule<RuleContext>[],
  context: RuleContext,
  fallbackDecision: boolean
): boolean {
  for (const rule of rules) {
    const decision = rule.evaluate(context);
    if (decision === "next") {
      continue;
    }
    return decision === "allow";
  }
  return fallbackDecision;
}

interface KafkaTopicRelevanceContext {
  content: string;
  lowerUrl: string;
  kafkaCount: number;
  hasTechnicalSignals: boolean;
  hasRedpanda: boolean;
  hasApacheKafka: boolean;
  hasPeterKafka: boolean;
  hasKafkaesque: boolean;
}

function createKafkaTopicRelevanceContext(
  context: MatcherEvaluationContext
): KafkaTopicRelevanceContext {
  const content = `${context.lowerTitle} ${context.lowerText}`.trim();
  return {
    content,
    lowerUrl: context.lowerUrl,
    kafkaCount: countWholeWord(content, "kafka"),
    hasTechnicalSignals: hasAnyPattern(content, KAFKA_TECHNICAL_PATTERNS),
    hasRedpanda: hasWholeWord(content, "redpanda"),
    hasApacheKafka: hasWholeWord(content, "apache kafka"),
    hasPeterKafka: hasWholeWord(content, "peter kafka"),
    hasKafkaesque: hasWholeWord(content, "kafkaesque"),
  };
}

const KAFKA_TOPIC_RELEVANCE_RULES: readonly TopicRelevanceRule<KafkaTopicRelevanceContext>[] = [
  {
    name: "empty_content",
    evaluate(context): TopicRelevanceDecision {
      return context.content.length === 0 ? "deny" : "next";
    },
  },
  {
    name: "explicit_kafka_terms",
    evaluate(context): TopicRelevanceDecision {
      return context.hasRedpanda || context.hasApacheKafka ? "allow" : "next";
    },
  },
  {
    name: "missing_kafka_term",
    evaluate(context): TopicRelevanceDecision {
      return context.kafkaCount === 0 ? "deny" : "next";
    },
  },
  {
    name: "peter_kafka_without_technical_context",
    evaluate(context): TopicRelevanceDecision {
      if (!context.hasPeterKafka || context.hasTechnicalSignals) {
        return "next";
      }
      return "deny";
    },
  },
  {
    name: "kafkaesque_without_technical_context",
    evaluate(context): TopicRelevanceDecision {
      if (!context.hasKafkaesque || context.hasTechnicalSignals) {
        return "next";
      }
      return "deny";
    },
  },
  {
    name: "techmeme_without_technical_context",
    evaluate(context): TopicRelevanceDecision {
      if (!context.lowerUrl.includes("techmeme.com") || context.hasTechnicalSignals) {
        return "next";
      }
      return "deny";
    },
  },
  {
    name: "technical_signal_match",
    evaluate(context): TopicRelevanceDecision {
      return context.hasTechnicalSignals ? "allow" : "next";
    },
  },
  {
    name: "multiple_kafka_mentions_fallback",
    evaluate(context): TopicRelevanceDecision {
      return context.kafkaCount >= 2 && !context.hasPeterKafka ? "allow" : "deny";
    },
  },
];

function isKafkaTopicRelevant(context: MatcherEvaluationContext): boolean {
  return evaluateTopicRelevanceRules(
    KAFKA_TOPIC_RELEVANCE_RULES,
    createKafkaTopicRelevanceContext(context),
    false
  );
}

interface OpenTelemetryTopicRelevanceContext {
  content: string;
  hasOpenTelemetry: boolean;
  hasOtel: boolean;
  hasTechnicalSignals: boolean;
}

function createOpenTelemetryTopicRelevanceContext(
  context: MatcherEvaluationContext
): OpenTelemetryTopicRelevanceContext {
  const content = `${context.lowerTitle} ${context.lowerText}`.trim();
  return {
    content,
    hasOpenTelemetry: hasWholeWord(content, "opentelemetry"),
    hasOtel: hasWholeWord(content, "otel"),
    hasTechnicalSignals: hasAnyPattern(content, OTEL_TECHNICAL_PATTERNS),
  };
}

const OPEN_TELEMETRY_TOPIC_RELEVANCE_RULES: readonly TopicRelevanceRule<OpenTelemetryTopicRelevanceContext>[] = [
  {
    name: "empty_content",
    evaluate(context): TopicRelevanceDecision {
      return context.content.length === 0 ? "deny" : "next";
    },
  },
  {
    name: "explicit_opentelemetry_term",
    evaluate(context): TopicRelevanceDecision {
      return context.hasOpenTelemetry ? "allow" : "next";
    },
  },
  {
    name: "missing_otel_term",
    evaluate(context): TopicRelevanceDecision {
      return context.hasOtel ? "next" : "deny";
    },
  },
  {
    name: "otel_technical_signal_match",
    evaluate(context): TopicRelevanceDecision {
      return context.hasTechnicalSignals ? "allow" : "deny";
    },
  },
];

function isOpenTelemetryTopicRelevant(context: MatcherEvaluationContext): boolean {
  return evaluateTopicRelevanceRules(
    OPEN_TELEMETRY_TOPIC_RELEVANCE_RULES,
    createOpenTelemetryTopicRelevanceContext(context),
    false
  );
}

type TopicRelevanceStrategy = (context: MatcherEvaluationContext) => boolean;

const ALWAYS_RELEVANT_TOPIC_STRATEGY: TopicRelevanceStrategy = () => true;

const TOPIC_RELEVANCE_STRATEGIES: Readonly<Record<string, TopicRelevanceStrategy>> = {
  "data.kafka": isKafkaTopicRelevant,
  "observability.opentelemetry": isOpenTelemetryTopicRelevant,
};

function resolveTopicRelevanceStrategy(topicKey: string): TopicRelevanceStrategy {
  return TOPIC_RELEVANCE_STRATEGIES[topicKey] ?? ALWAYS_RELEVANT_TOPIC_STRATEGY;
}

function passesTopicRelevanceFilter(topicKey: string, context: MatcherEvaluationContext): boolean {
  return resolveTopicRelevanceStrategy(topicKey)(context);
}

function isMatcherType(value: string): value is MatcherType {
  return value === "keyword" || value === "regex";
}

function parseAllowlist(content: string): RawAllowlist {
  const decoded = parseYaml(content) as unknown;
  const parsed = RawAllowlistSchema.safeParse(decoded);
  if (parsed.success) {
    return parsed.data;
  }

  const issue = parsed.error.issues[0];
  const path = issue.path.length > 0 ? issue.path.join(".") : "root";
  throw new Error(`Invalid allowlist format at '${path}': ${issue.message}`);
}

function compileMatcher(
  topicKey: string,
  matcher: RawMatcher,
  caseInsensitive: boolean
): CompiledMatcher {
  const normalizedType = matcher.type.trim().toLowerCase();
  if (!isMatcherType(normalizedType)) {
    throw new Error(`Topic ${topicKey}: unsupported matcher type '${matcher.type}'`);
  }

  return MATCHER_COMPILERS[normalizedType]({
    topicKey,
    matcher,
    caseInsensitive,
  });
}

function matchesCompiledMatcher(
  matcher: CompiledMatcher,
  context: MatcherEvaluationContext
): boolean {
  if (matcher.type === "keyword") {
    return MATCHER_EVALUATORS.keyword(matcher, context);
  }
  return MATCHER_EVALUATORS.regex(matcher, context);
}

/**
 * Load and compile the topics allowlist from a YAML file.
 * Pre-compiles all regex patterns for performance.
 *
 * @throws Error if the file cannot be read or contains invalid regex
 */
export function loadAllowlist(path: string): CompiledAllowlist {
  const content = readFileSync(path, "utf-8");
  const raw = parseAllowlist(content);

  const defaultPriority = raw.defaults?.default_priority ?? 50;
  const caseInsensitive = raw.defaults?.regex_case_insensitive ?? true;
  const maxTopicsPerEvent = raw.defaults?.max_topics_per_event ?? 5;
  const mutedTopics = new Set<string>(raw.suppression?.muted_topics ?? []);

  const topics: CompiledTopic[] = [];

  for (const topic of raw.topics) {
    const matchers: CompiledMatcher[] = topic.matchers.map((matcher) =>
      compileMatcher(topic.key, matcher, caseInsensitive)
    );

    topics.push({
      key: topic.key,
      displayName: topic.display_name,
      priority: topic.priority ?? defaultPriority,
      matchers,
    });
  }

  return {
    topics,
    maxTopicsPerEvent,
    defaultPriority,
    mutedTopics,
  };
}

/**
 * Match result with priority for sorting.
 */
interface TopicMatch {
  key: string;
  priority: number;
}

/**
 * Extract topics from event content using the compiled allowlist.
 *
 * Algorithm:
 * 1. Find ALL matching topics (no early exit)
 * 2. Sort by priority (descending), then by key (alphabetically) for ties
 * 3. Take top N (where N = maxTopicsPerEvent)
 *
 * This ensures deterministic results: same content + allowlist = same topics.
 */
export function extractTopics(
  event: { title?: string; text: string; url?: string | null },
  allowlist: CompiledAllowlist
): string[] {
  const title = event.title ?? "";
  const text = event.text;
  const url = event.url ?? "";
  const content = `${title} ${text}`;
  const matcherContext: MatcherEvaluationContext = {
    content,
    lowerContent: content.toLowerCase(),
    lowerTitle: title.toLowerCase(),
    lowerText: text.toLowerCase(),
    lowerUrl: url.toLowerCase(),
  };
  const matches: TopicMatch[] = [];

  // Step 1: Find ALL matching topics
  for (const topic of allowlist.topics) {
    // Skip muted topics
    if (allowlist.mutedTopics.has(topic.key)) {
      continue;
    }

    let matched = false;
    // Check each matcher (any match counts)
    for (const matcher of topic.matchers) {
      if (matchesCompiledMatcher(matcher, matcherContext)) {
        matched = true;
        break; // Only add topic once (first matching matcher wins)
      }
    }

    if (!matched) {
      continue;
    }
    if (!passesTopicRelevanceFilter(topic.key, matcherContext)) {
      continue;
    }
    matches.push({ key: topic.key, priority: topic.priority });
  }

  // Step 2: Sort by priority (desc), then key (asc) for determinism
  matches.sort((a, b) => {
    if (b.priority !== a.priority) {
      return b.priority - a.priority;
    }
    return a.key.localeCompare(b.key);
  });

  // Step 3: Take top N
  return matches.slice(0, allowlist.maxTopicsPerEvent).map((m) => m.key);
}

/**
 * Extract URLs from text content.
 */
export function extractUrls(text: string): string[] {
  const urlRegex = /https?:\/\/[^\s<>"{}|\\^`\[\]]+/gi;
  const matches = text.match(urlRegex);
  return matches ? [...new Set(matches)] : [];
}

/**
 * Extract hashtags from text content.
 */
export function extractHashtags(text: string): string[] {
  const hashtagRegex = /#([A-Za-z][A-Za-z0-9_]{1,30})/g;
  const matches: string[] = [];
  let match: RegExpExecArray | null;
  while ((match = hashtagRegex.exec(text)) !== null) {
    matches.push(match[1].toLowerCase());
  }
  return [...new Set(matches)];
}

/**
 * Extracted security and software entities from event text.
 */
export interface ExtractedEntities {
  cves: string[];
  ghsas: string[];
}

const CVE_REGEX = /\bCVE-\d{4}-\d{4,7}\b/g;
const GHSA_REGEX = /GHSA(-[23456789cfghjmpqrvwx]{4}){3}/g;

/**
 * Extract security-relevant entity identifiers (CVE IDs, GHSA IDs) from text.
 * Returns deduplicated, sorted arrays of canonical identifiers.
 */
export function extractEntities(text: string): ExtractedEntities {
  const cveMatches = text.match(CVE_REGEX);
  const cves = cveMatches
    ? [...new Set(cveMatches.map((m) => m.toUpperCase()))].sort()
    : [];

  const ghsaMatches = text.match(GHSA_REGEX);
  const ghsas = ghsaMatches
    ? [...new Set(ghsaMatches.map((m) => m.toUpperCase()))].sort()
    : [];

  return { cves, ghsas };
}
