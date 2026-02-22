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

function isKafkaTopicRelevant(context: MatcherEvaluationContext): boolean {
  const content = `${context.lowerTitle} ${context.lowerText}`.trim();
  if (content.length === 0) {
    return false;
  }

  if (hasWholeWord(content, "redpanda")) {
    return true;
  }
  if (hasWholeWord(content, "apache kafka")) {
    return true;
  }

  const kafkaCount = countWholeWord(content, "kafka");
  if (kafkaCount === 0) {
    return false;
  }

  const hasTechnicalSignals = hasAnyPattern(content, KAFKA_TECHNICAL_PATTERNS);
  if (hasWholeWord(content, "peter kafka") && !hasTechnicalSignals) {
    return false;
  }
  if (hasWholeWord(content, "kafkaesque") && !hasTechnicalSignals) {
    return false;
  }
  if (context.lowerUrl.includes("techmeme.com") && !hasTechnicalSignals) {
    return false;
  }

  if (hasTechnicalSignals) {
    return true;
  }

  return kafkaCount >= 2 && !hasWholeWord(content, "peter kafka");
}

function isOpenTelemetryTopicRelevant(context: MatcherEvaluationContext): boolean {
  const content = `${context.lowerTitle} ${context.lowerText}`.trim();
  if (content.length === 0) {
    return false;
  }
  if (hasWholeWord(content, "opentelemetry")) {
    return true;
  }
  if (!hasWholeWord(content, "otel")) {
    return false;
  }
  return hasAnyPattern(content, OTEL_TECHNICAL_PATTERNS);
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
