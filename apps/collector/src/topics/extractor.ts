import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";

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
interface RawAllowlist {
  defaults?: {
    max_topics_per_event?: number;
    regex_case_insensitive?: boolean;
    default_priority?: number;
  };
  topics: Array<{
    key: string;
    display_name: string;
    priority?: number;
    aliases?: string[];
    matchers: Array<{
      type: "keyword" | "regex";
      value?: string;
      pattern?: string;
    }>;
  }>;
  suppression?: {
    muted_topics?: string[];
  };
}

/**
 * Load and compile the topics allowlist from a YAML file.
 * Pre-compiles all regex patterns for performance.
 *
 * @throws Error if the file cannot be read or contains invalid regex
 */
export function loadAllowlist(path: string): CompiledAllowlist {
  const content = readFileSync(path, "utf-8");
  const raw = parseYaml(content) as RawAllowlist;

  const defaultPriority = raw.defaults?.default_priority ?? 50;
  const caseInsensitive = raw.defaults?.regex_case_insensitive ?? true;
  const maxTopicsPerEvent = raw.defaults?.max_topics_per_event ?? 5;
  const mutedTopics = new Set<string>(raw.suppression?.muted_topics ?? []);

  const topics: CompiledTopic[] = [];

  for (const topic of raw.topics) {
    const matchers: CompiledMatcher[] = [];

    for (const matcher of topic.matchers) {
      if (matcher.type === "keyword") {
        if (!matcher.value) {
          throw new Error(
            `Topic ${topic.key}: keyword matcher missing 'value'`
          );
        }
        matchers.push({
          type: "keyword",
          value: matcher.value.toLowerCase(),
        });
      } else if (matcher.type === "regex") {
        if (!matcher.pattern) {
          throw new Error(
            `Topic ${topic.key}: regex matcher missing 'pattern'`
          );
        }
        try {
          const flags = caseInsensitive ? "i" : "";
          matchers.push({
            type: "regex",
            pattern: new RegExp(matcher.pattern, flags),
          });
        } catch (error) {
          throw new Error(
            `Topic ${topic.key}: invalid regex pattern '${matcher.pattern}': ${error}`
          );
        }
      }
    }

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
  event: { title?: string; text: string },
  allowlist: CompiledAllowlist
): string[] {
  const content = `${event.title ?? ""} ${event.text}`;
  const lowerContent = content.toLowerCase();
  const matches: TopicMatch[] = [];

  // Step 1: Find ALL matching topics
  for (const topic of allowlist.topics) {
    // Skip muted topics
    if (allowlist.mutedTopics.has(topic.key)) {
      continue;
    }

    // Check each matcher (any match counts)
    for (const matcher of topic.matchers) {
      let matched = false;

      if (matcher.type === "keyword") {
        matched = lowerContent.includes(matcher.value);
      } else {
        matched = matcher.pattern.test(content);
      }

      if (matched) {
        matches.push({ key: topic.key, priority: topic.priority });
        break; // Only add topic once (first matching matcher wins)
      }
    }
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
  let match;
  while ((match = hashtagRegex.exec(text)) !== null) {
    matches.push(match[1].toLowerCase());
  }
  return [...new Set(matches)];
}
