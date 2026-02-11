import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";

type Flags = Record<string, string | boolean | string[]>;

const TOPIC_GLOB_PATTERN = /^[A-Za-z0-9.*?_-]+$/;

interface TopicCollection {
  topics: string[];
  warnings: string[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function getRepeatedStringFlag(flags: Flags, name: string): string[] {
  const value = flags[name];
  if (typeof value === "string") {
    return [value];
  }
  if (Array.isArray(value)) {
    return value.filter((entry): entry is string => typeof entry === "string");
  }
  return [];
}

export function normalizeTopicGlobs(topicGlobs: readonly string[]): string[] {
  const deduped: string[] = [];
  const seen = new Set<string>();

  for (const globRaw of topicGlobs) {
    const glob = globRaw.trim();
    if (!glob) {
      continue;
    }
    if (!TOPIC_GLOB_PATTERN.test(glob)) {
      throw new Error(`Invalid topic glob pattern: ${glob}`);
    }
    if (seen.has(glob)) {
      continue;
    }
    seen.add(glob);
    deduped.push(glob);
  }

  return deduped;
}

function collectTopicsFromNode(
  node: unknown,
  location: string,
  collection: TopicCollection
): void {
  if (Array.isArray(node)) {
    node.forEach((entry, index) => {
      collectTopicsFromNode(entry, `${location}[${index}]`, collection);
    });
    return;
  }

  if (!isRecord(node)) {
    return;
  }

  if (Object.hasOwn(node, "topics")) {
    const topicsValue = node.topics;
    if (Array.isArray(topicsValue)) {
      const normalized = topicsValue
        .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
        .filter((entry) => entry.length > 0);
      if (normalized.length === 0) {
        collection.warnings.push(`Ignoring empty topics array at ${location}.topics`);
      } else {
        collection.topics.push(...normalized);
      }
    }
  }

  for (const [key, value] of Object.entries(node)) {
    if (key === "topics") {
      continue;
    }
    collectTopicsFromNode(value, `${location}.${key}`, collection);
  }
}

export function deriveTopicGlobsFromFeedConfigs(
  feedConfigPaths: readonly string[]
): { topicGlobs: string[]; warnings: string[] } {
  const collection: TopicCollection = {
    topics: [],
    warnings: [],
  };

  for (const feedConfigPath of feedConfigPaths) {
    const trimmedPath = feedConfigPath.trim();
    if (!trimmedPath) {
      continue;
    }

    let content: string;
    try {
      content = readFileSync(trimmedPath, "utf-8");
    } catch (error) {
      throw new Error(`--feed-config path is missing or unreadable: ${trimmedPath}`);
    }

    let parsed: unknown;
    try {
      parsed = parseYaml(content);
    } catch (error) {
      throw new Error(
        `Failed to parse --feed-config YAML (${trimmedPath}): ${(error as Error).message}`
      );
    }

    collectTopicsFromNode(parsed, trimmedPath, collection);
  }

  return {
    topicGlobs: normalizeTopicGlobs(collection.topics),
    warnings: collection.warnings,
  };
}
