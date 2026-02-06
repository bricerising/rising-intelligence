import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";

export interface CompiledTopic {
  key: string;
  displayName: string;
  priority: number;
}

export interface CompiledAllowlist {
  topics: CompiledTopic[];
  topicMap: Map<string, CompiledTopic>;
  mutedTopics: Set<string>;
  maxTopicsPerEvent: number;
}

interface RawAllowlist {
  defaults?: {
    default_priority?: number;
    max_topics_per_event?: number;
  };
  topics?: Array<{
    key?: string;
    display_name?: string;
    priority?: number;
  }>;
  suppression?: {
    muted_topics?: string[];
  };
}

export function loadAllowlist(path: string): CompiledAllowlist {
  const content = readFileSync(path, "utf-8");
  const raw = parseYaml(content) as RawAllowlist;

  if (!raw.topics || raw.topics.length === 0) {
    throw new Error("Topics allowlist is empty");
  }

  const defaultPriority = raw.defaults?.default_priority ?? 50;
  const maxTopicsPerEvent = raw.defaults?.max_topics_per_event ?? 5;
  const mutedTopics = new Set<string>(raw.suppression?.muted_topics ?? []);

  const topics: CompiledTopic[] = [];
  const topicMap = new Map<string, CompiledTopic>();

  for (const topic of raw.topics) {
    const key = topic.key?.trim();
    if (!key) {
      throw new Error("Topic key is required");
    }

    if (topicMap.has(key)) {
      throw new Error(`Duplicate topic key in allowlist: ${key}`);
    }

    const compiled: CompiledTopic = {
      key,
      displayName: topic.display_name?.trim() || key,
      priority: topic.priority ?? defaultPriority,
    };

    topics.push(compiled);
    topicMap.set(compiled.key, compiled);
  }

  topics.sort((a, b) => {
    if (b.priority !== a.priority) {
      return b.priority - a.priority;
    }
    return a.key.localeCompare(b.key);
  });

  return {
    topics,
    topicMap,
    mutedTopics,
    maxTopicsPerEvent,
  };
}

export function filterTrackedTags(
  tags: string[] | undefined,
  allowlist: CompiledAllowlist
): string[] {
  if (!tags || tags.length === 0) {
    return [];
  }

  const dedupedKeys = [...new Set(tags.map((tag) => tag.trim()).filter((tag) => tag.length > 0))];
  const matched: CompiledTopic[] = [];

  for (const key of dedupedKeys) {
    if (allowlist.mutedTopics.has(key)) {
      continue;
    }

    const topic = allowlist.topicMap.get(key);
    if (!topic) {
      continue;
    }

    matched.push(topic);
  }

  matched.sort((a, b) => {
    if (b.priority !== a.priority) {
      return b.priority - a.priority;
    }
    return a.key.localeCompare(b.key);
  });

  return matched.slice(0, allowlist.maxTopicsPerEvent).map((topic) => topic.key);
}
