import {
  loadAllowlist as loadPipelineAllowlist,
  type CompiledAllowlist,
  type CompiledTopic,
} from "@rising-intelligence/pipeline";

export type { CompiledAllowlist, CompiledTopic };

export function loadAllowlist(path: string): CompiledAllowlist {
  const allowlist = loadPipelineAllowlist(path);
  const seenTopicKeys = new Set<string>();
  for (const topic of allowlist.topics) {
    if (seenTopicKeys.has(topic.key)) {
      throw new Error(`Duplicate topic key: ${topic.key}`);
    }
    seenTopicKeys.add(topic.key);
  }
  return allowlist;
}

export function filterTrackedTags(
  tags: string[] | undefined,
  allowlist: CompiledAllowlist
): string[] {
  if (!tags || tags.length === 0) {
    return [];
  }

  const dedupedKeys = [...new Set(tags.map((tag) => tag.trim()).filter((tag) => tag.length > 0))];
  const topicByKey = new Map<string, CompiledTopic>(
    allowlist.topics.map((topic) => [topic.key, topic])
  );
  const matched: CompiledTopic[] = [];

  for (const key of dedupedKeys) {
    if (allowlist.mutedTopics.has(key)) {
      continue;
    }

    const topic = topicByKey.get(key);
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
