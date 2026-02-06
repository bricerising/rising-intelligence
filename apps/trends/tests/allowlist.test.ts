import { mkdtempSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { filterTrackedTags, loadAllowlist } from "../src/allowlist.js";

function writeAllowlist(content: string): string {
  const dir = mkdtempSync(join(tmpdir(), "ri-trends-allowlist-"));
  const path = join(dir, "topics.allowlist.yaml");
  writeFileSync(path, content, "utf-8");
  return path;
}

describe("trends allowlist", () => {
  it("loads and filters tracked tags", () => {
    const path = writeAllowlist(`
defaults:
  max_topics_per_event: 3
  default_priority: 50
topics:
  - key: ai.openai
    display_name: OpenAI
    priority: 80
  - key: aws.bedrock
    display_name: Bedrock
    priority: 90
  - key: ai.general
    display_name: AI General
    priority: 20
suppression:
  muted_topics:
    - ai.general
`);

    const allowlist = loadAllowlist(path);
    const filtered = filterTrackedTags(
      ["ai.openai", "unknown.topic", "aws.bedrock", "ai.general", "aws.bedrock"],
      allowlist
    );

    expect(filtered).toEqual(["aws.bedrock", "ai.openai"]);
  });

  it("enforces max_topics_per_event ordering by priority then key", () => {
    const path = writeAllowlist(`
defaults:
  max_topics_per_event: 2
topics:
  - key: b.topic
    display_name: B Topic
    priority: 10
  - key: a.topic
    display_name: A Topic
    priority: 10
  - key: c.topic
    display_name: C Topic
    priority: 20
`);
    const allowlist = loadAllowlist(path);
    const filtered = filterTrackedTags(["a.topic", "b.topic", "c.topic"], allowlist);

    expect(filtered).toEqual(["c.topic", "a.topic"]);
  });

  it("throws when allowlist has duplicate keys", () => {
    const path = writeAllowlist(`
topics:
  - key: duplicate.topic
    display_name: Topic One
  - key: duplicate.topic
    display_name: Topic Two
`);
    expect(() => loadAllowlist(path)).toThrow("Duplicate topic key");
  });
});
