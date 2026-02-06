import { describe, it, expect, beforeAll } from "vitest";
import { loadAllowlist, extractTopics, extractUrls, extractHashtags } from "../src/topics/extractor.js";
import { mkdtempSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

describe("Topic Extraction", () => {
  let allowlist: ReturnType<typeof loadAllowlist>;

  beforeAll(() => {
    const configPath = join(__dirname, "../../../infra/config/topics.allowlist.yaml");
    allowlist = loadAllowlist(configPath);
  });

  it("should load the allowlist successfully", () => {
    expect(allowlist.topics.length).toBeGreaterThan(0);
    expect(allowlist.maxTopicsPerEvent).toBeGreaterThan(0);
  });

  it("should extract AWS Bedrock topic", () => {
    const topics = extractTopics(
      { title: "AWS Bedrock gets new features", text: "Amazon announces updates to Bedrock." },
      allowlist
    );
    expect(topics).toContain("aws.bedrock");
  });

  it("should extract multiple topics with priority ordering", () => {
    const topics = extractTopics(
      {
        title: "OpenAI launches GPT-5",
        text: "OpenAI announces GPT-5 with improved AI capabilities. Works great with Kubernetes.",
      },
      allowlist
    );
    expect(topics.length).toBeLessThanOrEqual(allowlist.maxTopicsPerEvent);
    expect(topics).toContain("ai.openai");
  });

  it("should respect maxTopicsPerEvent limit", () => {
    const topics = extractTopics(
      {
        title: "AWS Lambda with Kubernetes on Azure using GPT-4",
        text: "Using OpenAI, Anthropic Claude, Bedrock, SageMaker, Terraform, Docker, Redis, and Postgres.",
      },
      allowlist
    );
    expect(topics.length).toBeLessThanOrEqual(allowlist.maxTopicsPerEvent);
  });

  it("should return empty array for unmatched content", () => {
    const topics = extractTopics(
      { title: "Random unrelated content", text: "Nothing tech related here." },
      allowlist
    );
    expect(topics.length).toBe(0);
  });
});

describe("URL Extraction", () => {
  it("should extract URLs from text", () => {
    const urls = extractUrls("Check out https://example.com and http://test.org/path");
    expect(urls).toContain("https://example.com");
    expect(urls).toContain("http://test.org/path");
  });

  it("should deduplicate URLs", () => {
    const urls = extractUrls("https://example.com and https://example.com again");
    expect(urls.length).toBe(1);
  });
});

describe("Hashtag Extraction", () => {
  it("should extract hashtags", () => {
    const hashtags = extractHashtags("Check out #AWS and #MachineLearning");
    expect(hashtags).toContain("aws");
    expect(hashtags).toContain("machinelearning");
  });

  it("should deduplicate hashtags", () => {
    const hashtags = extractHashtags("#AWS and #aws and #AWS");
    expect(hashtags.length).toBe(1);
  });
});

describe("Allowlist loading and validation", () => {
  it("throws on invalid regex patterns", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-allowlist-"));
    const path = join(dir, "allowlist.yaml");
    writeFileSync(
      path,
      `
defaults:
  regex_case_insensitive: true
topics:
  - key: test.bad_regex
    display_name: Bad Regex
    matchers:
      - type: regex
        pattern: "["
`,
      "utf-8"
    );

    expect(() => loadAllowlist(path)).toThrow(/invalid regex/i);
  });

  it("throws on keyword matcher missing value", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-allowlist-"));
    const path = join(dir, "allowlist.yaml");
    writeFileSync(
      path,
      `
topics:
  - key: test.missing_keyword_value
    display_name: Missing Keyword Value
    matchers:
      - type: keyword
`,
      "utf-8"
    );

    expect(() => loadAllowlist(path)).toThrow(/missing 'value'/i);
  });

  it("respects suppression.muted_topics", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-allowlist-"));
    const path = join(dir, "allowlist.yaml");
    writeFileSync(
      path,
      `
defaults:
  max_topics_per_event: 5
topics:
  - key: test.muted
    display_name: Muted
    priority: 100
    matchers:
      - type: keyword
        value: "Muted"
  - key: test.unmuted
    display_name: Unmuted
    priority: 50
    matchers:
      - type: keyword
        value: "Muted"
suppression:
  muted_topics: ["test.muted"]
`,
      "utf-8"
    );

    const allowlist = loadAllowlist(path);
    const topics = extractTopics({ title: "Muted", text: "" }, allowlist);
    expect(topics).toEqual(["test.unmuted"]);
  });

  it("sorts by priority then key for determinism", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-allowlist-"));
    const path = join(dir, "allowlist.yaml");
    writeFileSync(
      path,
      `
defaults:
  max_topics_per_event: 5
topics:
  - key: test.b
    display_name: B
    priority: 10
    matchers:
      - type: keyword
        value: "x"
  - key: test.a
    display_name: A
    priority: 10
    matchers:
      - type: keyword
        value: "x"
`,
      "utf-8"
    );

    const allowlist = loadAllowlist(path);
    const topics = extractTopics({ title: "x", text: "" }, allowlist);
    expect(topics).toEqual(["test.a", "test.b"]);
  });

  it("supports case-sensitive regex when regex_case_insensitive is false", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-allowlist-"));
    const path = join(dir, "allowlist.yaml");
    writeFileSync(
      path,
      `
defaults:
  regex_case_insensitive: false
topics:
  - key: test.aws_upper
    display_name: AWS (Uppercase)
    matchers:
      - type: regex
        pattern: "\\\\bAWS\\\\b"
`,
      "utf-8"
    );

    const allowlist = loadAllowlist(path);

    expect(extractTopics({ title: "AWS", text: "" }, allowlist)).toContain("test.aws_upper");
    expect(extractTopics({ title: "aws", text: "" }, allowlist)).not.toContain("test.aws_upper");
  });
});
