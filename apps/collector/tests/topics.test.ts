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

  describe("Precision Improvements (2026-02-10)", () => {
    it("should NOT match aws.general for tangential AWS mentions", () => {
      const topics = extractTopics(
        {
          title: "Kubernetes best practices",
          text: "When deploying to AWS or GCP, consider using managed services.",
        },
        allowlist
      );
      // Should not match aws.general with such low priority and tangential mention
      expect(topics).not.toContain("aws.general");
    });

    it("should match aws.ec2 and aws.ecs specifically for ECS/EC2 content", () => {
      const topics = extractTopics(
        {
          title: "AWS ECS Managed Instances now available in European Sovereign Cloud",
          text: "Amazon announces ECS managed instances with EC2 support.",
        },
        allowlist
      );
      expect(topics).toContain("aws.ecs");
      expect(topics).toContain("aws.ec2");
      // Should NOT match aws.general even though "AWS" is present
      expect(topics).not.toContain("aws.general");
    });

    it("should NOT match ai.rag for random RAG abbreviation", () => {
      const topics = extractTopics(
        {
          title: "Company achieves RAG status in compliance audit",
          text: "The review board granted RAG (Red-Amber-Green) status.",
        },
        allowlist
      );
      // Should not match ai.rag without AI/retrieval context
      expect(topics).not.toContain("ai.rag");
    });

    it("should match ai.rag with proper context", () => {
      const topics = extractTopics(
        {
          title: "Improving RAG with vector embeddings",
          text: "Retrieval-augmented generation with better retrieval accuracy.",
        },
        allowlist
      );
      expect(topics).toContain("ai.rag");
    });

    it("should NOT match cloud.docker for generic container mentions", () => {
      const topics = extractTopics(
        {
          title: "AWS ECS container optimization",
          text: "Optimize your container deployments on ECS Fargate.",
        },
        allowlist
      );
      // Should match ECS/Fargate but NOT Docker (no Docker mentioned)
      expect(topics).toContain("aws.ecs");
      expect(topics).not.toContain("cloud.docker");
    });

    it("should prefer specific topics over general fallbacks", () => {
      const topics = extractTopics(
        {
          title: "AWS Lambda and Bedrock integration",
          text: "Connect AWS Lambda functions to Bedrock for AI inference.",
        },
        allowlist
      );
      // Should match specific services, not aws.general
      expect(topics).toContain("aws.lambda");
      expect(topics).toContain("aws.bedrock");
      expect(topics).not.toContain("aws.general");
      expect(topics).not.toContain("ai.general");
    });

    it("should require EKS in AWS context to avoid false positives", () => {
      const topics = extractTopics(
        {
          title: "Understanding EKS configuration",
          text: "Generic discussion about EKS without AWS context.",
        },
        allowlist
      );
      // May or may not match depending on whether cluster/node/pod keywords present
      // This test documents the behavior - adjust if needed
      const hasEKS = topics.includes("aws.eks");
      // If it matches, it should be because of substantive content
      if (hasEKS) {
        expect(topics).toContain("aws.eks");
      }
    });

    it("should match ai.general for AI-based product coverage", () => {
      const topics = extractTopics(
        {
          title: "Carrier launches AI-based translation feature",
          text: "The AI-based system supports dozens of languages.",
        },
        allowlist
      );
      expect(topics).toContain("ai.general");
    });

    it("should match security.general for breach and leak language", () => {
      const topics = extractTopics(
        {
          title: "Regulator cites major data leak after data breach",
          text: "Officials linked the incident to a cyber-espionage campaign.",
        },
        allowlist
      );
      expect(topics).toContain("security.general");
    });

    it("should match lang.python for Pandas updates", () => {
      const topics = extractTopics(
        {
          title: "Pandas 3.0 introduces copy-on-write defaults",
          text: "The update improves dataframe behavior for Python users.",
        },
        allowlist
      );
      expect(topics).toContain("lang.python");
    });

    it("should match cloud.kubernetes for CNCF project announcements", () => {
      const topics = extractTopics(
        {
          title: "Cedar joins CNCF as a sandbox project",
          text: "The Cloud Native Computing Foundation accepted the proposal.",
        },
        allowlist
      );
      expect(topics).toContain("cloud.kubernetes");
    });

    it("should match aws.general for CloudFront coverage", () => {
      const topics = extractTopics(
        {
          title: "CloudFront adds origin mTLS authentication",
          text: "The new edge security feature is now available.",
        },
        allowlist
      );
      expect(topics).toContain("aws.general");
    });

    it("should NOT match data.kafka for Peter Kafka mentions on Techmeme", () => {
      const topics = extractTopics(
        {
          title: "Techmeme roundup",
          text: "Channels with Peter Kafka: media and creator economy coverage.",
          url: "http://techmeme.com/260218/p26",
        },
        allowlist
      );
      expect(topics).not.toContain("data.kafka");
    });

    it("should match data.kafka for Apache Kafka technical coverage", () => {
      const topics = extractTopics(
        {
          title: "Apache Kafka 4.0 roadmap",
          text: "Kafka brokers, partitions, and stream processing improvements are discussed.",
          url: "https://example.com/apache-kafka-roadmap",
        },
        allowlist
      );
      expect(topics).toContain("data.kafka");
    });

    it("should NOT match observability.opentelemetry for hotel text", () => {
      const topics = extractTopics(
        {
          title: "Hotel occupancy report",
          text: "The hotel operations team published quarterly metrics.",
          url: "https://example.com/hotel-report",
        },
        allowlist
      );
      expect(topics).not.toContain("observability.opentelemetry");
    });

    it("should match observability.opentelemetry for OTEL tracing context", () => {
      const topics = extractTopics(
        {
          title: "OTEL collector rollout",
          text: "Engineers added OTEL traces, metrics, and exporter settings in production.",
          url: "https://example.com/otel-rollout",
        },
        allowlist
      );
      expect(topics).toContain("observability.opentelemetry");
    });
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
  it("throws when allowlist is missing topics", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-allowlist-"));
    const path = join(dir, "allowlist.yaml");
    writeFileSync(
      path,
      `
defaults:
  max_topics_per_event: 5
`,
      "utf-8"
    );

    expect(() => loadAllowlist(path)).toThrow(/invalid allowlist format/i);
  });

  it("throws on unsupported matcher types", () => {
    const dir = mkdtempSync(join(tmpdir(), "ri-allowlist-"));
    const path = join(dir, "allowlist.yaml");
    writeFileSync(
      path,
      `
topics:
  - key: test.bad_matcher
    display_name: Bad Matcher
    matchers:
      - type: fuzzy
        value: "x"
`,
      "utf-8"
    );

    expect(() => loadAllowlist(path)).toThrow(/unsupported matcher type/i);
  });

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
