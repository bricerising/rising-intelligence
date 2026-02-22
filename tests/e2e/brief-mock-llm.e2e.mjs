import { Kafka } from "kafkajs";

const BROKER = process.env.E2E_KAFKA_BROKER ?? "localhost:9093";
const REQUEST_TOPIC = process.env.E2E_SUMMARY_REQUESTS_TOPIC ?? "summary.requests";
const RESULT_TOPIC = process.env.E2E_SUMMARY_RESULTS_TOPIC ?? "summary.results";
const BRIEF_HEALTH_URL = process.env.E2E_BRIEF_HEALTH_URL ?? "http://localhost:3006/health";
const BRIEF_METRICS_URL = process.env.E2E_BRIEF_METRICS_URL ?? "http://localhost:3006/metrics";
const WAIT_TIMEOUT_MS = Number.parseInt(process.env.E2E_WAIT_TIMEOUT_MS ?? "60000", 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForHealthy(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        const payload = await response.json();
        if (payload?.status === "healthy") {
          return;
        }
      }
    } catch {
      // Retry until timeout.
    }
    await sleep(1000);
  }

  throw new Error(`Timed out waiting for healthy endpoint: ${url}`);
}

function createSummaryRequest(requestId, dailyBudgetUsd) {
  const requestedAt = new Date("2026-02-08T10:00:00.000Z").toISOString();

  return {
    request_id: requestId,
    requested_at: requestedAt,
    type: "daily",
    windows: [1, 2],
    budget: {
      daily_budget_usd: dailyBudgetUsd,
      max_topics: 5,
      max_evidence_per_topic: 3,
      max_output_tokens: 1200,
    },
    topics: [
      {
        topic: "aws.bedrock",
        metrics: [
          {
            topic: "aws.bedrock",
            window: 2,
            score: 12.4,
            volume: 19,
            acceleration: 0.7,
          },
        ],
        evidence: [
          {
            event_id: `${requestId}-event-1`,
            source: "rss",
            url: "https://example.com/bedrock-update",
            title: "Bedrock update",
            published_at: requestedAt,
            fetched_at: requestedAt,
            text_excerpt: "AWS announced a Bedrock update with latency improvements.",
          },
        ],
      },
    ],
  };
}

class ResultCollector {
  constructor(kafka, topic) {
    this.consumer = kafka.consumer({
      groupId: `brief-e2e-${Date.now()}`,
      allowAutoTopicCreation: false,
    });
    this.topic = topic;
    this.messagesByKey = new Map();
    this.listeners = new Set();
    this.runPromise = null;
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });
    this.runPromise = this.consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.key || !message.value) {
          return;
        }

        const key = message.key.toString("utf-8");
        let payload;
        try {
          payload = JSON.parse(message.value.toString("utf-8"));
        } catch {
          return;
        }

        const existing = this.messagesByKey.get(key) ?? [];
        existing.push(payload);
        this.messagesByKey.set(key, existing);

        for (const listener of this.listeners) {
          listener(key, payload);
        }
      },
    });
  }

  getCount(key) {
    return (this.messagesByKey.get(key) ?? []).length;
  }

  async waitForKey(key, timeoutMs) {
    const existing = this.messagesByKey.get(key);
    if (existing && existing.length > 0) {
      return existing[0];
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.listeners.delete(listener);
        reject(new Error(`Timed out waiting for result key: ${key}`));
      }, timeoutMs);

      const listener = (receivedKey, payload) => {
        if (receivedKey !== key) {
          return;
        }
        clearTimeout(timeout);
        this.listeners.delete(listener);
        resolve(payload);
      };

      this.listeners.add(listener);
    });
  }

  async ensureNoAdditional(key, baselineCount, durationMs) {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.listeners.delete(listener);
        resolve();
      }, durationMs);

      const listener = (receivedKey) => {
        if (receivedKey !== key) {
          return;
        }
        if (this.getCount(key) <= baselineCount) {
          return;
        }
        clearTimeout(timeout);
        this.listeners.delete(listener);
        reject(new Error(`Received unexpected additional result for key: ${key}`));
      };

      this.listeners.add(listener);
    });
  }

  async waitForAdditional(key, baselineCount, timeoutMs) {
    const existing = this.messagesByKey.get(key) ?? [];
    if (existing.length > baselineCount) {
      return existing[existing.length - 1];
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.listeners.delete(listener);
        reject(new Error(`Timed out waiting for additional result key: ${key}`));
      }, timeoutMs);

      const listener = (receivedKey, payload) => {
        if (receivedKey !== key) {
          return;
        }
        if (this.getCount(key) <= baselineCount) {
          return;
        }
        clearTimeout(timeout);
        this.listeners.delete(listener);
        resolve(payload);
      };

      this.listeners.add(listener);
    });
  }

  async stop() {
    try {
      await this.consumer.stop();
    } catch {
      // Ignore stop errors during cleanup.
    }

    if (this.runPromise) {
      try {
        await this.runPromise;
      } catch {
        // Ignore run-loop errors during cleanup.
      }
    }

    try {
      await this.consumer.disconnect();
    } catch {
      // Ignore disconnect errors during cleanup.
    }
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseCounter(metrics, name, labels = "") {
  const labelPart = labels.length > 0 ? `{${labels}}` : "";
  const pattern = new RegExp(`^${escapeRegex(`${name}${labelPart}`)} (.+)$`, "m");
  const match = metrics.match(pattern);
  if (!match) {
    throw new Error(`Metric not found: ${name}${labelPart}`);
  }
  return Number.parseFloat(match[1]);
}

async function main() {
  await waitForHealthy(BRIEF_HEALTH_URL, WAIT_TIMEOUT_MS);

  const kafka = new Kafka({
    clientId: "brief-e2e-runner",
    brokers: [BROKER],
  });

  const admin = kafka.admin();
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  const collector = new ResultCollector(kafka, RESULT_TOPIC);

  try {
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        { topic: REQUEST_TOPIC, numPartitions: 1, replicationFactor: 1 },
        { topic: RESULT_TOPIC, numPartitions: 1, replicationFactor: 1 },
      ],
    });

    await producer.connect();
    await collector.start();

    const firstRequest = createSummaryRequest("e2e-mock-llm-req-1", 0.02);
    await producer.send({
      topic: REQUEST_TOPIC,
      messages: [
        {
          key: firstRequest.request_id,
          value: Buffer.from(JSON.stringify(firstRequest), "utf-8"),
        },
      ],
    });

    const firstResult = await collector.waitForKey(firstRequest.request_id, WAIT_TIMEOUT_MS);
    assert(firstResult?.brief, "First request did not produce a success brief payload");
    assert(firstResult.brief.meta?.provider === "mock-llm", "Expected provider=mock-llm in first result");

    const baselineCount = collector.getCount(firstRequest.request_id);
    await producer.send({
      topic: REQUEST_TOPIC,
      messages: [
        {
          key: firstRequest.request_id,
          value: Buffer.from(JSON.stringify(firstRequest), "utf-8"),
        },
      ],
    });
    const duplicateResult = await collector.waitForAdditional(
      firstRequest.request_id,
      baselineCount,
      WAIT_TIMEOUT_MS
    );
    assert(duplicateResult?.brief, "Duplicate request did not republish a success brief payload");

    const secondRequest = createSummaryRequest("e2e-mock-llm-req-2", 0.001);
    await producer.send({
      topic: REQUEST_TOPIC,
      messages: [
        {
          key: secondRequest.request_id,
          value: Buffer.from(JSON.stringify(secondRequest), "utf-8"),
        },
      ],
    });

    const secondResult = await collector.waitForKey(secondRequest.request_id, WAIT_TIMEOUT_MS);
    assert(secondResult?.failure, "Second request did not produce a failure payload");
    assert(
      secondResult.failure.error_code === "budget_exceeded",
      `Expected budget_exceeded error, received: ${secondResult.failure.error_code}`
    );

    const metricsResponse = await fetch(BRIEF_METRICS_URL);
    assert(metricsResponse.ok, "Failed to fetch brief metrics endpoint");
    const metrics = await metricsResponse.text();

    assert(parseCounter(metrics, "ri_brief_generation_total", 'status="success"') >= 1, "Missing success metric");
    assert(parseCounter(metrics, "ri_brief_generation_total", 'status="skipped"') >= 2, "Expected skipped metric >= 2");
    assert(
      parseCounter(metrics, "ri_brief_llm_tokens_total", 'direction="input"') > 0,
      "Expected llm input token metric to be > 0"
    );

    // eslint-disable-next-line no-console
    console.log("Brief mock-LLM e2e test passed.");
  } finally {
    await collector.stop();
    try {
      await producer.disconnect();
    } catch {
      // Ignore disconnect errors during cleanup.
    }
    try {
      await admin.disconnect();
    } catch {
      // Ignore disconnect errors during cleanup.
    }
  }
}

main().catch((error) => {
  // eslint-disable-next-line no-console
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
