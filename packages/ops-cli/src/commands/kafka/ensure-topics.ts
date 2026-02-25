import { Kafka, type Admin } from "kafkajs";
import type { CliFlags } from "../../lib/args.js";
import { getStringFlag, parseKafkaBrokers } from "../../lib/flags.js";
import { parseNonNegativeIntegerStrict, parsePositiveIntegerStrict } from "../../lib/number.js";
import { REQUIRED_TOPICS, type TopicConfig } from "./required-topics.js";

interface EnsureTopicsConfig {
  brokers: string[];
  topics: TopicConfig[];
  waitTimeoutMs: number;
  pollIntervalMs: number;
}

function parseTopicNames(rawValue: string): string[] {
  const names = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  const deduped = [...new Set(names)];
  if (deduped.length === 0) {
    throw new Error("--topics resolved to an empty list");
  }
  return deduped;
}

export function resolveEnsureTopicsConfig(
  flags: CliFlags,
  env: NodeJS.ProcessEnv = process.env
): EnsureTopicsConfig {
  const kafkaBrokersRaw =
    getStringFlag(flags, "kafka-brokers") || env.KAFKA_BROKERS || "localhost:9092";
  const brokers = parseKafkaBrokers(kafkaBrokersRaw);

  const waitTimeoutMsRaw = getStringFlag(flags, "wait-timeout-ms") || "15000";
  const pollIntervalMsRaw = getStringFlag(flags, "poll-interval-ms") || "250";
  const waitTimeoutMs = parseNonNegativeIntegerStrict(waitTimeoutMsRaw, "--wait-timeout-ms");
  const pollIntervalMs = parsePositiveIntegerStrict(pollIntervalMsRaw, "--poll-interval-ms");

  const topicsRaw = getStringFlag(flags, "topics");
  if (!topicsRaw) {
    return {
      brokers,
      topics: [...REQUIRED_TOPICS],
      waitTimeoutMs,
      pollIntervalMs,
    };
  }

  const partitionsRaw = getStringFlag(flags, "partitions") || "1";
  const replicationFactorRaw = getStringFlag(flags, "replication-factor") || "1";
  const numPartitions = parsePositiveIntegerStrict(partitionsRaw, "--partitions");
  const replicationFactor = parsePositiveIntegerStrict(
    replicationFactorRaw,
    "--replication-factor"
  );
  const topicNames = parseTopicNames(topicsRaw);

  return {
    brokers,
    topics: topicNames.map((topic) => ({
      topic,
      numPartitions,
      replicationFactor,
    })),
    waitTimeoutMs,
    pollIntervalMs,
  };
}

function topicsNeedingCreate(existingTopics: string[], topics: TopicConfig[]): TopicConfig[] {
  return topics.filter((topic) => !existingTopics.includes(topic.topic));
}

function hasLeadersForTopics(metadata: Awaited<ReturnType<Admin["fetchTopicMetadata"]>>): boolean {
  return metadata.topics.every((topic) =>
    topic.partitions.length > 0 && topic.partitions.every((partition) => partition.leader >= 0)
  );
}

async function waitForTopicLeaders(
  admin: Admin,
  topics: string[],
  timeoutMs: number,
  pollIntervalMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const metadata = await admin.fetchTopicMetadata({ topics });
    if (hasLeadersForTopics(metadata)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  throw new Error(
    `Timed out waiting for topic leaders after ${timeoutMs}ms for topics: ${topics.join(", ")}`
  );
}

export async function kafkaEnsureTopics(flags: CliFlags): Promise<void> {
  const config = resolveEnsureTopicsConfig(flags);
  const kafka = new Kafka({
    clientId: "riops-kafka-ensure-topics",
    brokers: config.brokers,
  });
  const admin = kafka.admin();

  try {
    await admin.connect();

    const existingTopics = await admin.listTopics();
    const toCreate = topicsNeedingCreate(existingTopics, config.topics);

    if (toCreate.length > 0) {
      // eslint-disable-next-line no-console
      console.log(`Creating ${toCreate.length} missing topic(s)...`);
      await admin.createTopics({
        waitForLeaders: true,
        topics: toCreate.map((topic) => ({
          topic: topic.topic,
          numPartitions: topic.numPartitions,
          replicationFactor: topic.replicationFactor,
        })),
      });
    }

    const allTopicNames = config.topics.map((topic) => topic.topic);
    await waitForTopicLeaders(admin, allTopicNames, config.waitTimeoutMs, config.pollIntervalMs);

    const metadata = await admin.fetchTopicMetadata({ topics: allTopicNames });
    const metadataByTopic = new Map(metadata.topics.map((topic) => [topic.name, topic]));

    // eslint-disable-next-line no-console
    console.log(`Kafka topics ready (${allTopicNames.length}):`);
    for (const topic of config.topics) {
      const topicMetadata = metadataByTopic.get(topic.topic);
      if (!topicMetadata) {
        throw new Error(`Topic metadata missing after ensure: ${topic.topic}`);
      }
      // eslint-disable-next-line no-console
      console.log(`  ✓ ${topic.topic} (partitions: ${topicMetadata.partitions.length})`);
    }
  } finally {
    await admin.disconnect();
  }
}
