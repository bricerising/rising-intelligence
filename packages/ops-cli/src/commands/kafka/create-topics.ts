import { Kafka } from "kafkajs";
import { loadDotEnv } from "@rising-intelligence/shared";
import type { CliFlags } from "../../lib/args.js";
import { getStringFlag, parseKafkaBrokers } from "../../lib/flags.js";

interface TopicConfig {
  topic: string;
  numPartitions?: number;
  replicationFactor?: number;
}

const REQUIRED_TOPICS: TopicConfig[] = [
  { topic: "events.raw", numPartitions: 3, replicationFactor: 1 },
  { topic: "events.raw.dlq", numPartitions: 1, replicationFactor: 1 },
  { topic: "trends.snapshots", numPartitions: 1, replicationFactor: 1 },
  { topic: "summary.requests", numPartitions: 1, replicationFactor: 1 },
  { topic: "summary.results", numPartitions: 1, replicationFactor: 1 },
  { topic: "collector.heartbeat", numPartitions: 1, replicationFactor: 1 },
];

export function resolveKafkaBrokers(
  flags: CliFlags,
  env: NodeJS.ProcessEnv = process.env
): string[] {
  const fromFlag = getStringFlag(flags, "kafka-brokers");
  if (fromFlag !== undefined) {
    return parseKafkaBrokers(fromFlag);
  }

  const fromEnv = env.KAFKA_BROKERS;
  if (fromEnv !== undefined) {
    return parseKafkaBrokers(fromEnv);
  }

  return ["localhost:9092"];
}

export async function kafkaCreateTopics(flags: CliFlags): Promise<void> {
  loadDotEnv();
  const brokers = resolveKafkaBrokers(flags);

  // eslint-disable-next-line no-console
  console.log(`Creating topics on Kafka brokers: ${brokers.join(", ")}`);

  const kafka = new Kafka({
    clientId: "ops-cli-create-topics",
    brokers,
  });

  const admin = kafka.admin();

  try {
    await admin.connect();
    // eslint-disable-next-line no-console
    console.log("Connected to Kafka");

    const existingTopics = await admin.listTopics();
    // eslint-disable-next-line no-console
    console.log(`Found ${existingTopics.length} existing topics`);

    const topicsToCreate = REQUIRED_TOPICS.filter(
      (topic) => !existingTopics.includes(topic.topic)
    );

    if (topicsToCreate.length === 0) {
      // eslint-disable-next-line no-console
      console.log("All required topics already exist");
      return;
    }

    // eslint-disable-next-line no-console
    console.log(`Creating ${topicsToCreate.length} topics...`);

    await admin.createTopics({
      topics: topicsToCreate.map((topic) => ({
        topic: topic.topic,
        numPartitions: topic.numPartitions ?? 1,
        replicationFactor: topic.replicationFactor ?? 1,
      })),
    });

    for (const topic of topicsToCreate) {
      // eslint-disable-next-line no-console
      console.log(
        `✓ Created topic: ${topic.topic} (partitions: ${topic.numPartitions ?? 1}, replication: ${topic.replicationFactor ?? 1})`
      );
    }

    // eslint-disable-next-line no-console
    console.log("Topic creation completed successfully");
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error("Failed to create topics:", error);
    throw error;
  } finally {
    await admin.disconnect();
  }
}
