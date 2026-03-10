import { Kafka } from "kafkajs";
import { loadDotEnv } from "@rising-intelligence/shared/env";
import type { CliFlags } from "../../lib/args.js";
import { getStringFlag, parseKafkaBrokers } from "../../lib/flags.js";
import { REQUIRED_TOPICS } from "./required-topics.js";

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
        numPartitions: topic.numPartitions,
        replicationFactor: topic.replicationFactor,
      })),
    });

    for (const topic of topicsToCreate) {
      // eslint-disable-next-line no-console
      console.log(
        `✓ Created topic: ${topic.topic} (partitions: ${topic.numPartitions}, replication: ${topic.replicationFactor})`
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
