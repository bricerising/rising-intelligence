import { Admin, Kafka } from "kafkajs";
import { getEnvString } from "@rising-intelligence/shared";
import type { CliFlags } from "../../lib/args.js";
import { getStringFlag, parseKafkaBrokers } from "../../lib/flags.js";

export async function kafkaTopics(flags: CliFlags): Promise<void> {
  const kafkaBrokersRaw =
    getStringFlag(flags, "kafka-brokers") || getEnvString("KAFKA_BROKERS") || "localhost:9092";
  const brokers = parseKafkaBrokers(kafkaBrokersRaw);

  const kafka = new Kafka({
    clientId: "riops-kafka-topics",
    brokers,
  });

  const admin: Admin = kafka.admin();

  try {
    await admin.connect();
    const topics = await admin.listTopics();

    // Sort topics alphabetically
    const sortedTopics = topics.sort();

    // eslint-disable-next-line no-console
    console.log(`\nKafka Topics (${sortedTopics.length}):\n`);
    for (const topic of sortedTopics) {
      // eslint-disable-next-line no-console
      console.log(`  ${topic}`);
    }
    // eslint-disable-next-line no-console
    console.log();
  } finally {
    await admin.disconnect();
  }
}
