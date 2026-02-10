import { Admin, Kafka } from "kafkajs";
import { getEnvString } from "@rising-intelligence/shared";

type Flags = Record<string, string | boolean>;

function getStringFlag(flags: Flags, name: string): string | undefined {
  const value = flags[name];
  return typeof value === "string" ? value : undefined;
}

function parseKafkaBrokers(rawValue: string): string[] {
  const brokers = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS resolved to an empty value");
  }
  return brokers;
}

export async function kafkaTopics(flags: Flags): Promise<void> {
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
