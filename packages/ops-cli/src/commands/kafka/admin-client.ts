import { Kafka } from "kafkajs";

export interface KafkaTopicPartitionOffset {
  partition: number;
  offset: string;
}

export interface KafkaAdminConnection {
  listTopics(): Promise<string[]>;
  fetchTopicOffsets(topic: string): Promise<KafkaTopicPartitionOffset[]>;
  fetchGroupOffsets(groupId: string, topic: string): Promise<KafkaTopicPartitionOffset[]>;
  disconnect(): Promise<void>;
}

export interface CreateKafkaAdminConnectionOptions {
  brokers: string[];
  clientId: string;
}

export async function createKafkaAdminConnection(
  options: CreateKafkaAdminConnectionOptions
): Promise<KafkaAdminConnection> {
  const kafka = new Kafka({
    clientId: options.clientId,
    brokers: options.brokers,
  });
  const admin = kafka.admin();
  await admin.connect();

  return {
    listTopics(): Promise<string[]> {
      return admin.listTopics();
    },
    fetchTopicOffsets(topic: string): Promise<KafkaTopicPartitionOffset[]> {
      return admin.fetchTopicOffsets(topic);
    },
    async fetchGroupOffsets(
      groupId: string,
      topic: string
    ): Promise<KafkaTopicPartitionOffset[]> {
      const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
      return offsets.find((row) => row.topic === topic)?.partitions ?? [];
    },
    async disconnect(): Promise<void> {
      await admin.disconnect();
    },
  };
}
