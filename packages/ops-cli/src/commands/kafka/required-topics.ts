export interface TopicConfig {
  topic: string;
  numPartitions: number;
  replicationFactor: number;
}

export const REQUIRED_TOPICS: readonly TopicConfig[] = [
  { topic: "events.raw", numPartitions: 3, replicationFactor: 1 },
  { topic: "events.raw.dlq", numPartitions: 1, replicationFactor: 1 },
  { topic: "trends.snapshots", numPartitions: 1, replicationFactor: 1 },
  { topic: "summary.requests", numPartitions: 1, replicationFactor: 1 },
  { topic: "summary.results", numPartitions: 1, replicationFactor: 1 },
  { topic: "collector.heartbeat", numPartitions: 1, replicationFactor: 1 },
];
