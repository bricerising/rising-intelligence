import type { PrismaClient } from "@prisma/client";

export interface ConsumerLagUpdate {
  consumerGroup: string;
  topic: string;
  partition: number;
  currentOffset: bigint;
  latestOffset: bigint;
  lagMessages: bigint;
  observedAt: Date;
}

export async function upsertConsumerLag(
  prisma: PrismaClient,
  update: ConsumerLagUpdate
): Promise<void> {
  await prisma.consumerLag.upsert({
    where: {
      consumerGroup_topic_partition: {
        consumerGroup: update.consumerGroup,
        topic: update.topic,
        partition: update.partition,
      },
    },
    update: {
      currentOffset: update.currentOffset,
      latestOffset: update.latestOffset,
      lagMessages: update.lagMessages,
      updatedAt: update.observedAt,
    },
    create: {
      consumerGroup: update.consumerGroup,
      topic: update.topic,
      partition: update.partition,
      currentOffset: update.currentOffset,
      latestOffset: update.latestOffset,
      lagMessages: update.lagMessages,
      updatedAt: update.observedAt,
    },
  });
}
