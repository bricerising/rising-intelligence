import type { EachBatchPayload } from "kafkajs";

export interface KafkaBatchLifecycle {
  shouldContinue(): boolean;
  onMessageHandled(): Promise<void>;
  flushHeartbeat(): Promise<void>;
}

/**
 * Proxy around KafkaJS batch lifecycle signals so consumers share consistent
 * heartbeat cadence and stale/running checks.
 */
export function createKafkaBatchLifecycle(
  input: Pick<EachBatchPayload, "isRunning" | "isStale" | "heartbeat">,
  heartbeatIntervalMessages: number
): KafkaBatchLifecycle {
  if (!Number.isInteger(heartbeatIntervalMessages) || heartbeatIntervalMessages <= 0) {
    throw new Error(
      `heartbeatIntervalMessages must be a positive integer, received: ${heartbeatIntervalMessages}`
    );
  }

  let messagesSinceHeartbeat = 0;

  return {
    shouldContinue(): boolean {
      return input.isRunning() && !input.isStale();
    },
    async onMessageHandled(): Promise<void> {
      messagesSinceHeartbeat += 1;
      if (messagesSinceHeartbeat < heartbeatIntervalMessages) {
        return;
      }

      await input.heartbeat();
      messagesSinceHeartbeat = 0;
    },
    async flushHeartbeat(): Promise<void> {
      await input.heartbeat();
      messagesSinceHeartbeat = 0;
    },
  };
}
