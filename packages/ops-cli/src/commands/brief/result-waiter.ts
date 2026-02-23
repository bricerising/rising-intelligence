import type { Kafka } from "kafkajs";

interface RequestScopedResult {
  request_id: string;
}

interface MessagePayload {
  message: {
    value: Buffer | null;
  };
}

interface RequestResultConsumer {
  connect(): Promise<void>;
  subscribe(input: { topic: string; fromBeginning: boolean }): Promise<void>;
  run(input: { eachMessage(payload: MessagePayload): Promise<void> }): Promise<void>;
  stop(): Promise<void>;
  disconnect(): Promise<void>;
}

export interface RequestResultWaiter<TResult extends RequestScopedResult> {
  waitForResult(): Promise<TResult>;
  disconnect(): Promise<void>;
}

export interface SetupRequestResultWaiterInput<TResult extends RequestScopedResult> {
  kafka: Kafka;
  groupId: string;
  topic: string;
  requestId: string;
  timeoutSeconds: number;
  timeoutErrorMessage: string;
  parseResult(rawValue: string): TResult | null;
  fromBeginning?: boolean;
  startupDelayMs?: number;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function tryParseResult<TResult extends RequestScopedResult>(
  rawValue: string,
  parseResult: (rawValue: string) => TResult | null
): TResult | null {
  try {
    return parseResult(rawValue);
  } catch {
    return null;
  }
}

/**
 * Proxy around a Kafka consumer lifecycle for request-scoped result messages.
 * Callers provide request identity and parsing while this helper owns timeout,
 * stop/disconnect, and run() rejection handling.
 */
export async function setupRequestResultWaiter<TResult extends RequestScopedResult>(
  input: SetupRequestResultWaiterInput<TResult>
): Promise<RequestResultWaiter<TResult>> {
  const consumer = input.kafka.consumer({ groupId: input.groupId }) as RequestResultConsumer;
  await consumer.connect();
  await consumer.subscribe({
    topic: input.topic,
    fromBeginning: input.fromBeginning ?? true,
  });

  let settled = false;
  let timeoutHandle: NodeJS.Timeout | null = null;
  let stopPromise: Promise<void> | null = null;
  let resolveResult: ((result: TResult) => void) | undefined;
  let rejectResult: ((error: unknown) => void) | undefined;

  const requestStop = (): Promise<void> => {
    if (!stopPromise) {
      stopPromise = consumer.stop().catch((error) => {
        settleFailure(error);
        throw error;
      });
    }
    return stopPromise;
  };

  const settleSuccess = (result: TResult): void => {
    if (settled) {
      return;
    }
    settled = true;
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
      timeoutHandle = null;
    }
    resolveResult?.(result);
  };

  const settleFailure = (error: unknown): void => {
    if (settled) {
      return;
    }
    settled = true;
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
      timeoutHandle = null;
    }
    rejectResult?.(error);
  };

  const resultPromise = new Promise<TResult>((resolve, reject) => {
    resolveResult = resolve;
    rejectResult = reject;
  });

  timeoutHandle = setTimeout(() => {
    settleFailure(new Error(input.timeoutErrorMessage));
    void requestStop().catch(() => undefined);
  }, input.timeoutSeconds * 1000);

  const runPromise = consumer.run({
    eachMessage: async ({ message }): Promise<void> => {
      if (settled || !message.value) {
        return;
      }

      const result = tryParseResult(message.value.toString("utf-8"), input.parseResult);
      if (!result || result.request_id !== input.requestId) {
        return;
      }

      settleSuccess(result);
      // Avoid awaiting stop() inside eachMessage; KafkaJS may wait for handler completion.
      void requestStop().catch(() => undefined);
    },
  });

  void runPromise.catch((error) => {
    settleFailure(error);
  });

  if ((input.startupDelayMs ?? 0) > 0) {
    await delay(input.startupDelayMs ?? 0);
  }

  return {
    waitForResult(): Promise<TResult> {
      return resultPromise;
    },
    async disconnect(): Promise<void> {
      try {
        await requestStop();
      } catch {
        // Consumer may already be stopped; swallow cleanup errors for caller simplicity.
      }
      await consumer.disconnect();
    },
  };
}
