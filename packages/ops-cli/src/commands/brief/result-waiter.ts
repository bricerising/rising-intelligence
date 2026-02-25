import {
  createConsumerConnection,
  createMessageBatchStrategy,
  type PipelineLogger,
} from "@rising-intelligence/pipeline/transport";

interface RequestScopedResult {
  request_id: string;
}

export interface RequestResultWaiter<TResult extends RequestScopedResult> {
  waitForResult(): Promise<TResult>;
  disconnect(): Promise<void>;
}

export interface SetupRequestResultWaiterInput<TResult extends RequestScopedResult> {
  kafkaBrokers: string[];
  kafkaClientId: string;
  groupId: string;
  topic: string;
  requestId: string;
  timeoutSeconds: number;
  timeoutErrorMessage: string;
  parseResult(rawValue: string): TResult | null;
  fromBeginning?: boolean;
  startupDelayMs?: number;
}

const NOOP_LOGGER: PipelineLogger = {
  error() {},
  warn() {},
  info() {},
  debug() {},
};

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
 * Proxy around a request-scoped pipeline consumer.
 * The waiter resolves once a result for requestId is observed or timeout elapses.
 */
export async function setupRequestResultWaiter<TResult extends RequestScopedResult>(
  input: SetupRequestResultWaiterInput<TResult>
): Promise<RequestResultWaiter<TResult>> {
  const consumerConnection = await createConsumerConnection({
    brokers: input.kafkaBrokers,
    clientId: input.kafkaClientId,
    groupId: input.groupId,
    logger: NOOP_LOGGER,
  });

  let settled = false;
  let timeoutHandle: NodeJS.Timeout | null = null;
  let stopPromise: Promise<void> | null = null;
  let resolveResult: ((result: TResult) => void) | undefined;
  let rejectResult: ((error: unknown) => void) | undefined;

  const requestStop = (): Promise<void> => {
    if (!stopPromise) {
      stopPromise = consumerConnection.disconnect().catch((error) => {
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

  const strategy = createMessageBatchStrategy({
    strategy: {
      deserialize: (value: Buffer) => value,
      async onMessage(
        _ctx: void,
        _messageContext,
        messageValue: Buffer
      ): Promise<void> {
        if (settled) {
          return;
        }

        const result = tryParseResult(messageValue.toString("utf-8"), input.parseResult);
        if (!result || result.request_id !== input.requestId) {
          return;
        }

        settleSuccess(result);
        void requestStop().catch(() => undefined);
      },
    },
    acknowledge: true,
    progressInterval: 20,
  });

  const runPromise = consumerConnection.consume({
    topics: input.topic,
    ctx: undefined,
    strategy,
    fromBeginning: input.fromBeginning ?? true,
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
        // Ignore cleanup failures for caller simplicity.
      }
    },
  };
}
