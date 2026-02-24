import {
  isRateLimitError,
  isTransientError,
} from "@rising-intelligence/shared/resilience";
import {
  incrementEventsFailed,
  incrementRateLimitBackoff,
  type CollectorErrorType,
  type HealthContext,
} from "./health.js";
import type { Source } from "./types.js";

export interface AdapterBackoffController {
  waitRateLimit(): Promise<void>;
  waitTransient(): Promise<void>;
}

export interface AdapterErrorPolicyContext {
  healthContext: HealthContext;
  adapterSource: Source;
  backoff: AdapterBackoffController;
}

interface AdapterErrorHandlingInput extends AdapterErrorPolicyContext {
  error: unknown;
}

interface AdapterErrorHandlingStrategy {
  readonly name: string;
  matches(error: unknown): boolean;
  handle(input: AdapterErrorHandlingInput): Promise<void>;
}

export interface AdapterErrorPolicy {
  handle(error: unknown, context: AdapterErrorPolicyContext): Promise<void>;
}

function mapUnknownErrorType(error: unknown): CollectorErrorType {
  if (!(error instanceof Error)) {
    return "parse_error";
  }

  const normalized = error.message.toLowerCase();
  if (normalized.includes("kafka")) {
    return "kafka_error";
  }
  if (
    normalized.includes("auth")
    || normalized.includes("unauthorized")
    || normalized.includes("forbidden")
  ) {
    return "auth_error";
  }
  return "parse_error";
}

const RATE_LIMIT_STRATEGY: AdapterErrorHandlingStrategy = {
  name: "rate-limit",
  matches: isRateLimitError,
  async handle(input): Promise<void> {
    incrementEventsFailed(input.healthContext, input.adapterSource, "rate_limit");
    incrementRateLimitBackoff(input.healthContext, input.adapterSource);
    await input.backoff.waitRateLimit();
  },
};

const TRANSIENT_STRATEGY: AdapterErrorHandlingStrategy = {
  name: "transient",
  matches: isTransientError,
  async handle(input): Promise<void> {
    incrementEventsFailed(input.healthContext, input.adapterSource, "network_error");
    await input.backoff.waitTransient();
  },
};

const FALLBACK_STRATEGY: AdapterErrorHandlingStrategy = {
  name: "fallback",
  matches: () => true,
  async handle(input): Promise<void> {
    incrementEventsFailed(
      input.healthContext,
      input.adapterSource,
      mapUnknownErrorType(input.error)
    );
    await input.backoff.waitTransient();
  },
};

const DEFAULT_ERROR_HANDLING_STRATEGIES: ReadonlyArray<AdapterErrorHandlingStrategy> = [
  RATE_LIMIT_STRATEGY,
  TRANSIENT_STRATEGY,
  FALLBACK_STRATEGY,
];

export function createAdapterErrorPolicy(
  strategies: ReadonlyArray<AdapterErrorHandlingStrategy> = DEFAULT_ERROR_HANDLING_STRATEGIES
): AdapterErrorPolicy {
  if (strategies.length === 0) {
    throw new Error("Adapter error policy requires at least one strategy");
  }

  return {
    async handle(error, context): Promise<void> {
      for (const strategy of strategies) {
        if (!strategy.matches(error)) {
          continue;
        }

        await strategy.handle({
          error,
          ...context,
        });
        return;
      }

      throw new Error("Adapter error policy could not match any strategy");
    },
  };
}
