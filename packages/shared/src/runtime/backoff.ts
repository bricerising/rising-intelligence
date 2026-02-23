import type { Logger } from "pino";

export interface BackoffConfig {
  baseDelayMs: number;
  maxDelayMs: number;
  jitterFactor: number;
}

const DEFAULT_RATE_LIMIT_CONFIG: BackoffConfig = {
  baseDelayMs: 30_000, // 30 seconds
  maxDelayMs: 15 * 60 * 1000, // 15 minutes
  jitterFactor: 0.2,
};

const DEFAULT_TRANSIENT_CONFIG: BackoffConfig = {
  baseDelayMs: 5_000, // 5 seconds
  maxDelayMs: 5 * 60 * 1000, // 5 minutes
  jitterFactor: 0.2,
};

export class BackoffManager {
  private attempts = 0;
  private rateLimitConfig: BackoffConfig;
  private transientConfig: BackoffConfig;

  constructor(
    private readonly name: string,
    private readonly logger: Logger,
    rateLimitConfig?: Partial<BackoffConfig>,
    transientConfig?: Partial<BackoffConfig>
  ) {
    this.rateLimitConfig = { ...DEFAULT_RATE_LIMIT_CONFIG, ...rateLimitConfig };
    this.transientConfig = { ...DEFAULT_TRANSIENT_CONFIG, ...transientConfig };
  }

  reset(): void {
    this.attempts = 0;
  }

  getAttempts(): number {
    return this.attempts;
  }

  async waitRateLimit(): Promise<void> {
    await this.wait(this.rateLimitConfig, "rate_limit");
  }

  async waitTransient(): Promise<void> {
    await this.wait(this.transientConfig, "transient");
  }

  private async wait(config: BackoffConfig, reason: string): Promise<void> {
    const exponentialDelay = config.baseDelayMs * Math.pow(2, this.attempts);
    const cappedDelay = Math.min(exponentialDelay, config.maxDelayMs);
    const jitter = cappedDelay * config.jitterFactor * Math.random();
    const totalDelay = Math.floor(cappedDelay + jitter);

    this.logger.info(
      {
        adapter: this.name,
        delayMs: totalDelay,
        attempt: this.attempts,
        reason,
      },
      "Backing off"
    );

    await sleep(totalDelay);
    this.attempts++;
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface ErrorClassificationHandler {
  readonly name: string;
  matches(candidate: unknown): boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}

function getStatusCode(candidate: unknown): number | null {
  if (!isRecord(candidate) || !("status" in candidate)) {
    return null;
  }
  const status = candidate.status;
  if (typeof status !== "number" || !Number.isFinite(status)) {
    return null;
  }
  return status;
}

function getErrorMessage(candidate: unknown): string | null {
  if (!(candidate instanceof Error)) {
    return null;
  }

  return candidate.message.toLowerCase();
}

function includesAnyTerm(value: string | null, terms: readonly string[]): boolean {
  if (!value) {
    return false;
  }

  return terms.some((term) => value.includes(term));
}

function getCause(candidate: unknown): unknown {
  if (!isRecord(candidate) || !("cause" in candidate)) {
    return undefined;
  }
  return candidate.cause;
}

function iterateCandidates(error: unknown): Iterable<unknown> {
  const candidates: unknown[] = [];
  const visitedObjects = new Set<object>();
  let current: unknown = error;

  while (current !== undefined) {
    candidates.push(current);

    if (!isRecord(current)) {
      break;
    }

    if (visitedObjects.has(current)) {
      break;
    }
    visitedObjects.add(current);

    current = getCause(current);
  }

  return candidates;
}

function classifyError(
  error: unknown,
  handlers: readonly ErrorClassificationHandler[]
): boolean {
  for (const candidate of iterateCandidates(error)) {
    for (const handler of handlers) {
      if (handler.matches(candidate)) {
        return true;
      }
    }
  }
  return false;
}

const RATE_LIMIT_HANDLERS: readonly ErrorClassificationHandler[] = [
  {
    name: "status_429",
    matches(candidate): boolean {
      return getStatusCode(candidate) === 429;
    },
  },
  {
    name: "message_rate_limit",
    matches(candidate): boolean {
      return includesAnyTerm(getErrorMessage(candidate), ["429", "rate limit"]);
    },
  },
];

const TRANSIENT_HANDLERS: readonly ErrorClassificationHandler[] = [
  {
    name: "status_5xx",
    matches(candidate): boolean {
      const status = getStatusCode(candidate);
      return status !== null && status >= 500 && status < 600;
    },
  },
  {
    name: "message_network_transient",
    matches(candidate): boolean {
      return includesAnyTerm(getErrorMessage(candidate), [
        "econnrefused",
        "enotfound",
        "etimedout",
        "econnreset",
        "socket hang up",
        "network",
      ]);
    },
  },
];

export function isRateLimitError(error: unknown): boolean {
  return classifyError(error, RATE_LIMIT_HANDLERS);
}

export function isTransientError(error: unknown): boolean {
  return classifyError(error, TRANSIENT_HANDLERS);
}
