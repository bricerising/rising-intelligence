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

/**
 * Check if an error is a rate limit error (HTTP 429).
 */
export function isRateLimitError(error: unknown): boolean {
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    if (message.includes("429") || message.includes("rate limit")) {
      return true;
    }
    // Check for nested cause
    if ("cause" in error && error.cause instanceof Error) {
      return isRateLimitError(error.cause);
    }
  }
  // Check for fetch response-like objects
  if (error && typeof error === "object" && "status" in error) {
    return (error as { status: number }).status === 429;
  }
  return false;
}

/**
 * Check if an error is a transient error (5xx, network).
 */
export function isTransientError(error: unknown): boolean {
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    // Network errors
    if (
      message.includes("econnrefused") ||
      message.includes("enotfound") ||
      message.includes("etimedout") ||
      message.includes("econnreset") ||
      message.includes("socket hang up") ||
      message.includes("network")
    ) {
      return true;
    }
    // Check for nested cause
    if ("cause" in error && error.cause instanceof Error) {
      return isTransientError(error.cause);
    }
  }
  // Check for fetch response-like objects (5xx)
  if (error && typeof error === "object" && "status" in error) {
    const status = (error as { status: number }).status;
    return status >= 500 && status < 600;
  }
  return false;
}
