/**
 * Mock source adapters for testing ingestion without hitting real APIs.
 *
 * Usage:
 *   const adapter = new MockSourceAdapter({
 *     source: 'rss',
 *     events: [fixtures.events.awsBedrock],
 *   });
 *   const events = await adapter.fetch();
 */

// Note: Import actual types once they exist
type Source = 'rss' | 'news' | 'hackernews' | 'reddit' | 'github' | 'bluesky' | 'mastodon';

interface RawEvent {
  event_id: string;
  source: Source;
  fetched_at: string;
  [key: string]: unknown;
}

export interface MockSourceConfig {
  source: Source;
  events: RawEvent[];
  delayMs?: number;
  failAfterCalls?: number;
  rateLimitAfterCalls?: number;
}

export interface SourceAdapter {
  fetch(): Promise<RawEvent[]>;
}

export class MockSourceAdapter implements SourceAdapter {
  private config: MockSourceConfig;
  private callCount = 0;

  constructor(config: MockSourceConfig) {
    this.config = config;
  }

  /**
   * Get the number of times fetch() was called.
   */
  getCallCount(): number {
    return this.callCount;
  }

  /**
   * Reset call count.
   */
  reset(): void {
    this.callCount = 0;
  }

  /**
   * Simulate fetching events from the source.
   */
  async fetch(): Promise<RawEvent[]> {
    this.callCount++;

    // Simulate rate limiting
    if (this.config.rateLimitAfterCalls && this.callCount > this.config.rateLimitAfterCalls) {
      const error = new Error('Rate limited') as Error & { status: number };
      error.status = 429;
      throw error;
    }

    // Simulate failure
    if (this.config.failAfterCalls && this.callCount > this.config.failAfterCalls) {
      throw new Error(`MockSourceAdapter: Simulated failure for ${this.config.source}`);
    }

    // Simulate network latency
    if (this.config.delayMs && this.config.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.config.delayMs));
    }

    // Return configured events with updated fetched_at
    return this.config.events.map((event) => ({
      ...event,
      fetched_at: new Date().toISOString(),
    }));
  }
}

/**
 * Factory to create mock adapters for multiple sources.
 */
export class MockSourceFactory {
  private adapters: Map<Source, MockSourceAdapter> = new Map();

  register(config: MockSourceConfig): MockSourceAdapter {
    const adapter = new MockSourceAdapter(config);
    this.adapters.set(config.source, adapter);
    return adapter;
  }

  get(source: Source): MockSourceAdapter | undefined {
    return this.adapters.get(source);
  }

  getAll(): Map<Source, MockSourceAdapter> {
    return this.adapters;
  }

  resetAll(): void {
    for (const adapter of this.adapters.values()) {
      adapter.reset();
    }
  }
}
