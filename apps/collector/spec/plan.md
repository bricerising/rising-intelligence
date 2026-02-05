# Implementation Plan: Collector Service

## Overview

Build `apps/collector` as a simple ingestion service: External APIs → Kafka. No database dependencies.

## Architecture (High Level)

- **Input**: External APIs (RSS, HN, Reddit)
- **Output**: Kafka (`events.raw`, `events.raw.dlq`)
- **State**: Local checkpoints only (SQLite file)
- **No dependencies**: No Postgres, no Redis

```
External APIs → Adapters → Normalizer → Kafka
                              ↓
                       Local Checkpoints
```

## Dependencies

```json
{
  "@rising-intelligence/shared": "workspace:*",
  "kafkajs": "^2.x",
  "better-sqlite3": "^9.x",
  "rss-parser": "^3.x",
  "node-fetch": "^3.x"
}
```

Note: No `@rising-intelligence/db` — collector doesn't use Prisma.

## Phases

### Phase 1: Service skeleton + Kafka publish

- Create service bootstrap and config
- Implement `RawEvent` schema validation at the boundary
- Publish to `events.raw` (Kafka)
- DLQ for failures

**Deliverables**:
- `src/index.ts` - service entry point
- `src/config.ts` - environment config
- `src/kafka/producer.ts` - Kafka producer
- `src/normalizer.ts` - common normalization logic
- `src/validator.ts` - RawEvent schema validation

### Phase 2: MVP sources + checkpoints

- RSS/Atom adapter (poll + checkpoint)
- Hacker News adapter (poll + checkpoint)
- Reddit adapter (poll + checkpoint)
- SQLite checkpoint storage

**Deliverables**:
- `src/adapters/rss.ts`
- `src/adapters/hackernews.ts`
- `src/adapters/reddit.ts`
- `src/checkpoint.ts` - SQLite checkpoint read/write

### Phase 3: Reliability + ops

- Exponential backoff with jitter
- In-memory rate limit tracking
- Metrics + traces
- Health check endpoints

**Deliverables**:
- `src/backoff.ts` - backoff logic
- `src/ratelimit.ts` - in-memory rate limit tracking
- `src/health.ts` - `/healthz` and `/readyz` endpoints
- Grafana dashboard for collector metrics

### Phase 4: Additional sources (post-MVP)

- GitHub releases adapter
- Twitter/X adapter (if API access available)

## Key Implementation Details

### Adapter Interface

```typescript
interface SourceAdapter {
  name: string;
  pollIntervalMs: number;

  // Load checkpoint from local storage
  getCheckpoint(): Promise<Record<string, string>>;

  // Fetch new items since checkpoint
  fetch(): AsyncIterable<{ event: RawEvent; checkpoint: Record<string, string> }>;

  // Save checkpoint after successful batch
  saveCheckpoint(checkpoint: Record<string, string>): Promise<void>;
}
```

### Main Loop

```typescript
async function runAdapter(adapter: SourceAdapter) {
  const producer = await createKafkaProducer();
  const backoff = new BackoffManager(adapter.name);
  const checkpoints = new CheckpointStore(config.CHECKPOINT_DB_PATH);

  while (true) {
    try {
      let lastCheckpoint: Record<string, string> | null = null;
      let batchSize = 0;

      for await (const { event, checkpoint } of adapter.fetch()) {
        // Best-effort dedup before publish (SQLite seen cache)
        if (await checkpoints.hasSeen(event.source, event.event_id)) {
          metrics.increment('collector_duplicates_skipped_total', { source: event.source });
          continue;
        }

        // Validate
        const validated = validateRawEvent(event);
        if (!validated.success) {
          await publishToDLQ(producer, event, validated.error);
          continue;
        }

        // Publish to Kafka
        await producer.send({
          topic: 'events.raw',
          messages: [{ key: event.event_id, value: serialize(event) }],
        });

        await checkpoints.markSeen(event.source, event.event_id);
        lastCheckpoint = checkpoint;
        batchSize++;
      }

      // Save checkpoint after batch
      if (lastCheckpoint) {
        await adapter.saveCheckpoint(lastCheckpoint);
        log.info({ adapter: adapter.name, batchSize }, 'Batch complete');
      }

      // Reset backoff on success
      backoff.reset();

      // Wait for next poll
      await sleep(adapter.pollIntervalMs);

    } catch (error) {
      log.error({ adapter: adapter.name, error }, 'Adapter error');

      if (isRateLimitError(error)) {
        await backoff.waitRateLimit();
      } else {
        await backoff.waitTransient();
      }
    }
  }
}
```

### Checkpoint Storage (SQLite)

```typescript
import Database from 'better-sqlite3';

class CheckpointStore {
  private db: Database.Database;

  constructor(path: string) {
    this.db = new Database(path);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS checkpoints (
        source TEXT NOT NULL,
        checkpoint_key TEXT NOT NULL,
        checkpoint_value TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (source, checkpoint_key)
      )
    `);

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS seen_events (
        source TEXT NOT NULL,
        event_id TEXT NOT NULL,
        seen_at TEXT NOT NULL,
        PRIMARY KEY (source, event_id)
      );
      CREATE INDEX IF NOT EXISTS idx_seen_events_seen_at ON seen_events(seen_at);
    `);
  }

  getCheckpoint(source: string, checkpointKey: string): string | undefined {
    const row = this.db
      .prepare('SELECT checkpoint_value FROM checkpoints WHERE source = ? AND checkpoint_key = ?')
      .get(source, checkpointKey);
    return row?.checkpoint_value;
  }

  setCheckpoint(source: string, checkpointKey: string, checkpointValue: string): void {
    this.db.prepare(`
      INSERT OR REPLACE INTO checkpoints (source, checkpoint_key, checkpoint_value, updated_at)
      VALUES (?, ?, ?, datetime('now'))
    `).run(source, checkpointKey, checkpointValue);
  }

  listCheckpoints(sourcePrefix: string): Record<string, Record<string, string>> {
    const rows = this.db
      .prepare(
        `
          SELECT source, checkpoint_key, checkpoint_value
          FROM checkpoints
          WHERE source LIKE ?
        `,
      )
      .all(`${sourcePrefix}%`);

    const result: Record<string, Record<string, string>> = {};
    for (const row of rows) {
      result[row.source] ??= {};
      result[row.source][row.checkpoint_key] = row.checkpoint_value;
    }
    return result;
  }

  hasSeen(source: string, eventId: string): boolean {
    const row = this.db
      .prepare("SELECT 1 as present FROM seen_events WHERE source = ? AND event_id = ?")
      .get(source, eventId);
    return row?.present === 1;
  }

  markSeen(source: string, eventId: string): void {
    this.db
      .prepare(
        `
          INSERT OR IGNORE INTO seen_events (source, event_id, seen_at)
          VALUES (?, ?, datetime('now'))
        `,
      )
      .run(source, eventId);
  }
}
```

### RSS Adapter Example

```typescript
import Parser from 'rss-parser';

class RSSAdapter implements SourceAdapter {
  name = 'rss';
  pollIntervalMs = 5 * 60 * 1000; // 5 minutes

  constructor(
    private feedUrls: string[],
    private checkpoints: CheckpointStore,
  ) {}

  async getCheckpoint(): Promise<Record<string, string>> {
    return this.checkpoints.getAll('rss.');
  }

  async *fetch(): AsyncIterable<{ event: RawEvent; checkpoint: Record<string, string> }> {
    const parser = new Parser();

    for (const feedUrl of this.feedUrls) {
      const feedId = hashUrl(feedUrl);
      const lastGuid = this.checkpoints.get(`rss.${feedId}.last_guid`);

      const feed = await parser.parseURL(feedUrl);
      const newItems = getItemsAfter(feed.items, lastGuid);

      for (const item of newItems) {
        const event: RawEvent = {
          event_id: `rss:${hashUrl(item.link ?? item.guid)}`,
          source: 'RSS',
          fetched_at: new Date().toISOString(),
          published_at: item.pubDate,
          url: item.link,
          title: item.title,
          text: item.contentSnippet ?? item.content ?? '',
          // ... other fields
        };

        yield {
          event,
          checkpoint: { [`rss.${feedId}.last_guid`]: item.guid },
        };
      }
    }
  }

  async saveCheckpoint(checkpoint: Record<string, string>): Promise<void> {
    for (const [key, value] of Object.entries(checkpoint)) {
      this.checkpoints.set(key, value);
    }
  }
}
```

### Backoff Manager

```typescript
class BackoffManager {
  private attempts = 0;
  private readonly maxDelayMs = 15 * 60 * 1000; // 15 minutes

  constructor(private name: string) {}

  reset(): void {
    this.attempts = 0;
  }

  async waitRateLimit(): Promise<void> {
    const baseDelay = 30_000; // 30 seconds
    await this.wait(baseDelay);
  }

  async waitTransient(): Promise<void> {
    const baseDelay = 5_000; // 5 seconds
    await this.wait(baseDelay);
  }

  private async wait(baseDelay: number): Promise<void> {
    const delay = Math.min(
      baseDelay * Math.pow(2, this.attempts),
      this.maxDelayMs
    );
    const jitter = delay * 0.2 * Math.random();
    const total = delay + jitter;

    log.info({ adapter: this.name, delayMs: total, attempt: this.attempts }, 'Backing off');
    await sleep(total);
    this.attempts++;
  }
}
```

## Testing Strategy

### Unit Tests

- Normalizer: various input formats → RawEvent
- Checkpoint store: CRUD operations
- Backoff: timing and jitter
- Validation: schema edge cases

### Integration Tests

- Full adapter cycle with mock HTTP responses
- Kafka producer verification
- Checkpoint persistence round-trip

### Acceptance Tests

- Soak test: 24h continuous run, verify no crashes
- Restart test: kill mid-batch, verify checkpoint recovery
- Rate limit test: simulate 429s, verify backoff behavior

## Metrics

- `collector_events_published_total{source=...}`
- `collector_events_dlq_total{source=...}`
- `collector_fetch_duration_seconds{source=...}`
- `collector_backoff_total{source=...,reason=...}`
- `collector_checkpoint_lag_seconds{source=...}`
