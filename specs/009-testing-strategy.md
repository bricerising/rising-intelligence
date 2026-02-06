# Spec 009: Testing Strategy

**Created**: 2026-02-05
**Updated**: 2026-02-05
**Status**: Proposed

## Overview

This spec defines the testing strategy for Rising Intelligence. Tests are built **alongside implementation**, not as an afterthought. Every service implementation PR must include corresponding tests.

## Principles

1. **Test while building**: Write tests as you implement, not after
2. **Test the contract, not the implementation**: Focus on inputs/outputs, not internals
3. **Integration over unit**: Prefer tests that verify component interaction
4. **Fast feedback**: Tests must run in < 30 seconds for local dev
5. **Deterministic**: No flaky tests - mock time, randomness, and external APIs

## Test Categories

### Level 1: Unit Tests

Per-function tests for pure logic. Run without any infrastructure.

| Area | What to Test | Example |
|------|--------------|---------|
| URL normalization | Edge cases, tracking params | `normalizeUrl('https://x.com/a?utm_source=foo')` → `'https://x.com/a'` |
| Topic extraction | Allowlist matching | `extractTopics('AWS Bedrock is great')` → `['aws.bedrock']` |
| Scoring algorithm | Math correctness | `computeScore({ volume: 10, prev: 5 })` → expected score |
| Baseline calculation | Median, fallback logic | `calculateBaseline([1,2,3,4,5])` → `3` |

**Location**: `apps/<service>/src/**/*.test.ts`

**Run**: `npm test -- --filter=unit`

### Level 2: Integration Tests

Test service logic with real Redis/Postgres but mocked Kafka and external APIs.

| Service | What to Test |
|---------|--------------|
| Collector | Adapters parse real RSS/JSON correctly |
| Persister | Events written to Postgres, dedup works |
| Trends | Window counting, snapshot generation, story dedup |
| Brief | Prompt building, output parsing, budget tracking |

**Infrastructure**: Docker Compose test profile with Redis + Postgres only.

**Location**: `apps/<service>/tests/integration/`

**Run**: `npm test -- --filter=integration`

### Level 3: End-to-End Tests

Full pipeline tests with all infrastructure. Verify data flows correctly from ingestion to brief.

| Scenario | What to Verify |
|----------|----------------|
| Happy path | Event → Kafka → Persister → Postgres ✓ |
| Trend detection | Spike in topic → appears in snapshot ✓ |
| Brief generation | SummaryRequest → Brief with citations ✓ |
| Consumer lag | Lag tracked correctly in Postgres ✓ |

**Infrastructure**: Full Docker Compose stack.

**Location**: `tests/e2e/`

**Run**: `npm run test:e2e`

## Test Harness Architecture

### Mock Sources

Instead of calling real APIs, tests use mock source adapters that return deterministic data:

```typescript
// packages/shared/testing/mock-sources.ts

export interface MockSourceConfig {
  source: Source;
  events: RawEvent[];
  delay_ms?: number;  // Simulate network latency
  fail_after?: number;  // Simulate failures
}

export class MockSourceAdapter implements SourceAdapter {
  private config: MockSourceConfig;
  private callCount = 0;

  async fetch(): Promise<RawEvent[]> {
    this.callCount++;
    if (this.config.fail_after && this.callCount > this.config.fail_after) {
      throw new Error('Simulated failure');
    }
    if (this.config.delay_ms) {
      await sleep(this.config.delay_ms);
    }
    return this.config.events;
  }
}
```

### Mock LLM

Brief service tests use a mock LLM that returns deterministic responses:

```typescript
// packages/shared/testing/mock-llm.ts

export class MockLLM {
  private responses: Map<string, string> = new Map();

  setResponse(promptPattern: RegExp, response: string): void {
    this.responses.set(promptPattern.source, response);
  }

  async complete(prompt: string): Promise<LLMResponse> {
    for (const [pattern, response] of this.responses) {
      if (new RegExp(pattern).test(prompt)) {
        return {
          content: response,
          usage: { prompt_tokens: 100, completion_tokens: 50 },
        };
      }
    }
    throw new Error(`No mock response for prompt: ${prompt.slice(0, 100)}...`);
  }
}
```

### Test Fixtures

Reusable test data for consistent testing:

```typescript
// packages/shared/testing/fixtures.ts

export const fixtures = {
  events: {
    awsBedrock: {
      event_id: 'test-event-1',
      source: Source.RSS,
      fetched_at: '2026-02-05T10:00:00Z',
      url: 'https://aws.amazon.com/blogs/aws/bedrock-update',
      title: 'AWS Bedrock Gets New Models',
      text: 'Today we announce new foundation models in Amazon Bedrock...',
      tags: ['aws.bedrock', 'ai.llm'],
    },
    redditDiscussion: {
      event_id: 'test-event-2',
      source: Source.REDDIT,
      fetched_at: '2026-02-05T10:05:00Z',
      url: 'https://reddit.com/r/aws/comments/abc123',
      title: 'Bedrock pricing changes?',
      text: 'Has anyone noticed the new Bedrock pricing...',
      tags: ['aws.bedrock'],
    },
    // Same story, different source (for dedup testing)
    hnDiscussion: {
      event_id: 'test-event-3',
      source: Source.HACKERNEWS,
      fetched_at: '2026-02-05T10:10:00Z',
      url: 'https://aws.amazon.com/blogs/aws/bedrock-update',  // Same URL as RSS
      title: 'AWS Bedrock Gets New Models',
      text: 'Discussion on HN about the new models...',
      tags: ['aws.bedrock', 'ai.llm'],
    },
  },

  snapshots: {
    trending: {
      generated_at: '2026-02-05T11:00:00Z',
      window: TrendWindow.TREND_WINDOW_60M,
      topics: [
        { topic: 'aws.bedrock', volume: 15, acceleration: 2.5, score: 8.5 },
        { topic: 'ai.llm', volume: 10, acceleration: 1.2, score: 6.0 },
      ],
    },
  },

  briefs: {
    daily: {
      brief_id: 'test-brief-1',
      generated_at: '2026-02-05T01:00:00Z',
      window: BriefWindow.BRIEF_WINDOW_DAILY,
      title: 'Tech Brief: Feb 5, 2026',
      highlights: [
        {
          topic: 'aws.bedrock',
          what_happened: 'AWS announced new models [1]',
          why_it_matters: 'Better performance for production workloads',
          suggested_action: 'Review the pricing changes',
          citations: ['https://aws.amazon.com/blogs/aws/bedrock-update'],
        },
      ],
    },
  },
};
```

### Time Control

Tests control time to ensure deterministic window alignment:

```typescript
// packages/shared/testing/time.ts

export class TestClock {
  private frozen: Date | null = null;

  freeze(time: Date | string): void {
    this.frozen = typeof time === 'string' ? new Date(time) : time;
  }

  unfreeze(): void {
    this.frozen = null;
  }

  now(): Date {
    return this.frozen ?? new Date();
  }

  advance(ms: number): void {
    if (!this.frozen) throw new Error('Clock not frozen');
    this.frozen = new Date(this.frozen.getTime() + ms);
  }
}

// Usage in tests
const clock = new TestClock();
clock.freeze('2026-02-05T10:00:00Z');
// ... run test ...
clock.advance(60 * 60 * 1000); // Advance 1 hour
// ... verify window closed ...
```

## Test Infrastructure

### Docker Compose Test Profile

```yaml
# docker-compose.test.yml
services:
  redis-test:
    image: redis:7-alpine
    ports:
      - "127.0.0.1:6380:6379"

  postgres-test:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: test
      POSTGRES_PASSWORD: test
      POSTGRES_DB: rising_intelligence_test
    ports:
      - "127.0.0.1:5433:5432"

  # Redpanda for e2e tests only
  redpanda-test:
    profiles: ["e2e"]
    image: redpandadata/redpanda:v24.3.5
    command: ["redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "512M"]
    ports:
      - "127.0.0.1:9093:9092"
```

### Test Setup/Teardown

```typescript
// packages/shared/testing/setup.ts

export async function setupTestInfra(): Promise<TestContext> {
  const redis = new Redis({ port: 6380 });
  const prisma = new PrismaClient({
    datasources: { db: { url: 'postgresql://test:test@localhost:5433/rising_intelligence_test' } },
  });

  // Clean state
  await redis.flushall();
  await prisma.$executeRaw`TRUNCATE raw_events, trend_snapshots, brief_results, consumer_lag CASCADE`;

  return { redis, prisma };
}

export async function teardownTestInfra(ctx: TestContext): Promise<void> {
  await ctx.redis.quit();
  await ctx.prisma.$disconnect();
}
```

## Test Requirements Per Service

### Collector

| Test | Type | Must Pass Before Merge |
|------|------|------------------------|
| RSS adapter parses valid feed | Unit | ✅ |
| HN adapter handles pagination | Unit | ✅ |
| Reddit adapter respects rate limits | Unit | ✅ |
| URL normalization edge cases | Unit | ✅ |
| Topic extraction matches allowlist | Unit | ✅ |
| Checkpoint persistence | Integration | ✅ |
| Graceful shutdown | Integration | ✅ |

### Persister

| Test | Type | Must Pass Before Merge |
|------|------|------------------------|
| Events written to Postgres | Integration | ✅ |
| Duplicate events not re-inserted | Integration | ✅ |
| Batch insert performance | Integration | Optional |

### Trends

| Test | Type | Must Pass Before Merge |
|------|------|------------------------|
| Window bucket alignment | Unit | ✅ |
| Story dedup by URL | Unit | ✅ |
| Scoring formula | Unit | ✅ |
| Baseline calculation | Unit | ✅ |
| Snapshot generation | Integration | ✅ |
| Consumer lag tracking | Integration | ✅ |
| Brief trigger on schedule | Integration | ✅ |
| Data freshness check blocks brief | Integration | ✅ |

### Brief

| Test | Type | Must Pass Before Merge |
|------|------|------------------------|
| Prompt building from SummaryRequest | Unit | ✅ |
| Output parsing with Zod | Unit | ✅ |
| Budget tracking | Unit | ✅ |
| Idempotency (duplicate requests) | Integration | ✅ |
| Context truncation | Integration | ✅ |

### End-to-End

| Test | Type | Must Pass Before Merge |
|------|------|------------------------|
| Event flows through full pipeline | E2E | ✅ |
| Spike detection triggers snapshot | E2E | ✅ |
| Daily brief generated on schedule | E2E | ✅ |
| Brief contains valid citations | E2E | ✅ |

## CI/CD Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - run: npm ci
      - run: npm test -- --filter=unit

  integration:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:7-alpine
        ports: ["6379:6379"]
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
          POSTGRES_DB: rising_intelligence_test
        ports: ["5432:5432"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
      - run: npm ci
      - run: npx prisma migrate deploy
      - run: npm test -- --filter=integration

  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
      - run: docker compose -f docker-compose.test.yml --profile e2e up -d
      - run: npm ci
      - run: npm run test:e2e
      - run: docker compose -f docker-compose.test.yml down
```

## Coverage Requirements

| Category | Minimum Coverage |
|----------|-----------------|
| Unit tests | 80% line coverage |
| Integration tests | Key paths covered |
| E2E tests | Happy path + 1 failure mode |

Coverage is tracked but not enforced as a gate. Use judgment - 100% coverage of trivial code is less valuable than 60% coverage of critical paths.

## Testing Checklist for PRs

Before merging any service implementation:

- [ ] Unit tests for all pure functions
- [ ] Integration tests for storage operations
- [ ] Mock sources/LLM configured and working
- [ ] Time-sensitive tests use frozen clock
- [ ] No external API calls in tests
- [ ] Tests run in < 30 seconds
- [ ] CI passes

## Debugging Failed Tests

When tests fail:

1. **Check test isolation**: Each test must clean up after itself
2. **Check time assumptions**: Are you assuming a specific time? Use TestClock
3. **Check random data**: Are you using Math.random()? Seed it deterministically
4. **Check async timing**: Are you awaiting all promises? Use proper async handling
5. **Check Docker state**: Is test infrastructure running? `docker compose -f docker-compose.test.yml ps`
