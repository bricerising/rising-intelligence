# Rate Limits: Collector Service

## Overview

This document specifies the rate limits for each data source and the backoff strategies the Collector MUST implement.

## Design Philosophy: Relaxed Ingestion

**Key insight**: For trend detection, we don't need real-time data. Hourly catch-up is sufficient.

**Principles**:
1. **No aggressive catch-up**: If the Collector was down for 2 hours, there's no need to fetch everything immediately. Spread it over the next hour.
2. **Throttled fetching**: Fetch a bounded batch per poll cycle, not "everything since last check."
3. **Steady state over bursts**: Prefer consistent, predictable load over burst-then-idle patterns.
4. **Rate limits are friends**: Staying well under limits means fewer errors and simpler code.

**Implications**:
- Poll intervals are minimums, not targets
- Batch sizes are capped even if more data is available
- Missing some posts during high-volume periods is acceptable
- Freshness target: events available within ~60 minutes, not ~60 seconds

## Rate Limits by Source

### Reddit API

**Official limits** (with OAuth):
- 60 requests per minute (per OAuth client)
- 1000 requests per day for free tier

**Implementation**:
- Poll interval: minimum 60 seconds between requests per subreddit
- Respect `X-Ratelimit-Remaining` and `X-Ratelimit-Reset` headers
- Backoff: On 429, wait for `Retry-After` header value (or 60s default)

**Relevant headers**:
```
X-Ratelimit-Remaining: 59.0
X-Ratelimit-Reset: 45
X-Ratelimit-Used: 1
```

#### Reddit Budget Math (Relaxed Approach)

**Guiding principle**: We only need hourly freshness, not real-time. This dramatically reduces API pressure.

**Request cost per subreddit per poll cycle**:
| Operation | Requests | Notes |
|-----------|----------|-------|
| List new posts | 1 | `/r/{sub}/new.json?limit=25` (capped) |
| **Total per sub** | **1** | No hot posts needed for trend detection |

**Relaxed polling with 6 subreddits (comfortable MVP)**:
| Metric | Calculation | Result |
|--------|-------------|--------|
| Poll interval | 10 min (600s) | Plenty fresh for hourly trends |
| Polls per hour | 6 | |
| Requests per poll | 6 subs × 1 req | 6 req/poll |
| Requests per hour | 6 × 6 | 36 req/hr |
| Requests per day | 36 × 24 | **864 req/day** |

**✅ Well under 1,000 daily limit** with room for retries and growth.

**Simplified approach** (no priority tiers needed):
| Subreddits | Poll Interval | Requests/Day | Headroom |
|------------|---------------|--------------|----------|
| 4 | 10 min | 576 | 42% |
| 6 | 10 min | 864 | 14% |
| 8 | 15 min | 768 | 23% |
| 10 | 15 min | 960 | 4% |

**Recommendation**: 6 subreddits @ 10 min interval is the sweet spot.

#### Subreddit Configuration (Simplified)

With relaxed polling, priority tiers add unnecessary complexity. Just poll all configured subreddits equally:

```yaml
reddit:
  poll_interval_seconds: 600  # 10 minutes
  posts_per_poll: 25          # Cap per subreddit (don't fetch everything)
  subreddits:
    - r/aws
    - r/MachineLearning
    - r/devops
    - r/programming
    - r/LocalLLaMA
    - r/typescript
```

**Why no tiers?**
- With 10-min intervals, we're well under rate limits
- All subreddits get equal coverage
- Simpler code, fewer edge cases
- If you don't care about a subreddit enough to poll it equally, remove it

#### Budget Exhaustion Handling

Simple approach - just pause and wait:

```typescript
function shouldPollReddit(remaining: number, resetTimestamp: number): boolean {
  if (remaining <= 5) {
    const waitSeconds = resetTimestamp - Date.now() / 1000;
    log.warn(`Reddit budget low (${remaining}), pausing for ${waitSeconds}s`);
    return false;
  }
  return true;
}
```

**Metrics**:
- `ri_collector_reddit_budget_remaining`: Gauge of remaining daily requests
- `ri_collector_reddit_paused_total`: Counter of paused poll cycles

### Hacker News API (Firebase)

**Limits**:
- No official rate limit documented
- Recommended: max 1 request per second
- Large batch fetches should be spread over time

**Implementation**:
- Poll interval: minimum 60 seconds
- Max items per poll: 100 (top/new stories)
- Item detail fetches: stagger 100ms apart
- Backoff: On error, exponential backoff starting at 5s

### GitHub API

**Limits** (with token):
- 5000 requests per hour
- Search API: 30 requests per minute

**Implementation**:
- Poll interval: minimum 300 seconds (5 minutes) for releases
- Respect `X-RateLimit-Remaining` and `X-RateLimit-Reset` headers
- Backoff: On 403 rate limit, wait until reset time

**Relevant headers**:
```
X-RateLimit-Limit: 5000
X-RateLimit-Remaining: 4999
X-RateLimit-Reset: 1707177600
```

### RSS/Atom Feeds

**Limits**:
- No standard limits (varies by feed provider)
- Best practice: respect `Cache-Control` and `Last-Modified` headers

**Implementation**:
- Poll interval: minimum 300 seconds (5 minutes)
- Use conditional requests (`If-Modified-Since`, `If-None-Match`)
- Backoff: On 5xx errors, exponential backoff starting at 30s

### NewsAPI (if used)

**Limits** (free tier):
- 100 requests per day
- 50 results per request

**Implementation**:
- Poll interval: minimum 900 seconds (15 minutes)
- Use `pageSize=50` to maximize results per request
- Track daily usage to avoid exceeding quota

### Bluesky (AT Protocol)

**Limits**:
- Public API: No documented rate limits for reads (be reasonable)
- Firehose (Jetstream): Unlimited real-time streaming
- Authenticated: 3000 requests per 5 minutes (very generous)

**Implementation**:
- Poll interval: minimum 60 seconds for search/feed queries
- Prefer Jetstream firehose for real-time: `wss://jetstream.atproto.com/subscribe`
- Filter by hashtags: `#aws`, `#ai`, `#typescript`, etc.
- No backoff typically needed; implement standard exponential backoff for 5xx

**Relevant headers**:
```
RateLimit-Limit: 3000
RateLimit-Remaining: 2999
RateLimit-Reset: 1707177600
```

### Mastodon (ActivityPub)

**Limits** (varies by instance):
- Typical: 300 requests per 5 minutes per IP
- Some instances are more restrictive (100/5min)

**Implementation**:
- Poll interval: minimum 120 seconds per instance
- Use public timeline endpoint: `/api/v1/timelines/public`
- Prefer instances with tech focus:
  - `hachyderm.io` (tech professionals)
  - `fosstodon.org` (FOSS community)
  - `infosec.exchange` (security)
- Backoff: On 429, respect `X-RateLimit-Reset` header

**Relevant headers**:
```
X-RateLimit-Limit: 300
X-RateLimit-Remaining: 299
X-RateLimit-Reset: 2024-02-06T12:00:00.000Z
```

**Multi-instance strategy**:
- Track rate limits per instance separately
- Round-robin across instances when one is exhausted
- Consider running your own relay for aggregated access

## Backoff Strategy

All sources use the same exponential backoff with jitter:

```typescript
function calculateBackoff(attempt: number, baseMs: number = 1000): number {
  const exponential = Math.min(baseMs * Math.pow(2, attempt), 300_000); // max 5 min
  const jitter = exponential * 0.2 * Math.random(); // ±20% jitter
  return exponential + jitter;
}
```

**Backoff triggers**:
| HTTP Status | Action |
|-------------|--------|
| 429 | Wait for `Retry-After` or backoff |
| 5xx | Exponential backoff |
| Network error | Exponential backoff |
| 401/403 | Log error, pause source (may need credential refresh) |

**Circuit breaker**:
- Open after 5 consecutive failures
- Half-open after 60 seconds
- Close after 3 consecutive successes

## Metrics

- `ri_collector_rate_limit_backoff_total{source}`: Backoff events
- `ri_collector_rate_limit_remaining{source}`: Remaining requests (gauge)
- `ri_collector_poll_duration_seconds{source}`: Time per poll cycle
- `ri_collector_circuit_state{source}`: 0=closed, 0.5=half-open, 1=open

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `REDDIT_POLL_INTERVAL_SECONDS` | 300 | Reddit poll interval (5 min) |
| `HN_POLL_INTERVAL_SECONDS` | 300 | Hacker News poll interval (5 min) |
| `RSS_POLL_INTERVAL_SECONDS` | 300 | RSS feed poll interval (5 min) |
| `LOBSTERS_POLL_INTERVAL_SECONDS` | 600 | Lobsters poll interval (10 min) |
| `BLUESKY_POLL_INTERVAL_SECONDS` | 300 | Bluesky poll interval (5 min) |
| `MASTODON_POLL_INTERVAL_SECONDS` | 600 | Mastodon poll interval (10 min) |
| `GITHUB_POLL_INTERVAL_SECONDS` | 3600 | GitHub poll interval (60 min) |
| `REDDIT_MAX_ITEMS_PER_POLL` | 25 | Max posts per poll |
| `HN_MAX_ITEMS_PER_POLL` | 30 | Max stories per poll |
| `LOBSTERS_MAX_ITEMS_PER_POLL` | 25 | Max stories per poll |
| `BLUESKY_QUERIES` | "aws,bedrock,ai,llm,typescript,rust" | Comma-separated queries to track |
| `MASTODON_INSTANCES` | "hachyderm.io,fosstodon.org" | Comma-separated instances to poll |

## Catch-Up Behavior

**On restart after downtime**:
- Do NOT attempt to fetch all missed content immediately
- Resume normal polling from current time
- Accept that some posts during downtime may be missed
- Checkpoint stores "last seen" but doesn't trigger backfill

**Rationale**: For trend detection, a gap of a few hours is acceptable. Aggressive catch-up:
- Risks rate limit exhaustion
- Adds complexity (pagination, dedup across batches)
- Provides marginal value (old posts don't affect current trends much)

**Implementation**:
```typescript
async function pollSource(source: Source): Promise<void> {
  // Always fetch "recent" items, not "since last checkpoint"
  const items = await fetchRecent(source, BATCH_SIZE[source]);

  // Dedup against seen cache (handles overlap between polls)
  const newItems = items.filter(item => !seen.has(item.id));

  // Publish and checkpoint
  await publishToKafka(newItems);
  await updateCheckpoint(source, items[0]?.id);
}
```
