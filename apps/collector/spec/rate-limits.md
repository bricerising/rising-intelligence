# Rate Limits: Collector Service

## Overview

This document specifies the rate limits for each data source and the backoff strategies the Collector MUST implement.

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
| `POLL_INTERVAL_REDDIT_SECONDS` | 120 | Reddit poll interval |
| `POLL_INTERVAL_HN_SECONDS` | 60 | Hacker News poll interval |
| `POLL_INTERVAL_GITHUB_SECONDS` | 300 | GitHub poll interval |
| `POLL_INTERVAL_RSS_SECONDS` | 300 | RSS feed poll interval |
| `BACKOFF_MAX_RETRIES` | 5 | Max retries before circuit opens |
| `BACKOFF_BASE_MS` | 1000 | Base backoff duration |
