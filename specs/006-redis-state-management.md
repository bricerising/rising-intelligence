# Spec 006: Redis State Management

**Created**: 2026-02-05
**Status**: Proposed

## Overview

Redis provides **ephemeral state** for:

1. **Window aggregation state**: In-flight counters for trend computation
2. **Deduplication cache**: Short-term seen-event tracking
3. **Rate limit tracking**: Per-source request budgets

Redis is **not** the source of truth for any data. All durable state lives in Postgres. If Redis is lost, services recover by:
- Re-reading from Kafka (for window state)
- Accepting some duplicate processing (bounded by Kafka retention)

## Why Redis?

- **Speed**: Sub-millisecond reads/writes for hot-path operations
- **TTL support**: Automatic expiration for dedup cache and rate limits
- **Atomic operations**: INCR, EXPIRE, ZADD for counters and sorted sets
- **Memory-bounded**: LRU eviction prevents unbounded growth

## Key Patterns

### 1. Window Aggregation State (Trends Service)

Maintains rolling counts for topic volumes across time windows.

**Key pattern**: `window:{window}:{topic}:{bucket}`

```
window:15m:aws.bedrock:2026-02-05T14:15:00Z = 42
window:60m:aws.bedrock:2026-02-05T14:00:00Z = 187
```

**Operations**:
- `INCR` on each event processed
- `GET` to read current bucket
- `TTL` = window size × 3 (e.g., 15m window → 45m TTL)

**Bucket alignment**: Buckets align to clock time (e.g., 15m buckets at :00, :15, :30, :45).

**Recovery on restart**: Consumer replays from last committed Kafka offset. Counts may temporarily inflate but stabilize after one window period.

### 2. Previous Window Cache

Stores the previous window's counts for acceleration calculation.

**Key pattern**: `prev:{window}:{topic}`

```
prev:60m:aws.bedrock = 156
```

**TTL**: 2 × window size (e.g., 60m window → 120m TTL)

**Updated**: When a window closes, current count moves to `prev:*` before resetting.

### 3. Baseline Cache (Optional)

Caches 7-day baseline values computed from Postgres to avoid repeated queries.

**Key pattern**: `baseline:{window}:{topic}:{day_of_week}`

```
baseline:60m:aws.bedrock:wed = 145
```

**TTL**: 24 hours (recomputed daily from Postgres)

### 4. Deduplication Cache (Persister Service)

Marks events as "seen" after they've been persisted to Postgres. This allows other services to check if an event exists without querying Postgres.

**Key pattern**: `seen:{source}:{event_id}`

```
seen:REDDIT:t3_abc123 = 1
seen:HACKERNEWS:39876543 = 1
```

**Operations**:
- `SET key 1 EX 86400` after successful Postgres write
- TTL = 24 hours

**Writer**: Persister service (after Postgres write succeeds)

**Why Redis in addition to Postgres?**: The seen-check is faster than a Postgres query. Useful for downstream services that want quick duplicate detection without database round-trips.

**Cross-source deduplication** (future): For URL-based dedup across sources:

**Key pattern**: `seen:url:{normalized_url_hash}`

```
seen:url:sha256:abc123... = 1
```

### 5. Rate Limit Tracking (Collector Service)

Tracks remaining API budget per source to avoid hitting rate limits.

**Key pattern**: `ratelimit:{source}:{window_start}`

```
ratelimit:reddit:2026-02-05T14:00:00Z = 47  # requests remaining this minute
```

**Operations**:
- `DECR` on each request
- `GET` before request; skip if ≤ 0
- TTL = rate limit window + buffer (e.g., 70 seconds for per-minute limits)

**Alternative**: Use a token bucket (Redis sorted set) for smoother rate limiting.

### 6. Evidence Buffer (Trends Service)

Temporarily holds top evidence items per topic for inclusion in snapshots.

**Key pattern**: `evidence:{window}:{topic}` (sorted set)

```
ZADD evidence:60m:aws.bedrock 100 "event_id_1"
ZADD evidence:60m:aws.bedrock 85 "event_id_2"
```

**Score**: Engagement score or recency
**Operations**:
- `ZADD` on each event
- `ZREVRANGE ... LIMIT 0 10` to get top 10
- `ZREMRANGEBYRANK` to cap size
- TTL = window size × 2

### 7. Window Deduplication (Trends Service)

Tracks which event_ids have been counted in each window bucket to prevent duplicate events from inflating counts.

**Key pattern**: `dedup:{window}:{bucket}` (set)

```
SADD dedup:60m:2026-02-05T14:00:00Z "event_123"
SADD dedup:60m:2026-02-05T14:00:00Z "event_456"
```

**Operations**:
- `SADD` returns 1 if new, 0 if already exists
- Check return value before incrementing counters
- TTL = window size × 3 (e.g., 3h for 60m window)

**Why this matters**:
- Kafka delivers at-least-once (duplicates expected)
- Consumer restarts replay from last committed offset
- Without dedup, replayed events inflate trend scores
- SADD is O(1), minimal overhead per event

## Redis Configuration

From `docker-compose.yml`:

```yaml
redis:
  command:
    - redis-server
    - --save ""                    # Disable RDB snapshots (state is ephemeral)
    - --appendonly yes             # Enable AOF for crash recovery
    - --appendfsync everysec       # Sync every second (good balance)
    - --maxmemory 256mb            # Bounded memory
    - --maxmemory-policy allkeys-lru  # Evict least-recently-used keys
```

**Why AOF with ephemeral state?**: AOF provides fast recovery after restart without full Kafka replay. It's a performance optimization, not a durability guarantee.

## Key Naming Conventions

- Use colons as separators: `namespace:subtype:identifier`
- Include window/bucket in time-sensitive keys
- Use ISO8601 for time components
- Keep keys short but readable

## Memory Estimation (MVP)

| Component | Keys | Size/Key | Total |
|-----------|------|----------|-------|
| Window counters | 20 topics × 3 windows × 4 buckets | ~100 bytes | ~24 KB |
| Previous window | 20 topics × 3 windows | ~100 bytes | ~6 KB |
| Dedup cache (Persister) | ~10K events/day × 24h TTL | ~50 bytes | ~12 MB |
| Window dedup (Trends) | ~3K events/window × 3 windows | ~50 bytes | ~450 KB |
| Rate limits | 6 sources × 60 keys | ~50 bytes | ~18 KB |
| Evidence buffers | 20 topics × 3 windows × 10 items | ~100 bytes | ~60 KB |
| Budget tracking | 7 days | ~100 bytes | ~700 bytes |

**Total estimate**: ~15 MB active, well under 256 MB limit.

## Service Responsibilities

| Service | Redis Usage |
|---------|-------------|
| Collector | None (uses local SQLite for checkpoints, in-memory for rate limits) |
| Persister | Dedup cache (`seen:*` keys) |
| Trends | Window counters, previous window, baseline cache, evidence buffers |
| Brief | Budget tracking (`budget:*` keys) |

**Note**: The Collector has no Redis dependency. This keeps ingestion simple and decoupled. The Persister handles the dedup cache as it materializes events from Kafka.

## Failure Modes

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Redis unavailable | Collector may emit duplicates; Trends windows reset | Services continue with degraded dedup; counts recover after one window |
| Redis data loss | Same as unavailable | Same recovery |
| Memory exhaustion | LRU eviction drops old keys | Acceptable; oldest data evicted first |

## Monitoring

Metrics to track:
- `redis_connected_clients`
- `redis_used_memory_bytes`
- `redis_evicted_keys_total`
- `redis_keyspace_hits_total` / `redis_keyspace_misses_total`

Alert on:
- Memory > 80% of maxmemory
- Eviction rate spike (may indicate undersized cache)
- Connection failures
