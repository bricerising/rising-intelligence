# Data Model: Collector Service

## Overview

Collector is primarily a stateless transformer from "source items" → `RawEvent`.

## Key concepts

- **Source item**: the raw record returned by an upstream API/feed.
- **Cursor/checkpoint**: per-source state used to resume polling without excessive duplicates.

## Contracts

- Canonical output: `RawEvent` (defined in `specs/001-real-time-personal-intelligence-system.md`).
- DLQ payload shape is implementation-defined but MUST include:
  - `source`, `fetched_at`
  - error type/code
  - a redacted sample of the raw payload or URL reference

### Phase-1 source-pack metadata additions

For phase-1 POS/public feeds, collector enriches `RawEvent.source_meta` with classification and audit fields.

Expected fields (when available):

- `source_type` (for example `edgar`, `policy`, `security`, `wire`, `merchant`)
- `signal_tier` (`high_volume` or `low_volume`)
- `market_profiles` (`string[]`)
- `match_reasons` (`string[]`; includes profile matcher hints and `entity:<term>` for high-volume strict-gate matches)
- `feed_name`
- `feed_url`

EDGAR-specific metadata fields:

- `cik`
- `form_type`
- `accession_number`
- `filed_date`
- `accepted_at`
- `filing_detail_url`
- `primary_document_name` (detail metadata only, no document download)

### Tags vs market profiles

- `RawEvent.tags` continues to store canonical topic tags.
- Market classification tags are also added as `market.<profile>` keys.
- `source_meta.market_profiles` remains the audit source of truth for why an item passed ingest filters.

## Checkpoint storage (MVP)

MVP decision:

- Use **local SQLite** (`/data/checkpoints.db`) for checkpoint persistence.
- This keeps Collector fully decoupled from Redis (no external dependencies except Kafka).

### Why SQLite over Redis?

- **Service isolation**: Collector has zero database dependencies, making it simpler to reason about and test.
- **Persistence by default**: SQLite file is persisted via Docker volume mount.
- **Crash recovery**: On restart, Collector reads last checkpoint from SQLite and resumes.

### SQLite schema

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
  source TEXT NOT NULL,           -- e.g., 'rss.aws_blog', 'reddit.r_aws', 'hackernews'
  checkpoint_key TEXT NOT NULL,   -- e.g., 'last_guid', 'after_cursor', 'last_max_id'
  checkpoint_value TEXT NOT NULL, -- the cursor value
  updated_at TEXT NOT NULL,       -- ISO8601 timestamp
  PRIMARY KEY (source, checkpoint_key)
);

CREATE TABLE IF NOT EXISTS seen_events (
  source TEXT NOT NULL,
  event_id TEXT NOT NULL,
  seen_at TEXT NOT NULL,          -- ISO8601 timestamp
  PRIMARY KEY (source, event_id)
);

-- Cleanup old seen_events periodically (TTL ~7 days)
CREATE INDEX IF NOT EXISTS idx_seen_events_seen_at ON seen_events(seen_at);
```

### Example checkpoints

| source | checkpoint_key | checkpoint_value |
|--------|----------------|------------------|
| `rss.aws_blog` | `last_guid` | `https://aws.amazon.com/blogs/aws/...` |
| `reddit.r_aws` | `after_cursor` | `t3_abc123` |
| `hackernews` | `last_max_id` | `39876543` |

### Dedupe cache

To bound duplicates across restarts, the collector maintains the `seen_events` table:

- Insert `(source, event_id, now())` after successful Kafka publish
- Before emitting, check if `(source, event_id)` exists
- Periodically delete rows older than 7 days

### Docker volume mount

```yaml
collector:
  volumes:
    - collector-checkpoints:/data
```

This ensures checkpoints survive container restarts.

## Restart & Catch-Up Behavior

**Design decision**: No aggressive catch-up after downtime.

On restart:
1. Load last checkpoint from SQLite (for dedup, not backfill)
2. Resume normal polling cadence from current time
3. Fetch only recent items (bounded batch size)
4. Dedupe against `seen_events` cache

**What this means**:
- If Collector was down for 3 hours, we do NOT fetch 3 hours of missed posts
- We resume at normal pace (e.g., 25 posts per subreddit every 10 min)
- Some posts during the gap will be missed
- This is acceptable for trend detection (we care about patterns, not completeness)

**Why no backfill?**
- Rate limits: Backfilling would exhaust API quotas
- Complexity: Pagination, dedup across large result sets
- Diminishing value: Old posts don't affect current trend calculations
- YAGNI: If we really need historical data, we can import from archives

**Checkpoint purpose**:
- The checkpoint stores the last-seen ID for **deduplication**, not for "resume from here"
- It prevents re-emitting the same post if it's still in the API's "recent" results
- It does NOT trigger "fetch everything since this ID"

```typescript
// CORRECT: Fetch recent, dedup against checkpoint
const recent = await api.getRecent({ limit: 25 });
const unseen = recent.filter(p => !seenCache.has(p.id));

// WRONG: Fetch since checkpoint (don't do this)
// const since = await api.getSince({ after: checkpoint.lastId });
```
