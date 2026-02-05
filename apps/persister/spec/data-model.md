# Data Model: Persister Service

## Input: RawEvent (from Kafka)

The persister consumes `RawEvent` messages from `events.raw`. See `packages/shared/contracts/proto/rising_intelligence/v1/contracts.proto` for the canonical schema.

## Output: Postgres `raw_events`

The persister writes to the `raw_events` table. See `packages/db/prisma/schema.prisma` for the Prisma model.

### Field Mapping

| RawEvent (Protobuf) | raw_events (Postgres) | Notes |
|---------------------|----------------------|-------|
| `event_id` | `eventId` | Unique constraint |
| `source` | `source` | Enum mapping |
| `fetched_at` | `fetchedAt` | ISO8601 → DateTime |
| `published_at` | `publishedAt` | Optional |
| `url` | `url` | Optional |
| `title` | `title` | Optional |
| `text` | `text` | Required |
| `author.id` | `authorId` | Flattened |
| `author.handle` | `authorHandle` | Flattened |
| `author.display_name` | `authorDisplayName` | Flattened |
| `engagement.score` | `engagementScore` | Flattened |
| `engagement.comments` | `engagementComments` | Flattened |
| `engagement.likes` | `engagementLikes` | Flattened |
| `engagement.shares` | `engagementShares` | Flattened |
| `lang` | `lang` | Optional |
| `tags` | `tags` | Array |
| `extracted.hashtags` | `extractedHashtags` | Array |
| `extracted.urls` | `extractedUrls` | Array |
| `source_meta_json` | `sourceMeta` | JSON string → JSONB |
| (not in input) | `topics` | Empty array; populated by Trends |

### Source Enum Mapping

| Protobuf | Prisma |
|----------|--------|
| `SOURCE_RSS` | `RSS` |
| `SOURCE_NEWS` | `NEWS` |
| `SOURCE_HACKERNEWS` | `HACKERNEWS` |
| `SOURCE_REDDIT` | `REDDIT` |
| `SOURCE_GITHUB` | `GITHUB` |
| `SOURCE_TWITTER` | `TWITTER` |

## Output: Redis `seen:*`

The persister sets a key in Redis to mark events as "seen" for deduplication.

### Key Format

```
seen:{source}:{event_id}
```

Examples:
- `seen:RSS:rss_abc123`
- `seen:HACKERNEWS:hn_39876543`
- `seen:REDDIT:t3_xyz789`

### Value

Always `"1"` (presence is what matters).

### TTL

24 hours (86400 seconds). After TTL, the key is automatically deleted by Redis.
