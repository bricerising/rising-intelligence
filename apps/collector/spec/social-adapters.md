# Social Adapters: Bluesky & Mastodon

**Created**: 2026-02-05
**Status**: Planned

## Overview

This spec covers the social media adapters that replace Twitter/X for social signal. Both platforms have open APIs that are viable for personal use without enterprise pricing.

## Bluesky (AT Protocol)

### Why Bluesky

- Free public API with generous rate limits
- Growing tech community adoption (especially AI/developer crowd)
- Real-time firehose available (Jetstream)
- No authentication required for public data reads

### Data Access Methods

**Option A: Polling (MVP)**
- Simpler implementation
- Lower resource usage
- 5-minute freshness is acceptable

**Option B: Firehose (Post-MVP)**
- Real-time streaming via Jetstream
- Higher complexity (WebSocket management)
- Better for high-volume tracking

### Polling Implementation

#### Endpoint: Search Posts

```typescript
interface BlueskySearchParams {
  q: string;           // Search query (hashtags, keywords)
  limit?: number;      // Max 100, default 25
  cursor?: string;     // Pagination cursor
  sort?: 'top' | 'latest';
}

// No auth required for public search
const response = await fetch(
  `https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q=${encodeURIComponent(query)}&limit=50`
);
```

#### Checkpoint Strategy

| Checkpoint Key | Value | Purpose |
|---------------|-------|---------|
| `bluesky.search.{query_hash}.cursor` | Pagination cursor | Resume pagination |
| `bluesky.search.{query_hash}.last_indexed_at` | ISO8601 timestamp | Skip older posts |

**Note**: Bluesky search doesn't guarantee chronological order. Use `indexedAt` field for deduplication.

#### Search Queries

Configure hashtags and keywords to track:

```yaml
bluesky:
  mode: polling  # or 'firehose'
  poll_interval_seconds: 300
  queries:
    - "#aws"
    - "#bedrock"
    - "#ai"
    - "#llm"
    - "#typescript"
    - "#rust"
    - "openai"
    - "anthropic"
    - "claude"
```

#### Response Mapping

```typescript
function mapBlueskyPost(post: BlueskyPost): RawEvent {
  return {
    event_id: `bluesky:${post.uri}`,  // AT URI is globally unique
    source: Source.BLUESKY,
    fetched_at: new Date().toISOString(),
    published_at: post.record.createdAt,
    url: `https://bsky.app/profile/${post.author.handle}/post/${post.uri.split('/').pop()}`,
    title: undefined,  // Bluesky posts don't have titles
    text: post.record.text,
    author: {
      id: post.author.did,
      handle: post.author.handle,
      display_name: post.author.displayName,
    },
    engagement: {
      likes: post.likeCount ?? 0,
      comments: post.replyCount ?? 0,
      shares: post.repostCount ?? 0,
      score: (post.likeCount ?? 0) + (post.repostCount ?? 0) * 2,
    },
    extracted: {
      hashtags: extractHashtags(post.record.text),
      urls: extractUrls(post.record.facets),
    },
    source_meta_json: JSON.stringify({
      uri: post.uri,
      cid: post.cid,
      labels: post.labels,
    }),
  };
}
```

### Firehose Implementation (Post-MVP)

Jetstream provides a real-time stream of all public Bluesky activity.

#### Connection

```typescript
const ws = new WebSocket('wss://jetstream1.us-east.bsky.network/subscribe');

ws.on('message', (data) => {
  const event = JSON.parse(data);

  // Filter for posts only (not likes, follows, etc.)
  if (event.kind === 'commit' && event.commit.collection === 'app.bsky.feed.post') {
    const post = event.commit.record;

    // Filter by content (check for tracked keywords/hashtags)
    if (matchesTrackedTopics(post.text)) {
      processPost(event);
    }
  }
});
```

#### Firehose Filtering

The firehose sends ALL posts. Filter client-side:

```typescript
function matchesTrackedTopics(text: string): boolean {
  const lowerText = text.toLowerCase();
  return TRACKED_KEYWORDS.some(kw => lowerText.includes(kw.toLowerCase()));
}
```

#### Checkpoint for Firehose

| Checkpoint Key | Value | Purpose |
|---------------|-------|---------|
| `bluesky.firehose.cursor` | Jetstream cursor (time_us) | Resume from last position |

On reconnect, pass cursor to resume:
```
wss://jetstream1.us-east.bsky.network/subscribe?cursor=1707177600000000
```

### Error Handling

| Error | Action |
|-------|--------|
| 429 Rate Limited | Exponential backoff (unlikely with public API) |
| 5xx Server Error | Exponential backoff, max 5 min |
| WebSocket Disconnect | Reconnect with last cursor |
| Invalid Response | Log to DLQ, continue |

### Metrics

- `ri_collector_bluesky_posts_fetched_total`
- `ri_collector_bluesky_poll_duration_seconds`
- `ri_collector_bluesky_firehose_events_total{matched, dropped}`
- `ri_collector_bluesky_firehose_reconnects_total`

---

## Mastodon (ActivityPub)

### Why Mastodon

- Federated network with strong tech communities
- Public timeline access without authentication
- Multiple instances provide redundancy and diversity

### Instance Selection

Target tech-focused instances for relevant signal:

| Instance | Focus | Typical Rate Limit |
|----------|-------|-------------------|
| `hachyderm.io` | Tech professionals | 300 req/5min |
| `fosstodon.org` | FOSS community | 300 req/5min |
| `infosec.exchange` | Security | 300 req/5min |
| `mastodon.social` | General (large) | 300 req/5min |

**Configuration**:

```yaml
mastodon:
  poll_interval_seconds: 600
  instances:
    - host: hachyderm.io
      enabled: true
    - host: fosstodon.org
      enabled: true
    - host: infosec.exchange
      enabled: true
  filter_tags:
    - aws
    - ai
    - machinelearning
    - typescript
    - rust
    - devops
```

### Polling Implementation

#### Endpoint: Public Timeline

```typescript
// No auth required for public timeline
const response = await fetch(
  `https://${instance}/api/v1/timelines/public?limit=40&local=false`,
  {
    headers: {
      'Accept': 'application/json',
    },
  }
);
```

#### Endpoint: Tag Timeline (Preferred)

More targeted than public timeline:

```typescript
const response = await fetch(
  `https://${instance}/api/v1/timelines/tag/${tag}?limit=40`,
  {
    headers: {
      'Accept': 'application/json',
    },
  }
);
```

#### Checkpoint Strategy

| Checkpoint Key | Value | Purpose |
|---------------|-------|---------|
| `mastodon.{instance}.{tag}.max_id` | Status ID | Pagination (older) |
| `mastodon.{instance}.{tag}.since_id` | Status ID | Fetch newer only |

Mastodon uses Link headers for pagination:
```
Link: <https://hachyderm.io/api/v1/timelines/tag/aws?max_id=123>; rel="next"
```

#### Response Mapping

```typescript
function mapMastodonStatus(status: MastodonStatus, instance: string): RawEvent {
  return {
    event_id: `mastodon:${instance}:${status.id}`,
    source: Source.MASTODON,
    fetched_at: new Date().toISOString(),
    published_at: status.created_at,
    url: status.url,
    title: undefined,  // Mastodon posts don't have titles
    text: stripHtml(status.content),  // Content is HTML
    author: {
      id: status.account.id,
      handle: `${status.account.acct}@${instance}`,
      display_name: status.account.display_name,
    },
    engagement: {
      likes: status.favourites_count,
      comments: status.replies_count,
      shares: status.reblogs_count,
      score: status.favourites_count + status.reblogs_count * 2,
    },
    lang: status.language,
    extracted: {
      hashtags: status.tags.map(t => t.name),
      urls: status.media_attachments
        .filter(m => m.type === 'link')
        .map(m => m.url),
    },
    source_meta_json: JSON.stringify({
      instance,
      visibility: status.visibility,
      sensitive: status.sensitive,
      spoiler_text: status.spoiler_text,
    }),
  };
}
```

#### HTML Stripping

Mastodon content is HTML. Strip tags for text analysis:

```typescript
function stripHtml(html: string): string {
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<p>/gi, '\n')
    .replace(/<\/p>/gi, '')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .trim();
}
```

### Multi-Instance Handling

Track rate limits per instance independently:

```typescript
interface InstanceState {
  host: string;
  rateLimitRemaining: number;
  rateLimitReset: Date;
  lastPollAt: Date;
  consecutiveErrors: number;
}

async function selectInstance(instances: InstanceState[]): Promise<InstanceState | null> {
  const now = new Date();

  // Find instance with available budget and not in backoff
  return instances.find(i =>
    i.rateLimitRemaining > 10 &&
    i.consecutiveErrors < 3 &&
    (now.getTime() - i.lastPollAt.getTime()) > POLL_INTERVAL_MS
  ) ?? null;
}
```

### Error Handling

| Error | Action |
|-------|--------|
| 429 Rate Limited | Respect `X-RateLimit-Reset`, skip instance this cycle |
| 5xx Server Error | Exponential backoff per instance |
| Instance Unreachable | Mark degraded, continue with others |
| Invalid Response | Log to DLQ, continue |

### Metrics

- `ri_collector_mastodon_posts_fetched_total{instance}`
- `ri_collector_mastodon_poll_duration_seconds{instance}`
- `ri_collector_mastodon_rate_limit_remaining{instance}`
- `ri_collector_mastodon_instance_errors_total{instance}`

---

## Cross-Source Deduplication

Both Bluesky and Mastodon users often cross-post. Handle at the story level (in Trends service) via URL normalization, not in Collector.

**Collector responsibility**: Emit all posts, even if they link to the same URL.

**Trends responsibility**: Group posts by canonical URL when counting.

---

## Configuration Summary

```bash
# Bluesky
BLUESKY_ENABLED=true
BLUESKY_MODE=polling  # or 'firehose'
BLUESKY_POLL_INTERVAL_SECONDS=300
BLUESKY_QUERIES="aws,bedrock,ai,llm,typescript,rust,openai,anthropic"
BLUESKY_BATCH_SIZE=50

# Mastodon
MASTODON_ENABLED=true
MASTODON_POLL_INTERVAL_SECONDS=600
MASTODON_INSTANCES="hachyderm.io,fosstodon.org,infosec.exchange"
MASTODON_TAGS="aws,ai,machinelearning,typescript,rust,devops"
MASTODON_BATCH_SIZE=40
```

---

## Testing

### Mock Responses

```typescript
// packages/shared/testing/fixtures/bluesky.ts
export const blueskySearchResponse = {
  posts: [
    {
      uri: 'at://did:plc:abc123/app.bsky.feed.post/xyz789',
      cid: 'bafyreiabc123',
      author: {
        did: 'did:plc:abc123',
        handle: 'techuser.bsky.social',
        displayName: 'Tech User',
      },
      record: {
        text: 'Just tried the new AWS Bedrock features. The Claude integration is impressive! #aws #ai',
        createdAt: '2026-02-05T10:00:00Z',
      },
      likeCount: 42,
      replyCount: 5,
      repostCount: 12,
    },
  ],
  cursor: 'cursor_abc123',
};
```

### Integration Test

```typescript
describe('BlueskyAdapter', () => {
  it('should fetch and map posts correctly', async () => {
    const adapter = new BlueskyAdapter({
      queries: ['#aws'],
      batchSize: 10,
    });

    mockFetch(blueskySearchResponse);

    const events = await adapter.poll();

    expect(events).toHaveLength(1);
    expect(events[0].source).toBe(Source.BLUESKY);
    expect(events[0].event_id).toBe('bluesky:at://did:plc:abc123/app.bsky.feed.post/xyz789');
    expect(events[0].engagement.likes).toBe(42);
  });
});
```

---

## Lobsters (Computing-Focused Community)

### Why Lobsters

- High-signal, low-noise: Strictly moderated for on-topic computing content
- Invite-only community with engaged technical members
- Complements Hacker News with more niche engineering/CS content
- Official RSS feed available with consistent structure

### API Access

Lobsters provides a simple, no-auth public API:

```typescript
// Fetch hottest stories
const response = await fetch('https://lobste.rs/hottest.json');

// Fetch newest stories
const response = await fetch('https://lobste.rs/newest.json');

// Fetch by tag
const response = await fetch('https://lobste.rs/t/rust.json');
```

### RSS Feed

```
https://lobste.rs/rss
```

### Polling Implementation

```typescript
interface LobstersStory {
  short_id: string;
  short_id_url: string;
  created_at: string;
  title: string;
  url: string;
  score: number;
  flags: number;
  comment_count: number;
  description: string;
  comments_url: string;
  submitter_user: {
    username: string;
  };
  tags: string[];
}

async function fetchLobstersHot(): Promise<LobstersStory[]> {
  const response = await fetch('https://lobste.rs/hottest.json');
  if (!response.ok) {
    throw new Error(`Lobsters API error: ${response.status}`);
  }
  return response.json();
}
```

### Checkpoint Strategy

| Checkpoint Key | Value | Purpose |
|---------------|-------|---------|
| `lobsters.last_short_id` | Short ID (e.g., `abc123`) | Track last seen story |
| `lobsters.last_poll_at` | ISO8601 timestamp | Avoid re-fetching too soon |

### Response Mapping

```typescript
function mapLobstersStory(story: LobstersStory): RawEvent {
  return {
    event_id: `lobsters:${story.short_id}`,
    source: Source.NEWS,  // Use 'news' source type
    fetched_at: new Date().toISOString(),
    published_at: story.created_at,
    url: story.url || story.short_id_url,  // External URL or Lobsters URL
    title: story.title,
    text: story.description || story.title,
    author: {
      handle: story.submitter_user.username,
      display_name: story.submitter_user.username,
    },
    engagement: {
      score: story.score,
      comments: story.comment_count,
    },
    tags: story.tags,  // Lobsters has built-in tags
    source_meta_json: JSON.stringify({
      short_id: story.short_id,
      comments_url: story.comments_url,
      flags: story.flags,
      lobsters_tags: story.tags,
    }),
  };
}
```

### Rate Limiting

Lobsters doesn't document rate limits, but be respectful:
- Poll no more than every 30 minutes
- Fetch only top/hot stories (not full archive)
- Include a User-Agent header identifying your bot

```typescript
const headers = {
  'User-Agent': 'RisingIntelligence/1.0 (https://github.com/your-repo)',
  'Accept': 'application/json',
};
```

### Error Handling

| Error | Action |
|-------|--------|
| 429 (unlikely) | Exponential backoff, max 1 hour |
| 5xx Server Error | Exponential backoff, max 5 min |
| Invalid JSON | Log to DLQ, continue |

### Metrics

- `ri_collector_lobsters_stories_fetched_total`
- `ri_collector_lobsters_poll_duration_seconds`
- `ri_collector_lobsters_errors_total{type}`

### Configuration

```bash
# Lobsters
LOBSTERS_ENABLED=true
LOBSTERS_MODE=hot  # or 'newest'
LOBSTERS_POLL_INTERVAL_SECONDS=1800
LOBSTERS_BATCH_SIZE=25
```

### Tag Filtering (Optional)

Lobsters has a rich tag system. You can filter by tag if desired:

```typescript
const RELEVANT_TAGS = ['ai', 'ml', 'cloud', 'devops', 'rust', 'go', 'security'];

function isRelevantStory(story: LobstersStory): boolean {
  return story.tags.some(tag => RELEVANT_TAGS.includes(tag));
}
```

However, for a general tech intelligence system, fetching all hot stories is recommended since Lobsters is already well-moderated for relevance.
