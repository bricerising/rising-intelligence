# Spec 001: Real-Time Personal Intelligence System

**Created**: 2026-02-05  
**Status**: Proposed

## Overview

Build a self-hosted, near-real-time “personal intelligence aggregator” that:

1) continuously ingests signals from curated tech sources and social/developer platforms,  
2) detects which topics are gaining traction (volume + acceleration), and  
3) produces actionable briefs and dashboards (with optional alerts).

The system is optimized for a single operator (you) monitoring tech, AI, and AWS trends on a home network.

## Goals

- Ingest multiple sources (social + curated) into a uniform event stream.
- Detect emerging trends using volume and acceleration metrics on sliding windows.
- Generate daily (and optionally on-demand) topic briefs with links and suggested actions.
- Provide real-time dashboards for both raw chatter and computed trend metrics.
- Keep components decoupled so ingestion, trend detection, and LLM summarization can evolve independently.

## Non-goals

- Building a generalized “web crawler” or full-text search engine.
- Multi-tenant SaaS, complex user management, or external public-facing APIs.
- Perfect topic modeling from day 1 (MVP relies on allowlisted topics + simple entity extraction).
- Long-term archival of all raw content (retain enough for trend windows + short replay).
- “Real-time” LLM processing per-event (LLM runs on aggregates to control cost/latency).

## Definitions

- **Event**: A normalized record of a single source item (tweet/post/article/release).
- **Topic**: A tracked term/entity/category used for counting (e.g., “AWS”, “Bedrock”, “Rust”, “OpenAI”).
- **Volume**: Count of events mentioning a topic within a window (e.g., 60 minutes).
- **Acceleration**: Change in volume/rate between windows (e.g., last 60m vs previous 60m), optionally normalized to a baseline.
- **Trend Score**: A composite score used to rank topics (volume + acceleration + baseline delta).
- **Brief**: An LLM-generated report summarizing top trends with source links and suggested actions.

## Repository Layout (Specify-Poker Style)

This repo follows the same documentation + monorepo shape as `specify-poker`:

- System intent and cross-cutting constraints live in `specs/`.
- Each deployable service lives in `apps/<service>/` with a local spec bundle in `apps/<service>/spec/`.
- Shared runtime primitives and contracts live in `packages/shared/` (schemas, config, lifecycle, telemetry helpers).
- Local-first infrastructure wiring lives in `infra/` and `docker-compose.yml`.

Planned tree:

```text
apps/
  collector/          # multi-source ingestion → RawEvent
    spec/
  trends/             # windowed aggregation + scoring → TrendSnapshot
    spec/
  brief/              # LLM summarization → Brief
    spec/
packages/
  shared/             # contracts + config + lifecycle + telemetry + kafka helpers
infra/
  grafana/            # provisioning + dashboards
  loki/
  mimir/
  tempo/
  otel/
specs/                # thematic system specifications
```

## Requirements

### Functional

- **R-001 (Ingestion)**: The system MUST ingest items from at least 3 sources in MVP:
  - 1 curated source (RSS/news/blogs),
  - 1 developer community source (e.g., Hacker News, GitHub releases),
  - 1 discussion source (e.g., Reddit).
- **R-002 (Normalization)**: All ingested items MUST be converted to a shared `RawEvent` schema and published to the stream.
- **R-003 (Idempotency)**: Ingestion MUST deduplicate by stable `event_id` per source.
- **R-004 (Trend Metrics)**: The system MUST compute topic metrics on sliding windows and publish periodic `TrendSnapshot` outputs.
- **R-005 (Ranking)**: The system MUST output a ranked “Top N Trends” list for a configurable window (e.g., 60m and 24h).
- **R-006 (Brief Generation)**: The system MUST produce a daily brief (scheduled) from top trends and their supporting items.
- **R-007 (Dashboards)**: The system MUST expose dashboards for:
  - raw event exploration (search/filter by source/topic),
  - trend time series and “Top N” tables,
  - latest brief content.
- **R-008 (Alerts, Optional MVP)**: The system SHOULD support alerting when a trend crosses a threshold (score or acceleration).

### Non-functional

- **NFR-001 (Latency)**: Ingested events SHOULD be available for processing within 60 seconds of fetch (excluding upstream API delays).
- **NFR-002 (Replay)**: The pipeline MUST support replay/reprocessing for at least 7 days of data.
- **NFR-003 (Resilience)**: The system MUST tolerate upstream source outages and API rate limiting without data corruption.
- **NFR-004 (Cost Control)**: LLM usage MUST be bounded (batch + top-trends only) with a configurable daily token/cost budget.
- **NFR-005 (Local-first Security)**: Secrets MUST be stored out of source control and not logged.
- **NFR-006 (Auditability)**: Trend scores and briefs MUST link back to source URLs/IDs used as evidence.

## Invariants (“Constitution”)

- **I-001**: Raw source content is immutable once ingested (append-only); downstream processing is derived data.
- **I-002**: Consumers MUST be safe under at-least-once delivery (duplicates are expected).
- **I-003**: Briefs MUST include citations (links) for each major claim or trend driver.
- **I-004**: No secrets (API keys/tokens) are ever emitted to logs, Kafka topics, or dashboard panels.

## Architecture

### High-level components

- **Collector service** (`apps/collector`): fetch (multiple sources) → normalize → publish.
- **Kafka (or compatible)**: central event bus + retention for replay.
- **Trend processor** (`apps/trends`): topic extraction + windowed aggregation + scoring.
- **Storage/observability**:
  - **Loki** for raw event logs (search + ad-hoc LogQL metrics).
  - Optional **Prometheus/Mimir** for first-class metrics.
  - Optional **Postgres/SQLite** for durable trend snapshots and briefs.
- **LLM summarizer service** (`apps/brief`, LangChain): consumes summary requests → emits briefs.
- **Grafana dashboards**: metrics, logs, top trends, and brief display.

### Data flow

```mermaid
flowchart LR
  subgraph Sources
    RSS[RSS / News / Blogs]
    HN[Hacker News / Dev feeds]
    RD[Reddit]
    GH[GitHub releases/trending]
    X["Twitter/X (optional)"]
  end

  C[collector]

  K[(Kafka / Redpanda)]
  L[(Loki)]
  P[(Prometheus/Mimir\n(optional))]
  DB[(DB\n(optional))]

  TP[trend-processor]
  SR[summary-requests topic]
  SS[llm-summarizer]
  OUT[briefs + trend snapshots]
  G[Grafana]

  RSS --> C --> K
  HN --> C
  RD --> C
  GH --> C
  X --> C

  C --> L --> G
  K --> TP --> OUT --> G
  TP --> P --> G
  TP --> DB

  TP --> SR --> SS --> OUT
  SS --> DB
```

## Event & Topic Model

### RawEvent schema (contract)

MVP uses a single canonical schema for all sources.

```ts
export type Source =
  | "rss"
  | "news"
  | "hackernews"
  | "reddit"
  | "github"
  | "twitter";

export interface RawEvent {
  event_id: string; // stable per-source unique ID (e.g., tweet id, reddit fullname, URL hash)
  source: Source;
  fetched_at: string; // ISO8601
  published_at?: string; // ISO8601 (if known)

  url?: string;
  title?: string;
  text: string; // primary content for extraction/search

  author?: { id?: string; handle?: string; display_name?: string };
  engagement?: { score?: number; comments?: number; likes?: number; shares?: number };

  // Derived at ingestion time (cheap)
  lang?: string;
  tags?: string[]; // e.g., ["ai", "aws"]
  extracted?: {
    hashtags?: string[];
    urls?: string[];
  };

  // Free-form metadata per-source (kept small; no secrets)
  source_meta?: Record<string, unknown>;
}
```

### TrendSnapshot schema (contract)

```ts
export interface TopicMetrics {
  topic: string; // canonical topic key, e.g. "aws.bedrock"
  window: "15m" | "60m" | "24h";
  window_end: string; // ISO8601

  volume: number; // count in window
  prev_volume?: number; // previous equal-sized window
  acceleration?: number; // e.g., (volume - prev_volume) / max(prev_volume, 1)
  baseline_volume?: number; // e.g., 7d moving average for that window slot
  baseline_delta?: number; // e.g., (volume - baseline) / max(baseline, 1)

  score: number; // normalized 0..10 (or 0..100), configurable
  evidence?: { top_urls: string[]; top_event_ids: string[] };
}

export interface TrendSnapshot {
  generated_at: string; // ISO8601
  window: "15m" | "60m" | "24h";
  topics: TopicMetrics[]; // sorted desc by score
}
```

### Topic extraction (MVP)

MVP topic extraction uses:

- an **allowlist** of canonical topics with matchers (regex, keyword sets), and
- lightweight entity hints (hashtags, repo names, product names) extracted from `RawEvent.text/title`.

The allowlist MUST support aliases (e.g., “EC2” → `aws.ec2`, “Bedrock” → `aws.bedrock`).

## Streaming & Storage Contracts

### Kafka topics (suggested)

- `events.raw`: all `RawEvent` messages (partition key: `event_id`).
- `events.raw.dlq`: failed parse/normalize (includes error context; no secrets).
- `trends.snapshots`: periodic `TrendSnapshot`.
- `summary.requests`: requests to generate a brief (daily or threshold-triggered).
- `summary.results`: produced briefs (plus metadata/citations).

### Retention

- `events.raw`: 7–14 days (enough for replay + baseline computation).
- `trends.snapshots`: 30–90 days (small, useful for history charts).
- `summary.results`: 90+ days (very small, high value).

## Configuration

All configuration MUST be externalized (env vars and/or config files) and safe to commit (no secrets).

### Required (MVP)

- `KAFKA_BROKERS` (e.g., `localhost:9092`)
- `KAFKA_CLIENT_ID`
- `KAFKA_CONSUMER_GROUP` (per service)
- `LOKI_URL` (if mirroring raw events to Loki)
- `TOPICS_ALLOWLIST_PATH` (aliases + matchers)

### Source configuration (suggested)

- RSS/Blogs: `RSS_FEED_URLS` (comma-separated)
- Reddit: `REDDIT_SUBREDDITS` (comma-separated), plus credentials if required
- Hacker News: `HN_MODE` (`top`|`new`) and `HN_POLL_INTERVAL_SECONDS`
- GitHub: `GITHUB_TRACKED_REPOS` (comma-separated `owner/repo`), `GITHUB_TOKEN`
- LLM: `LLM_PROVIDER`, `LLM_MODEL`, `LLM_DAILY_BUDGET_USD`, `LLM_MAX_TOPICS_PER_BRIEF`

### Suggested initial sources (MVP defaults)

- RSS/Blogs:
  - AWS News Blog
  - AWS “What’s New” RSS
  - A small set of tech/AI outlets you trust (3–10 feeds total)
- Reddit subreddits:
  - `r/aws`, `r/MachineLearning`, `r/technology`, `r/devops` (tune to taste)
- Dev/curation:
  - Hacker News top stories (poll)
  - GitHub releases for a curated list of repos (avoid scraping trending in MVP)

## Trend Detection & Scoring

### Windows

- Compute metrics on at least `15m` and `60m` windows in MVP.
- Optionally compute `24h` aggregates for “daily context”.

### Scoring (MVP proposal)

Trend score SHOULD balance:

- **volume**: topics with meaningful absolute activity, and
- **acceleration**: topics rapidly increasing.

Example (configurable):

- `accel = (volume_60m - prev_volume_60m) / max(prev_volume_60m, 1)`
- `baseline_delta = (volume_60m - baseline_60m) / max(baseline_60m, 1)` (baseline = 7-day median or mean)
- `score = clamp01(wv * norm(volume_60m) + wa * norm(accel) + wb * norm(baseline_delta)) * 10`

Where `norm()` maps to 0..1 (e.g., logistic scaling) and weights `wv/wa/wb` are tunable.

### Trend detection rules

- A topic is “Trending” if `score >= threshold` OR it is in the current Top N.
- A topic is “Emerging” if `acceleration >= accel_threshold` AND `volume >= min_volume`.
- The system SHOULD support suppression rules (mute topics) to reduce noise.

## Summarization & Insight Generation (LLM)

### Responsibilities

The LLM summarizer service MUST:

- Generate a concise brief for each summary request.
- Ground each trend in **evidence** (URLs + representative event IDs).
- Produce at least one **actionable suggestion** per top trend (e.g., “read X”, “test Y”, “watch for Z”).
- Avoid fabricated facts; if uncertain, it MUST say so.

### Brief format (contract)

```ts
export interface Brief {
  brief_id: string;
  generated_at: string; // ISO8601
  window: "daily" | "threshold";

  title: string;
  highlights: Array<{
    topic: string;
    why_it_matters: string;
    what_happened: string;
    suggested_action: string;
    citations: string[]; // URLs
  }>;

  notes?: string; // limitations, coverage gaps, etc.
}
```

### Triggering

- **Daily**: fixed local time (e.g., 17:00) using the last 24h + last 60m context.
- **Threshold** (optional): if any topic exceeds alert threshold, request a short “flash brief”.

### Cost controls

- Summarize only Top N (e.g., 5–10) topics per brief.
- Limit evidence per topic (e.g., 3–8 items) by a deterministic selector:
  - top-engagement items,
  - diverse sources (at least 1 curated + 1 discussion where available),
  - dedupe near-identical text/URLs.

## Dashboards & UX

### Grafana panels (MVP)

- **Top Trends (60m)**: table of `topic, volume, acceleration, score`.
- **Mentions Over Time**: time series for top topics (last 24h).
- **Raw Stream Explorer**: Loki log panel filtering by `source` and `topic`.
- **Latest Brief**: text/markdown panel showing most recent `Brief`.

### Alerting (optional MVP)

- Alert when `score` or `acceleration` crosses configured thresholds.
- Alert routing: email, Slack, or push notification (implementation-specific).

## Observability

### Logs (required fields)

- `service`, `source`, `event_id`, `topic` (if derived), `fetched_at`, `published_at` (if known)
- `kafka_topic`, `partition`, `offset` (for consumers)
- `error_code`, `error_message` (no secrets), `retry_count`

### Metrics (minimum)

- Ingestion: `events_ingested_total{source=...}`, `ingest_failures_total{source=...}`, `ingest_lag_seconds{source=...}`
- Kafka consumer: `consumer_lag{group=...}`
- Trend processing: `trend_compute_duration_seconds`, `topics_ranked_total`
- LLM: `briefs_generated_total`, `llm_latency_seconds`, `llm_tokens_total`, `llm_cost_estimated_usd`

## Resilience

- Ingestion MUST implement exponential backoff with jitter for:
  - HTTP 429 rate limits,
  - transient network errors,
  - 5xx responses.
- Each ingestion service SHOULD checkpoint “last seen” cursor per source to avoid gaps/duplicates.
- Consumers MUST be idempotent (store per-window aggregates in a way that tolerates reprocessing).
- Failures to parse/normalize MUST go to DLQ with enough context to debug.

## Security / Privacy

- API keys/secrets MUST be provided via environment variables or a local secrets manager.
- The system MUST support redaction of potentially sensitive fields before storage.
- Data retention MUST be configurable (especially for social content) to respect privacy and storage limits.
- The operator MUST ensure ingestion complies with each platform’s Terms of Service (especially Twitter/X).

## Acceptance

### MVP acceptance scenarios

1) **Ingestion → Stream**
   - Given configured credentials/feeds,
   - When the ingestion services run for 30 minutes,
   - Then `events.raw` receives `RawEvent` messages from all MVP sources with valid schema and no duplicates beyond at-least-once expectations.

2) **Trend snapshots**
   - Given events flowing for at least 2 hours,
   - When the trend processor runs,
   - Then `trends.snapshots` publishes snapshots at a fixed cadence (e.g., every 5 minutes) and ranks topics deterministically.

3) **Daily brief**
   - Given at least 1 day of events,
   - When the daily brief job triggers,
   - Then a `Brief` is produced with Top N trends, each including citations and one suggested action.

4) **Grafana visibility**
   - Given Loki and Grafana configured,
   - When events and snapshots are produced,
   - Then the dashboard shows raw events (filterable) and trend metrics time series for the last 24 hours.

5) **Safety / compliance**
   - Given the system runs with real credentials,
   - When ingestion, processing, and brief generation run,
   - Then no secrets appear in logs, Kafka topics, or dashboards, and retention settings are enforced.

## Definition of Done (MVP)

MVP is “done” when:

- Acceptance scenarios 1–5 are satisfied in a local Compose environment.
- The daily brief runs on schedule for 3 consecutive days without manual intervention.
- LLM spend stays within the configured daily budget for those runs.

## Local Verification (planned)

Once implemented, provide a minimal set of commands/docs to verify locally:

- Bring up stack: `docker compose up -d`
- Verify services: Grafana UI loads and Loki data source can query recent `events.raw` logs
- Verify pipeline:
  - ingestion services publish `RawEvent` to `events.raw`
  - trend processor publishes to `trends.snapshots`
  - brief generator publishes to `summary.results`
- Verify dashboards:
  - “Top Trends (60m)” table is populated
  - “Mentions Over Time” shows non-empty time series
  - “Latest Brief” shows the newest brief content

## Milestones (suggested)

- **M0 (Local infra)**: Docker Compose brings up Kafka/Redpanda + Grafana + Loki.
- **M1 (MVP ingestion)**: RSS + Hacker News + Reddit → `events.raw` (+ Loki mirror).
- **M2 (MVP trends)**: allowlist topics + `15m/60m` windows + `trends.snapshots` output.
- **M3 (MVP daily brief)**: LLM summarizer consumes top trends and outputs a `Brief`.
- **M4 (Alerts + baselines)**: 7-day baseline, alert rules, and “flash brief” on spikes.
- **M5 (Quality upgrades)**: better entity extraction, topic aliasing, suppression rules, and source diversity in evidence selection.

## Open Questions / Decisions

- **Kafka vs Redpanda**: is Kafka required, or is API-compatibility sufficient?
- **Twitter/X access**: do you have API access? If not, which alternate sources cover enough “reaction” signal?
- **Storage**: is Loki sufficient for raw events, or do you also want a DB for queryable history and briefs?
- **Baselines**: pick baseline method (7-day mean vs median; day-of-week normalization).
- **LLM model**: hosted API vs local model; required latency and daily budget.

## References (context only)

- Grafana Labs: [Going off-label with Grafana Loki: low-cost Twitter analysis](https://grafana.com/blog/going-off-label-with-grafana-loki-how-to-set-up-a-low-cost-twitter-analysis/)
- Kai Waehner: [GenAI Demo with Kafka, Flink, LangChain and OpenAI](https://www.kai-waehner.de/blog/2024/01/29/genai-demo-with-kafka-flink-langchain-and-openai/)
- Example repos:
  - [davidjosipovic/news-trend-analysis](https://github.com/davidjosipovic/news-trend-analysis)
  - [elecmonkey/news-pipeline](https://github.com/elecmonkey/news-pipeline)
- Medium (Oct 2025): [Kafka + LangChain + LLMs: Streaming Real-Time Intelligence…](https://medium.com/@atnofordatascience/kafka-langchain-llms-streaming-real-time-intelligence-into-your-data-stack-b7f91fc08a3f)
