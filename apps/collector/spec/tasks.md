# Tasks: Collector Service

## Progress

- 2026-02-11: Implemented T009 source staleness metrics (`ri_collector_last_success_timestamp{source}`, `ri_collector_source_healthy{source}`) and preserved per-source `last_poll_at` across error states.
- 2026-02-15: Implemented POS source-pack tasks T012-T020 (feed-set config, market-filter loading, high/low-volume gates, market tags + metadata, EDGAR form allowlist + detail metadata, no primary-doc downloads, and 30-minute EDGAR polling jitter guardrails). Expanded EDGAR watchlist coverage in `infra/config/feeds.pos.yaml` and corrected CIK mappings for Global Payments and Adyen.
- 2026-02-15: Hardened POS runtime defaults for local deployment: added configurable SEC-compliant EDGAR user-agent (`SEC_USER_AGENT`), switched BIS source to a live RSS endpoint, and added public payments newsroom feeds (`PYMNTS`, `PaymentsJournal`) to improve steady POS signal volume.

## Phase 1: Skeleton + contracts

### T001: Service skeleton

- **Acceptance**: service starts, exports `/metrics`, emits a startup log with `service=collector`.

### T002: Kafka publish + DLQ

- **Acceptance**: invalid payloads are rejected and sent to `events.raw.dlq`; valid payloads land on `events.raw`.

## Phase 2: MVP sources

### T003: RSS/Atom adapter

- **Acceptance**: new feed entries are emitted exactly once per `event_id` per run.

### T004: Hacker News adapter

- **Acceptance**: top/new stories (configurable) are emitted with stable IDs.

### T005: Reddit adapter

- **Acceptance**: new posts from configured subreddits are emitted; 429 handling backs off.

## Phase 3: Ops hardening

### T006: Cursor checkpointing

- **Acceptance**: restart does not cause full backfill; cursor resumes.

### T007: Observability wiring

- **Acceptance**: traces appear in Tempo and logs correlate via `traceId`.

### T008: Health heartbeat publishing

- **Acceptance**: Collector publishes a heartbeat event to `collector.heartbeat` topic every 60 seconds per source, indicating the source is being actively polled.

### T009: Source staleness metrics

- **Acceptance**: Metrics `ri_collector_last_success_timestamp{source}` and `ri_collector_source_healthy{source}` are exported for alerting.

## Phase 4: Additional sources

### T010: Bluesky adapter

- **Acceptance**: Posts matching configured hashtags are emitted; optional Jetstream firehose mode.

### T011: Mastodon adapter

- **Acceptance**: Public timeline posts from configured instances are emitted; per-instance rate limiting.

## Phase 5: POS source-pack (public feeds, phase 1)

### T012: POS feed-set config

- **Acceptance**: `infra/config/feeds.pos.yaml` exists with phase-1 public feeds (EDGAR watchlist, SEC, Fed, BIS, CISA, Target, PR Newswire).

### T013: Market filter profile directory

- **Acceptance**: Collector loads all `infra/config/market-filters/*.yaml` profiles at startup; startup fails loudly for invalid profile YAML.

### T014: High-volume strict gate

- **Acceptance**: PR Newswire items are ingested only when they match both an entity term and a market keyword.

### T015: Low-volume keyword gate

- **Acceptance**: Low-volume public-feed items are ingested only when they match market keywords.

### T016: Market tags + metadata

- **Acceptance**: Matching entries include `market.<profile>` in `RawEvent.tags` and also set `source_meta.market_profiles` + `source_meta.match_reasons`.

### T017: EDGAR high-signal forms allowlist

- **Acceptance**: EDGAR ingestion is restricted to `8-K`, `6-K`, `10-Q`, `10-K`, `20-F`, `40-F`.

### T018: EDGAR filing detail metadata enrichment

- **Acceptance**: For retained EDGAR entries, collector fetches filing detail pages and stores normalized metadata (cik, form_type, accession_number, filed_date, accepted_at, filing_detail_url where available).

### T019: No primary-doc downloads (phase 1)

- **Acceptance**: Collector does not download primary filing documents for EDGAR events.

### T020: EDGAR polling guardrails

- **Acceptance**: EDGAR uses a single 30-minute base polling interval with significant jitter to reduce bursty request patterns.
