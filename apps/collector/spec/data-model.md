# Data Model: Collector Service

## Overview

Collector is primarily a stateless transformer from “source items” → `RawEvent`.

## Key concepts

- **Source item**: the raw record returned by an upstream API/feed.
- **Cursor/checkpoint**: per-source state used to resume polling without excessive duplicates.

## Contracts

- Canonical output: `RawEvent` (defined in `specs/001-real-time-personal-intelligence-system.md`).
- DLQ payload shape is implementation-defined but MUST include:
  - `source`, `fetched_at`
  - error type/code
  - a redacted sample of the raw payload or URL reference

## Checkpoint storage (MVP)

MVP decision:

- Use Redis keys (local Compose includes Redis with persistence enabled).

### Suggested Redis keys

- `collector:cursor:rss:<feed_url_hash>` → last seen GUID/URL hash + timestamp
- `collector:cursor:hn` → last seen item id + timestamp
- `collector:cursor:reddit:<subreddit>` → last seen fullname/cursor + timestamp

### Dedupe cache

To bound duplicates across restarts, the collector SHOULD maintain a TTL cache of recently emitted IDs:

- `collector:seen:<source>:<event_id>` → `1` (TTL 7–14 days)
