# Spec 010: POS Intelligence Source Pack

**Created**: 2026-02-11  
**Updated**: 2026-02-15  
**Status**: Implemented (phase-1 public feeds)

## Overview

This spec defines the phase-1 source-pack behavior for point-of-sale (POS) and merchant-payments intelligence using publicly accessible feeds only.

It introduces:

- a POS-focused feed set configuration,
- SEC/EDGAR watchlist ingestion constraints,
- market-profile filtering semantics,
- high-volume vs low-volume filter policy, and
- metadata/tagging expectations required for downstream trend and brief workflows.

## Scope

### In scope (phase 1)

- Public feeds:
  - SEC/EDGAR per-company Atom feeds (watchlist coverage: P1 + P2 + P3 companies).
  - SEC press releases.
  - Federal Reserve RSS.
  - BIS RSS.
  - CISA advisory XML feeds.
  - Target corporate RSS feeds.
  - PR Newswire all releases RSS.
- Collector-side filtering and metadata enrichment for market profiles.
- `riops brief trigger` spec changes for deriving topic globs from selected feed config YAML files.

### Out of scope (phase 1)

- Licensed/private feed endpoints (for example, tokenized Business Wire or paid GlobeNewswire content feeds).
- Primary filing document downloads from EDGAR filing detail pages.
- Non-RSS website scraping for newsroom pages without stable feeds.

## Source Policy

### EDGAR watchlist

- Ingestion MUST include all watchlist companies provided by the operator (P1/P2/P3).
- CIK MUST be the stable primary identifier.
- Allowed form types in phase 1 are restricted to:
  - `8-K`
  - `6-K`
  - `10-Q`
  - `10-K`
  - `20-F`
  - `40-F`
- Collector MUST fetch filing detail pages for matched entries.
- Collector MUST NOT download primary filing documents in phase 1.

### Polling and jitter

- EDGAR company feeds MUST use a single base interval of 30 minutes.
- Polling MUST include significant random jitter to avoid synchronized request bursts.
- Jitter MUST be configurable (`EDGAR_POLL_JITTER_RATIO`) and enabled by default.
- `EDGAR_POLL_JITTER_RATIO` MUST be validated on startup to be within 0.0–1.0 inclusive; invalid values MUST cause a fail-fast startup error.

### High-volume vs low-volume policy

- PR Newswire is treated as the only high-volume source in phase 1.
- High-volume sources MUST pass strict gate:
  - at least one watchlist entity match, and
  - at least one market keyword match.
- Low-volume sources MUST still apply market keyword filtering.

## Market Filter Model

### Config shape and loading

- Market filters live in:
  - `infra/config/market-filters/*.yaml`
- Collector MUST load all profiles found in this folder.
- There is no active-profile selector in phase 1; all profiles are evaluated.

### Match semantics

- An event is eligible for ingest when it matches at least one profile.
- Profile match results MUST be captured as:
  - tags (for example `market.pos`), and
  - metadata fields for auditability (`market_profiles`, `match_reasons`).

### Relationship to topic extraction

- Market profile tags and canonical topic tags are distinct concerns:
  - market tags represent profile classification,
  - canonical tags represent trend taxonomy.
- Both may coexist in `RawEvent.tags` when matched.

## Metadata Requirements

Collector SHOULD enrich `source_meta` with the following fields when available:

- `source_type` (for example `edgar`, `policy`, `security`, `wire`, `merchant`)
- `signal_tier` (`high_volume` or `low_volume`)
- `market_profiles` (array)
- `match_reasons` (array of compact rule explanations)
- `feed_name`
- `feed_url`

For EDGAR entries, collector SHOULD include:

- `cik`
- `form_type`
- `accession_number`
- `filed_date`
- `accepted_at` (if available)
- `filing_detail_url`
- `primary_document_name` (if available from detail page metadata)

## `riops` Topic-Glob Derivation Policy

When `riops brief trigger` is used with `--feed-config`:

- It MUST parse one or more YAML files (repeated `--feed-config` flags).
- It MUST derive topic globs from non-empty `topics` arrays across all feed entries, regardless of section name.
- It MUST include topics from entries even if `enabled: false`.
- It MUST ignore empty `topics: []` entries and emit warnings.
- It MUST union derived globs with explicit `--topic-globs`.
- If a referenced feed config file does not exist, command MUST fail with actionable error.
- If derived globs are empty but explicit `--topic-globs` are provided, command MAY proceed with warning.
- If neither derived nor explicit globs are provided, query defaults remain wildcard (`*`).

## Acceptance Checks

1. With market filter profiles present, collected events include `market.<profile>` tags and `source_meta.market_profiles`.
2. PR Newswire entries without both entity + keyword match are dropped.
3. Low-volume feed entries require keyword match and are dropped otherwise.
4. EDGAR polling runs at 30-minute base cadence with jitter.
5. EDGAR events include filing detail-page metadata but no downloaded primary-doc payload.
6. `riops brief trigger` can derive/merge topic globs from multiple `--feed-config` files with warning/error behaviors as specified.

## Implementation Notes

- Baseline feed config lives in `infra/config/feeds.pos.yaml`.
- Baseline market profile lives in `infra/config/market-filters/pos.yaml`.
- Collector implementation and tests are in `apps/collector/src/adapters/rss.ts` and `apps/collector/tests/adapters/rss.test.ts`.
- Feed-derived topic glob behavior for `riops brief trigger` is implemented in `packages/ops-cli/src/commands/brief/feed-config.ts` and `packages/ops-cli/src/commands/brief/trigger.ts`.
