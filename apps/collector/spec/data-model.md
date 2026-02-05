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

## Checkpoint storage (TBD)

MVP options (choose one):

- Redis keys (recommended if Redis is in the stack)
- Local file persisted via a bind mount (dev-only)
