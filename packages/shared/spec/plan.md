# Implementation Plan: Shared Package

## Phase 1: Contracts + runtime primitives (MVP)

- Protobuf contracts in `contracts/proto/`
- Runtime helpers:
  - `.env` loader for local convenience
  - `getSecret` / `getSecretValue` with `*_FILE` support
- Shared constants for Schema Registry publication

## Phase 2: Config builders (post-MVP)

- Typed config builders per service (collector/trends/brief)
- Redaction helpers for safe logging of config objects

## Phase 3: Generated types (post-MVP)

- Adopt a Protobuf toolchain (e.g., Buf) for:
  - breaking-change checks
  - generated TS types for message contracts

