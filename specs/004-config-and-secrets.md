# Spec 004: Configuration + Secrets

**Created**: 2026-02-05  
**Status**: Proposed

## Overview

This repo is local-first, but it still needs consistent configuration and secret handling across:

- services (`apps/*`)
- ops tooling (`packages/ops-cli`, binary: `riops`)

The source of truth for configuration conventions is `@rising-intelligence/shared`.

## Principles

- **Env-first**: configuration is provided via environment variables.
- **No secrets in Git**: secrets are never committed; `.env` is gitignored.
- **One resolver everywhere**: CLI and services resolve secrets the same way.
- **Secret-file support**: every secret MAY be provided via a `*_FILE` path.

## Local development flow

1) Copy `.env.example` → `.env` and fill in real values.
2) Docker Compose uses `.env` for variable substitution when starting containers.
3) `riops` loads `.env` automatically on startup (via `@rising-intelligence/shared`).

## Secret resolution contract

For any secret named `FOO`:

1) If `FOO` is set, use it.
2) Else, if `FOO_FILE` is set, read the file contents and use it (trailing newline trimmed).
3) Else, if `FOO` is required, fail fast with a clear error.

This behavior is implemented in:

- `packages/shared/src/runtime/secrets.ts`

## Dotenv loading (dev convenience)

`riops` loads a repo-root `.env` file by default (or `RI_ENV_PATH` if set).

Implementation:

- `packages/shared/src/runtime/env.ts`

Services MAY use the same helper in local non-container runs, but when running in Compose they should rely on real env vars.

## Redaction / logging requirements

- Never log raw secret values.
- When logging config objects, redact keys that match common secret patterns (`*_KEY`, `*_TOKEN`, `*_PASSWORD`, etc.).

(Implementation will land once the shared logger exists.)
