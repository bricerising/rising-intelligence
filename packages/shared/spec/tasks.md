# Tasks: Shared Package

## Phase 1: MVP

### T001: Secret resolution helpers

- **Acceptance**: `getSecretValue("X")` resolves from `X` or `X_FILE` and never logs secret values.

### T002: Dotenv loader

- **Acceptance**: `loadDotEnv()` loads repo-root `.env` (or `RI_ENV_PATH`) for local tooling.

### T003: Contract paths + subject constants

- **Acceptance**: ops tooling can locate proto files and subject names via shared exports.

## Phase 2: Post-MVP

### T004: Config redaction helper

- **Acceptance**: `redactConfig()` replaces secret fields with `"[REDACTED]"` for safe logging.

