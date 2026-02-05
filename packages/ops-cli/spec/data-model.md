# Data Model: Ops CLI

The CLI is command-driven; it does not own durable storage.

Key inputs:

- env vars (loaded via `@rising-intelligence/shared`)
- arguments parsed from `process.argv`

Key outputs:

- stdout/stderr (human-readable logs)
- exit code (0 success, non-zero failure)

