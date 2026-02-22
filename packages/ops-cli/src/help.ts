import type { CommandRegistry } from "./command-registry.js";

function renderCommands(registry: CommandRegistry): string {
  const rows = registry.list().map((command) => ({
    label: `${command.group} ${command.command}`,
    summary: command.summary,
  }));

  const width = rows.reduce((max, row) => Math.max(max, row.label.length), 0) + 2;
  return rows
    .map((row) => `  ${row.label.padEnd(width)}${row.summary}`)
    .join("\n");
}

export function printHelp(registry: CommandRegistry, errorMessage?: string) {
  if (errorMessage) {
    // eslint-disable-next-line no-console
    console.error(errorMessage);
    // eslint-disable-next-line no-console
    console.error("");
  }

  // eslint-disable-next-line no-console
  console.log(`rising-intelligence ops CLI

Usage:
  riops <group> <command> [flags]

Commands:
${renderCommands(registry)}

Flags (schema-registry publish-protos):
  --schema-registry-url <url>      Default: $SCHEMA_REGISTRY_URL or http://localhost:8081
  --compatibility <mode>           Default: $SCHEMA_COMPATIBILITY or BACKWARD
  --retries <n>                    Default: $SCHEMA_REGISTRY_RETRIES or 10
  --retry-initial-ms <ms>          Default: $SCHEMA_REGISTRY_RETRY_INITIAL_MS or 250
  --retry-max-ms <ms>              Default: $SCHEMA_REGISTRY_RETRY_MAX_MS or 5000
  --timeout-ms <ms>                Default: $SCHEMA_REGISTRY_TIMEOUT_MS or 8000
  --dry-run                        Print actions without calling Schema Registry

Flags (kafka create-topics):
  --kafka-brokers <brokers>        Default: $KAFKA_BROKERS or localhost:9092

Flags (kafka topics):
  --kafka-brokers <brokers>        Default: $KAFKA_BROKERS or localhost:9092

Flags (kafka ensure-topics):
  --kafka-brokers <brokers>        Default: $KAFKA_BROKERS or localhost:9092
  --topics <csv>                   Optional CSV topic list (default: required topics set)
  --partitions <n>                 Used with --topics (default: 1)
  --replication-factor <n>         Used with --topics (default: 1)
  --wait-timeout-ms <ms>           Default: 15000
  --poll-interval-ms <ms>          Default: 250

Flags (topics list):
  --database-url <url>             Default: $DATABASE_URL or constructed from postgres-* flags
  --postgres-host <host>           Default: $POSTGRES_HOST or localhost
  --postgres-port <port>           Default: $POSTGRES_PORT or 5432
  --postgres-db <db>               Default: $POSTGRES_DB or rising_intelligence
  --postgres-user <user>           Default: $POSTGRES_USER or rising
  --postgres-password <password>   Default: $POSTGRES_PASSWORD or secret file
  --counts                         Show event count per topic (sorted by count desc)
  --min-count <n>                  Minimum event count to include (requires --counts)

Flags (topics retag):
  --database-url <url>             Default: $DATABASE_URL or constructed from postgres-* flags
  --postgres-host <host>           Default: $POSTGRES_HOST or localhost
  --postgres-port <port>           Default: $POSTGRES_PORT or 5432
  --postgres-db <db>               Default: $POSTGRES_DB or rising_intelligence
  --postgres-user <user>           Default: $POSTGRES_USER or rising
  --postgres-password <password>   Default: $POSTGRES_PASSWORD or secret file
  --allowlist-path <path>          Default: <repo>/infra/config/topics.allowlist.yaml
  --all                            Retag all rows (default: only rows with empty tags/topics)
  --source <source>                Optional source filter (rss|news|hackernews|reddit|github|bluesky|mastodon)
  --limit <n>                      Maximum rows to scan
  --batch-size <n>                 Batch size for reads/updates (default: 200)
  --dry-run                        Show what would change without updating rows

Flags (events enrich):
  --database-url <url>             Default: $DATABASE_URL or constructed from postgres-* flags
  --postgres-host <host>           Default: $POSTGRES_HOST or localhost
  --postgres-port <port>           Default: $POSTGRES_PORT or 5432
  --postgres-db <db>               Default: $POSTGRES_DB or rising_intelligence
  --postgres-user <user>           Default: $POSTGRES_USER or rising
  --postgres-password <password>   Default: $POSTGRES_PASSWORD or secret file
  --allowlist-path <path>          Default: <repo>/infra/config/topics.allowlist.yaml (when retag step is enabled)
  --steps <csv>                    Enrichment steps in order (default: retag,quality)
  --missing-only                   Process only rows with empty tags/topics
  --source <source>                Optional source filter (rss|news|hackernews|reddit|github|bluesky|mastodon)
  --limit <n>                      Maximum rows to scan
  --batch-size <n>                 Batch size for reads/updates (default: 200)
  --dry-run                        Show what would change without updating rows

Flags (db snapshot):
  --database-url <url>             Default: $DATABASE_URL or constructed from postgres-* flags
  --postgres-host <host>           Default: $POSTGRES_HOST or localhost
  --postgres-port <port>           Default: $POSTGRES_PORT or 5432
  --postgres-db <db>               Default: $POSTGRES_DB or rising_intelligence
  --postgres-user <user>           Default: $POSTGRES_USER or rising
  --postgres-password <password>   Default: $POSTGRES_PASSWORD or secret file
  --output-dir <path>              Default: $POSTGRES_SNAPSHOT_DIR or <repo>/backups/postgres
  --label <text>                   Optional suffix label in snapshot filename
  --retention-days <n>             Default: $POSTGRES_SNAPSHOT_RETENTION_DAYS or 14 (0 disables pruning)
  --loop                           Run continuously on a fixed interval for scheduler containers
  --interval-seconds <n>           Default: $POSTGRES_SNAPSHOT_INTERVAL_SECONDS or 86400
  --dry-run                        Print planned snapshot path only

Flags (db test-bootstrap-check):
  --database-url <url>             Default: $DATABASE_URL or constructed from postgres-* flags
  --postgres-host <host>           Default: $POSTGRES_HOST or localhost
  --postgres-port <port>           Default: $POSTGRES_PORT or 5432
  --postgres-db <db>               Default: $POSTGRES_DB or rising_intelligence
  --postgres-user <user>           Default: $POSTGRES_USER or rising
  --postgres-password <password>   Default: $POSTGRES_PASSWORD or secret file
  --required-tables <csv>          Optional CSV override for required table names
  --required-migrations <csv>      Optional CSV override for required migration directory names
  --migrations-dir <path>          Default: <repo>/packages/db/prisma/migrations
  --skip-migrations-check          Only validate required tables

Flags (brief trigger):
  --kafka-brokers <brokers>        Default: $KAFKA_BROKERS or localhost:9092
  --kafka-client-id <id>           Default: riops-brief-trigger
  --summary-requests-topic <name>  Default: $KAFKA_TOPIC_SUMMARY_REQUESTS or summary.requests
  --request-id <id>                Default: manual-<timestamp>
  --requested-at <iso8601>         Default: current UTC timestamp
  --type <daily|threshold>         Default: daily
  --windows <csv>                  Default: 2 (query mode requires window 2)
  --lookback-days <n>              Default: $BRIEF_DEFAULT_LOOKBACK_DAYS or 7 (applies to published_at in query mode)
  --max-lookback-days <n>          Default: $BRIEF_MAX_LOOKBACK_DAYS or 30
  --topic-globs <csv>              Default: * (example: aws.*,ai.*)
  --feed-config <path>             Repeatable; derive topic globs from feed YAML files
  --max-events-per-topic <n>       Default: $BRIEF_MAX_QUERY_EVENTS_PER_TOPIC or --max-evidence-per-topic
  --report-timezone <iana>         Optional notes framing timezone (example: America/New_York)
  --report-start-at <iso8601>      Optional notes framing start time
  --report-end-at <iso8601>        Optional notes framing end time
  --topic-key <key>                Optional (enables explicit mode; example: aws.bedrock)
  --evidence-url <url>             Optional with --topic-key (enables explicit mode)
  --evidence-source <source>       Default: rss
  --evidence-title <text>          Default: Manual summary request trigger
  --evidence-excerpt <text>        Default: generated text
  --score <n>                      Default: 8.5
  --volume <n>                     Default: 100
  --acceleration <n>               Default: 0.4
  --daily-budget-usd <n>           Default: $BRIEF_DAILY_BUDGET_USD or $LLM_DAILY_BUDGET_USD or 5
  --max-topics <n>                 Default: $BRIEF_MAX_TOPICS or 5 (caps top-level topic groups in query mode)
  --max-evidence-per-topic <n>     Default: $BRIEF_MAX_EVIDENCE_PER_TOPIC or 3
  --max-output-tokens <n>          Default: $BRIEF_MAX_OUTPUT_TOKENS or 1200
  --llm-provider <name>            Default: $LLM_PROVIDER or codex-cli
  --dry-run                        Print request payload without publishing

Flags (brief diagnose):
  --kafka-brokers <brokers>        Default: $KAFKA_BROKERS or localhost:9092
  --kafka-client-id <id>           Default: riops-brief-diagnose
  --summary-requests-topic <name>  Default: $KAFKA_TOPIC_SUMMARY_REQUESTS or summary.requests
  --summary-results-topic <name>   Default: $KAFKA_TOPIC_SUMMARY_RESULTS or summary.results
  --request-id <id>                Default: diagnose-<timestamp>
  --timeout <seconds>              Default: 120
  --brief-health-url <url>         Default: http://localhost:3005/health
  --lookback-days <n>              Default: 2
  --topic-globs <csv>              Default: *
  --llm-provider <name>            Default: $LLM_PROVIDER or codex-cli
  --skip-trigger                   Diagnose listeners/checks without publishing a request
  --skip-logs                      Skip docker-compose log collection on failures/timeouts
  --docker-compose-file <path>     Default: docker-compose.yml
  --docker-compose-project <name>  Optional compose project name for scoped logs
  --docker-service <name>          Default: brief
  --docker-logs-tail <n>           Default: 200
  --dry-run                        Print request payload without publishing

Flags (e2e brief-run):
  --compose-project <name>         Default: ri-brief-e2e
  --kafka-host-port <port>         Optional override for E2E Kafka host port
  --schema-registry-host-port <p>  Optional override for E2E Schema Registry host port
  --brief-host-port <port>         Optional override for E2E brief host port
  --redis-host-port <port>         Optional override for E2E Redis host port
  --postgres-host-port <port>      Optional override for E2E Postgres host port
  --mock-llm-host-port <port>      Optional override for E2E mock LLM host port
  --wait-timeout-ms <ms>           Optional override for E2E wait timeout
  --keep-up                        Keep E2E Compose stack running after command exits
  --dry-run                        Print resolved config without executing the script

Examples:
  riops schema-registry publish-protos
  riops schema-registry publish-protos --schema-registry-url http://localhost:8081
  SCHEMA_REGISTRY_URL=http://localhost:8081 riops schema-registry publish-protos
  riops kafka create-topics
  riops kafka topics
  riops kafka topics --kafka-brokers localhost:9092
  riops kafka ensure-topics
  riops kafka ensure-topics --kafka-brokers localhost:9093 --topics summary.requests,summary.results,trends.snapshots
  riops topics list
  riops topics list --counts
  riops topics list --counts --min-count 10
  riops topics retag --dry-run
  riops topics retag --source rss --limit 100 --dry-run
  riops topics retag --all --batch-size 500
  riops events enrich --dry-run
  riops events enrich --steps retag,quality --source rss --limit 100 --dry-run
  riops events enrich --missing-only --batch-size 500 --dry-run
  riops db snapshot
  riops db snapshot --output-dir ./backups/postgres --label manual --retention-days 30
  riops db snapshot --loop --interval-seconds 86400
  riops db test-bootstrap-check --postgres-port 5433 --postgres-db rising_intelligence_test
  riops brief trigger --dry-run
  riops brief trigger --lookback-days 7 --topic-globs "aws.*,ai.*" --dry-run
  riops brief trigger --report-timezone America/New_York --report-start-at 2026-01-01T00:00:00-05:00 --report-end-at 2026-02-10T23:59:59-05:00 --dry-run
  riops brief trigger --topic-key aws.bedrock --evidence-url https://example.com/bedrock
  riops brief diagnose --timeout 180
  riops brief diagnose --docker-compose-file docker-compose.test.yml --docker-compose-project ri-brief-e2e-a --docker-service brief-test
  riops e2e brief-run --compose-project ri-brief-e2e --keep-up
`);
}
