export function printHelp(errorMessage?: string) {
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
  schema-registry publish-protos   Publish protobuf schemas + subjects
  kafka create-topics              Create required Kafka topics
  kafka topics                     List all Kafka topics
  lgtm urls                        Print local dev endpoints
  brief trigger                    Publish a manual SummaryRequest to Kafka
  topics list                      List distinct topics from raw_events table

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

Flags (topics list):
  --database-url <url>             Default: $DATABASE_URL or constructed from postgres-* flags
  --postgres-host <host>           Default: $POSTGRES_HOST or localhost
  --postgres-port <port>           Default: $POSTGRES_PORT or 5432
  --postgres-db <db>               Default: $POSTGRES_DB or rising_intelligence
  --postgres-user <user>           Default: $POSTGRES_USER or rising
  --postgres-password <password>   Default: $POSTGRES_PASSWORD or secret file
  --counts                         Show event count per topic (sorted by count desc)
  --min-count <n>                  Minimum event count to include (requires --counts)

Flags (brief trigger):
  --kafka-brokers <brokers>        Default: $KAFKA_BROKERS or localhost:9092
  --kafka-client-id <id>           Default: riops-brief-trigger
  --summary-requests-topic <name>  Default: $KAFKA_TOPIC_SUMMARY_REQUESTS or summary.requests
  --request-id <id>                Default: manual-<timestamp>
  --requested-at <iso8601>         Default: current UTC timestamp
  --type <daily|threshold>         Default: daily
  --windows <csv>                  Default: 2 (query mode requires window 2)
  --lookback-days <n>              Default: $BRIEF_DEFAULT_LOOKBACK_DAYS or 7
  --max-lookback-days <n>          Default: $BRIEF_MAX_LOOKBACK_DAYS or 30
  --topic-globs <csv>              Default: * (example: aws.*,ai.*)
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
  --max-topics <n>                 Default: $BRIEF_MAX_TOPICS or 5
  --max-evidence-per-topic <n>     Default: $BRIEF_MAX_EVIDENCE_PER_TOPIC or 3
  --max-output-tokens <n>          Default: $BRIEF_MAX_OUTPUT_TOKENS or 1200
  --dry-run                        Print request payload without publishing

Examples:
  riops schema-registry publish-protos
  riops schema-registry publish-protos --schema-registry-url http://localhost:8081
  SCHEMA_REGISTRY_URL=http://localhost:8081 riops schema-registry publish-protos
  riops kafka create-topics
  riops kafka topics
  riops kafka topics --kafka-brokers localhost:9092
  riops topics list
  riops topics list --counts
  riops topics list --counts --min-count 10
  riops brief trigger --dry-run
  riops brief trigger --lookback-days 7 --topic-globs "aws.*,ai.*" --dry-run
  riops brief trigger --report-timezone America/New_York --report-start-at 2026-01-01T00:00:00-05:00 --report-end-at 2026-02-10T23:59:59-05:00 --dry-run
  riops brief trigger --topic-key aws.bedrock --evidence-url https://example.com/bedrock
`);
}
