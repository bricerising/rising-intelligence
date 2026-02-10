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
  lgtm urls                        Print local dev endpoints
  brief trigger                    Publish a manual SummaryRequest to Kafka

Flags (schema-registry publish-protos):
  --schema-registry-url <url>      Default: $SCHEMA_REGISTRY_URL or http://localhost:8081
  --compatibility <mode>           Default: $SCHEMA_COMPATIBILITY or BACKWARD
  --retries <n>                    Default: $SCHEMA_REGISTRY_RETRIES or 10
  --retry-initial-ms <ms>          Default: $SCHEMA_REGISTRY_RETRY_INITIAL_MS or 250
  --retry-max-ms <ms>              Default: $SCHEMA_REGISTRY_RETRY_MAX_MS or 5000
  --timeout-ms <ms>                Default: $SCHEMA_REGISTRY_TIMEOUT_MS or 8000
  --dry-run                        Print actions without calling Schema Registry

Flags (brief trigger):
  --kafka-brokers <brokers>        Default: $KAFKA_BROKERS or localhost:9092
  --kafka-client-id <id>           Default: riops-brief-trigger
  --summary-requests-topic <name>  Default: $KAFKA_TOPIC_SUMMARY_REQUESTS or summary.requests
  --request-id <id>                Default: manual-<timestamp>
  --requested-at <iso8601>         Default: current UTC timestamp
  --type <daily|threshold>         Default: daily
  --windows <csv>                  Default: 1,2
  --topic-key <key>                Required (example: aws.bedrock)
  --evidence-url <url>             Required
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
  riops brief trigger --topic-key aws.bedrock --evidence-url https://example.com/bedrock
`);
}
