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

Flags (schema-registry publish-protos):
  --schema-registry-url <url>      Default: $SCHEMA_REGISTRY_URL or http://localhost:8081
  --compatibility <mode>           Default: $SCHEMA_COMPATIBILITY or BACKWARD
  --retries <n>                    Default: $SCHEMA_REGISTRY_RETRIES or 10
  --retry-initial-ms <ms>          Default: $SCHEMA_REGISTRY_RETRY_INITIAL_MS or 250
  --retry-max-ms <ms>              Default: $SCHEMA_REGISTRY_RETRY_MAX_MS or 5000
  --timeout-ms <ms>                Default: $SCHEMA_REGISTRY_TIMEOUT_MS or 8000
  --dry-run                        Print actions without calling Schema Registry

Examples:
  riops schema-registry publish-protos
  riops schema-registry publish-protos --schema-registry-url http://localhost:8081
  SCHEMA_REGISTRY_URL=http://localhost:8081 riops schema-registry publish-protos
`);
}
