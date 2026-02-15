export {
  CONTRACTS_PROTO_PATH,
  PROTO_ROOT,
  REPO_ROOT,
  SERVICES_PROTO_PATH,
  SHARED_PACKAGE_ROOT,
} from "./runtime/paths.js";

export const CONTRACTS_SUBJECT = "rising-intelligence.contracts.v1";
export const GRPC_SUBJECT = "grpc.rising-intelligence.v1";
export const CONTRACT_REFERENCE_NAME = "rising_intelligence/v1/contracts.proto";

export const KAFKA_VALUE_SUBJECTS = [
  "events.raw-value",
  "events.raw.dlq-value",
  "trends.snapshots-value",
  "summary.requests-value",
  "summary.results-value",
] as const;

export * from "./runtime/backoff.js";
export * from "./runtime/circuit-breaker.js";
export * from "./runtime/config.js";
export * from "./runtime/env.js";
export * from "./runtime/errors.js";
export * from "./runtime/health.js";
export * from "./runtime/http.js";
export * from "./runtime/kafka-batch.js";
export * from "./runtime/kafka.js";
export * from "./runtime/lifecycle.js";
export * from "./runtime/logger.js";
export * from "./runtime/raw-event.js";
export * from "./runtime/raw-event-enrichment.js";
export * from "./runtime/secrets.js";
export * from "./runtime/service-bootstrap.js";
export * from "./runtime/source.js";
export * from "./runtime/topic-router.js";
export * from "./runtime/topic-extraction.js";
export * from "./runtime/url.js";
