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
