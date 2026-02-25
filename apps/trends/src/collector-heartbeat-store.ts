import type { CollectorHeartbeatState } from "./health.js";

export type RecordCollectorHeartbeatResult = "stored" | "ignored_stale";

export function recordCollectorHeartbeat(
  heartbeatsBySource: Map<string, CollectorHeartbeatState>,
  nextHeartbeat: CollectorHeartbeatState
): RecordCollectorHeartbeatResult {
  const currentHeartbeat = heartbeatsBySource.get(nextHeartbeat.source);
  if (
    currentHeartbeat
    && nextHeartbeat.timestamp.getTime() < currentHeartbeat.timestamp.getTime()
  ) {
    return "ignored_stale";
  }

  heartbeatsBySource.set(nextHeartbeat.source, nextHeartbeat);
  return "stored";
}
