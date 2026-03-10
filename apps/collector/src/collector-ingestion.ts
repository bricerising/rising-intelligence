import type { CompiledAllowlist } from "@rising-intelligence/pipeline";
import type { Logger } from "pino";
import type { CheckpointStore } from "./checkpoint.js";
import {
  createCollectorIngestionJob,
  createCollectorEventProcessor,
  type CollectorEventProcessResult,
} from "./ingestion-pipeline.js";
import type { HealthContext } from "./health.js";
import type { CollectorIngestionPublisher } from "./publishing-facade.js";
import type { CollectorIngestionEvent, Source } from "./types.js";

export type CollectorIngestionResult = CollectorEventProcessResult;

export interface CreateCollectorIngestionInput {
  adapterName: string;
  adapterSource: Source;
  allowlist: CompiledAllowlist;
  checkpointStore: Pick<CheckpointStore, "hasSeen" | "markSeen">;
  healthContext: HealthContext;
  logger: Logger;
  publisher: CollectorIngestionPublisher;
  now?: () => Date;
  generateDlqId?: () => string;
}

export interface CollectorIngestion {
  ingest(event: CollectorIngestionEvent): Promise<CollectorIngestionResult>;
}

export function createCollectorIngestion(
  input: CreateCollectorIngestionInput
): CollectorIngestion {
  const processor = createCollectorEventProcessor(input);

  return {
    async ingest(event: CollectorIngestionEvent): Promise<CollectorIngestionResult> {
      return processor.execute(createCollectorIngestionJob(event));
    },
  };
}
