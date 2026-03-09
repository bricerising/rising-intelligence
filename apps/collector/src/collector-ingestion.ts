import type { CompiledAllowlist } from "@rising-intelligence/pipeline";
import type { Logger } from "pino";
import type { CheckpointStore } from "./checkpoint.js";
import {
  createCollectorEventProcessor,
  type CollectorEventProcessResult,
} from "./ingestion-pipeline.js";
import type { HealthContext } from "./health.js";
import type { CollectorIngestionPublisher } from "./publishing-facade.js";
import type { RawEvent, Source } from "./types.js";

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
  ingest(event: RawEvent): Promise<CollectorIngestionResult>;
}

export function createCollectorIngestion(
  input: CreateCollectorIngestionInput
): CollectorIngestion {
  const processor = createCollectorEventProcessor(input);

  return {
    async ingest(event: RawEvent): Promise<CollectorIngestionResult> {
      return processor.process(event);
    },
  };
}
