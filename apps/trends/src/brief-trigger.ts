import type pino from "pino";
import type { Config } from "./config.js";
import type { HealthContext } from "./health.js";
import type { PublishedWindowSnapshot } from "./snapshot.js";

interface TriggerContext {
  config: Config;
  logger: pino.Logger;
  healthContext: HealthContext;
  snapshots: PublishedWindowSnapshot[];
  lastDailyTriggerDate: string | null;
  now?: Date;
}

// Request-driven architecture: Trends must not auto-publish summary requests.
export async function maybeTriggerDailySummaryRequest(ctx: TriggerContext): Promise<string | null> {
  const now = ctx.now ?? new Date();
  ctx.logger.debug(
    {
      snapshotCount: ctx.snapshots.length,
      at: now.toISOString(),
      mode: "request-driven",
    },
    "Automatic brief triggering is disabled"
  );
  return ctx.lastDailyTriggerDate;
}
