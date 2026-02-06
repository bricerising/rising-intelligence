import type { Logger } from "pino";
import { serializeError } from "./errors.js";

export interface ServiceDefinition<TCtx> {
  name: string;
  initialize(): Promise<TCtx>;
  run(ctx: TCtx): Promise<void>;
  shutdown(ctx: TCtx): Promise<void>;
  getLogger(): Logger;
  shutdownTimeoutMs: number;
}

export function runService<TCtx>(def: ServiceDefinition<TCtx>): void {
  let context: TCtx | null = null;
  let shuttingDown = false;

  const requestShutdown = async (exitCode: number) => {
    if (shuttingDown) return;
    shuttingDown = true;

    if (context === null) {
      process.exit(exitCode);
    }

    const logger = def.getLogger();
    logger.info(`Shutting down ${def.name} service`);

    const timeout = setTimeout(() => {
      logger.error({ timeoutMs: def.shutdownTimeoutMs }, "Shutdown timeout reached");
      process.exit(1);
    }, def.shutdownTimeoutMs);

    let finalExitCode = exitCode;
    try {
      await def.shutdown(context);
    } catch (error) {
      finalExitCode = 1;
      logger.error({ error: serializeError(error) }, `${def.name} shutdown failed`);
    } finally {
      clearTimeout(timeout);
    }

    process.exit(finalExitCode);
  };

  process.on("SIGTERM", () => {
    void requestShutdown(0);
  });

  process.on("SIGINT", () => {
    void requestShutdown(0);
  });

  process.on("uncaughtException", (error) => {
    def.getLogger().error({ error: serializeError(error) }, "Uncaught exception");
    void requestShutdown(1);
  });

  process.on("unhandledRejection", (reason) => {
    def.getLogger().error({ error: serializeError(reason) }, "Unhandled rejection");
    void requestShutdown(1);
  });

  async function start() {
    try {
      context = await def.initialize();
      await def.run(context);
    } catch (error) {
      def.getLogger().fatal({ error: serializeError(error) }, `Fatal error in ${def.name}`);
      await requestShutdown(1);
    }
  }

  void start();
}
