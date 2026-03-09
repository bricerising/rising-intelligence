import type { Logger } from "pino";
import { createServiceLogger, type LogLevel } from "./logger.js";

export interface ServiceBootstrapConfig {
  SERVICE_NAME: string;
  LOG_LEVEL: LogLevel;
  SHUTDOWN_TIMEOUT_MS: number;
}

export interface ServiceBootstrap<Config extends ServiceBootstrapConfig> {
  getConfig(): Config;
  getServiceName(): string;
  getShutdownTimeoutMs(): number;
  getLogger(): Logger;
  setRuntimeLogger(logger: Logger): void;
}

export type CreateLoggerFn = (name: string, level: LogLevel) => Logger;

/**
 * Factory for consistent service bootstrap concerns:
 * - lazy config loading
 * - bootstrap logger creation
 * - runtime logger handoff after initialization
 */
export function createServiceBootstrap<Config extends ServiceBootstrapConfig>(
  loadConfig: () => Config,
  createLogger: CreateLoggerFn = createServiceLogger
): ServiceBootstrap<Config> {
  let runtimeConfig: Config | null = null;
  let logger: Logger | null = null;

  function getConfig(): Config {
    if (!runtimeConfig) {
      runtimeConfig = loadConfig();
    }
    return runtimeConfig;
  }

  return {
    getConfig,
    getServiceName(): string {
      return getConfig().SERVICE_NAME;
    },
    getShutdownTimeoutMs(): number {
      return getConfig().SHUTDOWN_TIMEOUT_MS;
    },
    getLogger(): Logger {
      if (!logger) {
        const config = getConfig();
        logger = createLogger(config.SERVICE_NAME, config.LOG_LEVEL);
      }
      return logger;
    },
    setRuntimeLogger(runtimeLogger: Logger): void {
      logger = runtimeLogger;
    },
  };
}
