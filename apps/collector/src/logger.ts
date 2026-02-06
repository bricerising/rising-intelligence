import pino from "pino";
import { getConfig } from "./config.js";

let _logger: pino.Logger | null = null;

export function getLogger(): pino.Logger {
  if (!_logger) {
    const config = getConfig();
    _logger = pino({
      name: config.SERVICE_NAME,
      level: config.LOG_LEVEL,
      formatters: {
        level: (label) => ({ level: label }),
      },
      timestamp: pino.stdTimeFunctions.isoTime,
    });
  }
  return _logger;
}

export function createChildLogger(bindings: Record<string, unknown>): pino.Logger {
  return getLogger().child(bindings);
}
