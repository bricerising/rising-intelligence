import pino from "pino";
import { getConfig } from "./config.js";

let cachedLogger: pino.Logger | null = null;

export function getLogger(): pino.Logger {
  if (!cachedLogger) {
    const config = getConfig();
    cachedLogger = pino({
      name: config.SERVICE_NAME,
      level: config.LOG_LEVEL,
      formatters: {
        level: (label) => ({ level: label }),
      },
      timestamp: pino.stdTimeFunctions.isoTime,
    });
  }

  return cachedLogger;
}

export function createChildLogger(bindings: Record<string, unknown>): pino.Logger {
  return getLogger().child(bindings);
}
