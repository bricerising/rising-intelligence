import pino from "pino";

export type LogLevel = "trace" | "debug" | "info" | "warn" | "error";

export function createServiceLogger(name: string, level: LogLevel): pino.Logger {
  return pino({
    name,
    level,
    formatters: {
      level: (label) => ({ level: label }),
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
