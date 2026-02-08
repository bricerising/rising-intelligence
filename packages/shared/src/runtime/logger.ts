import pino from "pino";
import { context, isSpanContextValid, trace } from "@opentelemetry/api";

export type LogLevel = "trace" | "debug" | "info" | "warn" | "error";

function getTraceBindings(): Record<string, string> {
  const span = trace.getSpan(context.active());
  if (!span) {
    return {};
  }

  const spanContext = span.spanContext();
  if (!isSpanContextValid(spanContext)) {
    return {};
  }

  return {
    traceId: spanContext.traceId,
    spanId: spanContext.spanId,
  };
}

export function createServiceLogger(name: string, level: LogLevel): pino.Logger {
  return pino({
    name,
    base: {
      service: name,
    },
    level,
    mixin: getTraceBindings,
    formatters: {
      level: (label) => ({ level: label }),
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
