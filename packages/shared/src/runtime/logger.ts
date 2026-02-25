import pino from "pino";
import { context, isSpanContextValid, trace } from "@opentelemetry/api";

export type LogLevel = "trace" | "debug" | "info" | "warn" | "error";

export interface ComponentLoggerFactory<TComponent extends string> {
  create(component: TComponent): pino.Logger;
}

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

class DefaultComponentLoggerFactory<TComponent extends string>
  implements ComponentLoggerFactory<TComponent>
{
  private readonly componentLoggers = new Map<TComponent, pino.Logger>();

  constructor(private readonly rootLogger: pino.Logger) {}

  create(component: TComponent): pino.Logger {
    const existing = this.componentLoggers.get(component);
    if (existing) {
      return existing;
    }

    const childLogger = this.rootLogger.child({ component });
    this.componentLoggers.set(component, childLogger);
    return childLogger;
  }
}

export function createComponentLoggerFactory<TComponent extends string>(
  rootLogger: pino.Logger
): ComponentLoggerFactory<TComponent> {
  return new DefaultComponentLoggerFactory(rootLogger);
}
