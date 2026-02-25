import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { Logger } from "pino";

// ---------------------------------------------------------------------------
// Health server
// ---------------------------------------------------------------------------

export interface HealthHandlers {
  getHealth(): { status: "healthy" | "degraded" | "unhealthy"; body: unknown };
  isReady(): { ready: boolean; body: unknown };
  formatMetrics(): string;
}

export function createHealthHandler(handlers: HealthHandlers) {
  return (req: IncomingMessage, res: ServerResponse) => {
    if (req.method !== "GET") {
      res.writeHead(405);
      res.end("Method not allowed");
      return;
    }

    if (req.url === "/health" || req.url === "/healthz") {
      const { status, body } = handlers.getHealth();
      const statusCode = status === "unhealthy" ? 503 : 200;
      res.writeHead(statusCode, { "Content-Type": "application/json" });
      res.end(JSON.stringify(body, null, 2));
      return;
    }

    if (req.url === "/ready" || req.url === "/readyz") {
      const { ready, body } = handlers.isReady();
      res.writeHead(ready ? 200 : 503, { "Content-Type": "application/json" });
      res.end(JSON.stringify(body));
      return;
    }

    if (req.url === "/metrics") {
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
      res.end(handlers.formatMetrics());
      return;
    }

    res.writeHead(404);
    res.end("Not found");
  };
}

export function startHealthServer(port: number, handlers: HealthHandlers, logger: Logger): Server {
  const server = createServer(createHealthHandler(handlers));

  server.listen(port, () => {
    logger.info({ port }, "Health server started");
  });

  return server;
}

// ---------------------------------------------------------------------------
// Prometheus helpers
// ---------------------------------------------------------------------------

export function quoteMetricLabelValue(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll("\"", "\\\"")
    .replaceAll("\n", "\\n");
}

export function formatMetricLabels(labels: Record<string, string>): string {
  const entries = Object.entries(labels);
  if (entries.length === 0) {
    return "";
  }

  return `{${entries
    .map(([name, value]) => `${name}="${quoteMetricLabelValue(value)}"`)
    .join(",")}}`;
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

export interface HistogramState {
  buckets: number[];
  bucketCounts: number[];
  overflowCount: number;
  count: number;
  sum: number;
}

export function createHistogram(buckets: number[]): HistogramState {
  return {
    buckets,
    bucketCounts: new Array(buckets.length).fill(0) as number[],
    overflowCount: 0,
    count: 0,
    sum: 0,
  };
}

export function observeHistogram(histogram: HistogramState, value: number): void {
  if (!Number.isFinite(value) || value < 0) {
    return;
  }

  histogram.count++;
  histogram.sum += value;

  for (let i = 0; i < histogram.buckets.length; i++) {
    if (value <= histogram.buckets[i]) {
      histogram.bucketCounts[i]++;
      return;
    }
  }

  histogram.overflowCount++;
}

export function formatHistogram(
  name: string,
  help: string,
  histogram: HistogramState,
  extraLabels: Record<string, string> = {},
  includeMetadata = true
): string[] {
  const lines: string[] = [];
  if (includeMetadata) {
    lines.push(`# HELP ${name} ${help}`);
    lines.push(`# TYPE ${name} histogram`);
  }

  let cumulativeCount = 0;
  for (let i = 0; i < histogram.buckets.length; i++) {
    cumulativeCount += histogram.bucketCounts[i];
    const labels = formatMetricLabels({ ...extraLabels, le: histogram.buckets[i].toString() });
    lines.push(`${name}_bucket${labels} ${cumulativeCount}`);
  }

  const infLabels = formatMetricLabels({ ...extraLabels, le: "+Inf" });
  lines.push(`${name}_bucket${infLabels} ${histogram.count}`);

  const countLabels = formatMetricLabels(extraLabels);
  lines.push(`${name}_sum${countLabels} ${histogram.sum}`);
  lines.push(`${name}_count${countLabels} ${histogram.count}`);

  return lines;
}

export function getMaxConsumerLag(lagMap: Map<number, bigint>): bigint {
  let maxLag = 0n;
  for (const lag of lagMap.values()) {
    if (lag > maxLag) {
      maxLag = lag;
    }
  }
  return maxLag;
}
