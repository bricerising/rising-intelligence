import http from "node:http";

const PORT = Number.parseInt(process.env.PORT ?? "8080", 10);
const LATENCY_MS = Number.parseInt(process.env.MOCK_LLM_LATENCY_MS ?? "0", 10);
const RESPONSE_MODE = (process.env.MOCK_LLM_RESPONSE_MODE ?? "success").trim().toLowerCase();
const MODEL = (process.env.MOCK_LLM_MODEL ?? "mock-llm-v1").trim();

function sendJson(res, statusCode, body) {
  const payload = JSON.stringify(body);
  res.writeHead(statusCode, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

function estimateTokenCount(text) {
  return Math.max(1, Math.ceil(text.length / 4));
}

function dedupeCitations(values) {
  const unique = new Set();
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }
    const trimmed = value.trim();
    if (trimmed.length === 0) {
      continue;
    }
    unique.add(trimmed);
  }
  return [...unique].slice(0, 3);
}

function getPrimaryMetric(topic) {
  if (!topic || !Array.isArray(topic.metrics)) {
    return null;
  }
  return topic.metrics.find((metric) => metric?.window === 2) ?? topic.metrics[0] ?? null;
}

function buildHighlight(topic) {
  const topicName = typeof topic?.topic === "string" ? topic.topic.trim() : "unknown.topic";
  const primaryMetric = getPrimaryMetric(topic);
  const score = Number.isFinite(primaryMetric?.score) ? primaryMetric.score.toFixed(1) : "0.0";
  const volume = Number.isFinite(primaryMetric?.volume)
    ? Math.round(primaryMetric.volume)
    : Array.isArray(topic?.evidence)
      ? topic.evidence.length
      : 0;
  const acceleration = Number.isFinite(primaryMetric?.acceleration)
    ? primaryMetric.acceleration.toFixed(2)
    : "0.00";
  const citations = dedupeCitations((topic?.evidence ?? []).map((item) => item?.url));

  return {
    topic: topicName,
    what_happened: `${topicName} reached score ${score} with volume ${volume} and acceleration ${acceleration}.`,
    why_it_matters:
      citations.length > 0
        ? `${topicName} continues to gather evidence-backed momentum.`
        : `${topicName} has early momentum but sparse evidence.`,
    suggested_action:
      citations.length > 0
        ? `Validate ${citations[0]} against current priorities.`
        : "Collect more evidence before taking action.",
    citations,
  };
}

function buildResponse(input) {
  const topics = Array.isArray(input?.topics) ? input.topics : [];
  const maxTopicsRaw = input?.budget?.max_topics;
  const maxTopics =
    Number.isInteger(maxTopicsRaw) && maxTopicsRaw > 0 ? Math.min(maxTopicsRaw, topics.length) : topics.length;
  const selectedTopics = topics.slice(0, maxTopics);
  const highlights = selectedTopics.map((topic) => buildHighlight(topic));
  const notes = highlights.some((highlight) => highlight.citations.length === 0)
    ? "Limited coverage for one or more topics."
    : "All highlights include source citations.";

  const promptTokens = estimateTokenCount(JSON.stringify(input));
  const completionTokens = estimateTokenCount(JSON.stringify(highlights));
  const estimatedCostUsd = Number((Math.max(0.005, (promptTokens + completionTokens) * 0.00002)).toFixed(4));

  return {
    title: `Mock Trend Brief ${(new Date()).toISOString().slice(0, 10)}`,
    highlights,
    notes,
    usage: {
      prompt_tokens: promptTokens,
      completion_tokens: completionTokens,
    },
    meta: {
      provider: "mock-llm",
      model: MODEL,
      estimated_cost_usd: estimatedCostUsd,
    },
  };
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString("utf-8");
  if (raw.trim().length === 0) {
    return {};
  }
  return JSON.parse(raw);
}

function withLatency(fn) {
  if (LATENCY_MS <= 0) {
    fn();
    return;
  }
  setTimeout(fn, LATENCY_MS);
}

const server = http.createServer(async (req, res) => {
  if (req.url === "/health" && req.method === "GET") {
    sendJson(res, 200, { status: "ok" });
    return;
  }

  if (req.url === "/v1/generate" && req.method === "POST") {
    let input;
    try {
      input = await readJsonBody(req);
    } catch {
      sendJson(res, 400, { error: "invalid_json" });
      return;
    }

    if (RESPONSE_MODE !== "success") {
      withLatency(() => sendJson(res, 503, { error: "mock_failure" }));
      return;
    }

    const responsePayload = buildResponse(input);
    withLatency(() => sendJson(res, 200, responsePayload));
    return;
  }

  sendJson(res, 404, { error: "not_found" });
});

server.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`[mock-llm] listening on ${PORT}`);
});
