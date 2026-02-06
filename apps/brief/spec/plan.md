# Implementation Plan: Brief Service

## Overview

Build `apps/brief` as a Kafka consumer/producer that wraps all LLM interactions with structured output, budget enforcement, and evidence grounding.

## Architecture (High Level)

- Input: `summary.requests` (request includes window + top topics + evidence)
- LLM call: OpenAI/Anthropic via structured output (JSON mode)
- Output: `summary.results` (`BriefResult`)
- Persistence: Postgres (`brief_results`)
- Budget tracking: Redis

## Dependencies

```json
{
  "@rising-intelligence/db": "workspace:*",
  "@rising-intelligence/shared": "workspace:*",
  "kafkajs": "^2.x",
  "ioredis": "^5.x",
  "openai": "^4.x",
  "zod": "^3.x"
}
```

## Phases

### Phase 1: Message contracts + stub generator

- Kafka consumer/producer setup
- Implement `Brief` contract and a "no-LLM" deterministic brief for testing
- Postgres persistence
- Basic metrics

**Deliverables**:
- `src/index.ts` - service entry point
- `src/config.ts` - environment config
- `src/kafka/consumer.ts` - Kafka consumer
- `src/kafka/producer.ts` - Kafka producer
- `src/stub.ts` - stub brief generator (no LLM)
- `src/db/results.ts` - Postgres persistence

### Phase 2: LLM integration + structured output

- OpenAI client integration
- System prompt + user prompt templates
- JSON mode for structured output
- Zod validation of LLM response
- Error handling + retries

**Deliverables**:
- `src/llm/client.ts` - OpenAI client wrapper
- `src/llm/prompts.ts` - prompt templates
- `src/llm/schema.ts` - Zod schemas for output validation
- `src/generator.ts` - main brief generation logic

### Phase 3: Budget enforcement + context management

- Daily cost budget tracking in Redis
- Token estimation + cost calculation
- Evidence truncation for context limits
- Source diversity in evidence selection

**Deliverables**:
- `src/budget.ts` - budget tracking and enforcement
- `src/truncate.ts` - evidence truncation logic
- `src/tokens.ts` - token estimation

### Phase 4: Quality + observability

- Health check endpoints
- Metrics + traces
- Audit logging (prompt inputs, no secrets)
- Grafana dashboard

**Deliverables**:
- `src/health.ts` - `/healthz` and `/readyz`
- `src/audit.ts` - audit logging
- Grafana dashboard for brief metrics

## Key Implementation Details

### Consumer Loop

```typescript
async function run() {
  const consumer = kafka.consumer({ groupId: 'brief-generator' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'summary.requests' });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const request = deserialize<SummaryRequest>(message.value);
      await processRequest(request);
    },
  });
}

async function processRequest(request: SummaryRequest) {
  const span = tracer.startSpan('brief.generate');

  try {
    // Check budget
    const estimatedCost = estimateCost(request);
    if (!(await checkBudget(estimatedCost))) {
      await publishFailure(request, 'BUDGET_EXCEEDED', 'Daily budget exceeded');
      return;
    }

    // Generate brief
    const brief = await generateBrief(request);

    // Record spend
    await recordSpend(brief.meta.estimated_cost_usd);

    // Publish success
    await publishSuccess(request, brief);

    // Persist to Postgres
    await persistResult(request.request_id, 'success', brief);

  } catch (error) {
    log.error({ requestId: request.request_id, error }, 'Brief generation failed');
    await publishFailure(request, 'GENERATION_FAILED', error.message);
    await persistResult(request.request_id, 'failure', { error: error.message });
  } finally {
    span.end();
  }
}
```

### Prompt Building

```typescript
const SYSTEM_PROMPT = `
You are a technical intelligence analyst for a cloud/AI engineer...
[full system prompt from spec]
`;

function buildUserPrompt(request: SummaryRequest): string {
  const date = new Date().toLocaleDateString('en-US', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  const topicsSummary = request.topics
    .map((t, i) => `${i + 1}. ${t.topic} (score: ${t.metrics[0]?.score?.toFixed(1) ?? 'N/A'})`)
    .join('\n');

  const evidenceBlocks = request.topics.map(topic => {
    const header = `## ${topic.topic} (score: ${topic.metrics[0]?.score?.toFixed(1)}, volume: ${topic.metrics[0]?.volume}, acceleration: ${((topic.metrics[0]?.acceleration ?? 0) * 100).toFixed(0)}%)`;

    const items = topic.evidence.map((e, i) => `
[${i + 1}] ${e.title ?? 'Untitled'}
    Source: ${e.source} | Published: ${e.published_at ?? 'Unknown'}
    URL: ${e.url}
    Excerpt: ${e.text_excerpt}
    Engagement: ${e.engagement?.score ?? 'N/A'}
`).join('');

    return `${header}\n\nEvidence items:\n${items}`;
  }).join('\n---\n\n');

  return `
Generate a ${request.type.toLowerCase()} briefing for ${date}.

TOP TRENDING TOPICS (by score):
${topicsSummary}

EVIDENCE BY TOPIC:

${evidenceBlocks}

Generate the briefing now. Remember: cite sources, be concise, focus on actionable insights.
`;
}
```

### LLM Call with Structured Output

```typescript
import OpenAI from 'openai';
import { z } from 'zod';

const BriefOutputSchema = z.object({
  title: z.string(),
  highlights: z.array(z.object({
    topic: z.string(),
    what_happened: z.string(),
    why_it_matters: z.string(),
    suggested_action: z.string(),
    citations: z.array(z.string()),
  })),
  notes: z.string().optional(),
});

async function callLLM(systemPrompt: string, userPrompt: string): Promise<z.infer<typeof BriefOutputSchema>> {
  const openai = new OpenAI({ apiKey: config.OPENAI_API_KEY });

  const response = await openai.chat.completions.create({
    model: config.LLM_MODEL,
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt },
    ],
    response_format: { type: 'json_object' },
    max_tokens: config.LLM_MAX_OUTPUT_TOKENS,
    temperature: 0.3, // Lower temperature for more consistent output
  });

  const content = response.choices[0].message.content;
  if (!content) {
    throw new Error('Empty response from LLM');
  }

  const parsed = JSON.parse(content);
  return BriefOutputSchema.parse(parsed);
}
```

### Budget Tracking

```typescript
interface BudgetState {
  spentUsd: number;
  requestsCount: number;
}

async function checkBudget(estimatedCost: number): Promise<boolean> {
  const today = new Date().toISOString().slice(0, 10);
  const key = `budget:${today}`;

  const state = await redis.hgetall(key);
  const spent = parseFloat(state.spentUsd ?? '0');

  if (spent + estimatedCost > config.LLM_DAILY_BUDGET_USD) {
    metrics.increment('ri_brief_budget_exceeded_total');
    return false;
  }

  return true;
}

async function recordSpend(cost: number): Promise<void> {
  const today = new Date().toISOString().slice(0, 10);
  const key = `budget:${today}`;

  await redis.hincrbyfloat(key, 'spentUsd', cost);
  await redis.hincrby(key, 'requestsCount', 1);
  await redis.expire(key, 7 * 86400); // 7 days

  metrics.increment('ri_brief_llm_cost_usd_total', cost);
}
```

### Evidence Truncation

```typescript
function truncateEvidence(request: SummaryRequest, maxTokens: number): SummaryRequest {
  const truncated = { ...request, topics: [...request.topics] };
  let tokens = estimateTokens(truncated);

  while (tokens > maxTokens) {
    // Strategy 1: Reduce evidence per topic
    for (const topic of truncated.topics) {
      if (topic.evidence.length > 2) {
        // Keep highest engagement items
        topic.evidence.sort((a, b) =>
          (b.engagement?.score ?? 0) - (a.engagement?.score ?? 0)
        );
        topic.evidence = topic.evidence.slice(0, topic.evidence.length - 1);
      }
    }

    const newTokens = estimateTokens(truncated);
    if (newTokens === tokens) {
      // Strategy 2: Truncate excerpts
      for (const topic of truncated.topics) {
        for (const item of topic.evidence) {
          if (item.text_excerpt.length > 200) {
            item.text_excerpt = item.text_excerpt.slice(0, 200) + '...';
          }
        }
      }
      break;
    }
    tokens = newTokens;
  }

  return truncated;
}

function estimateTokens(request: SummaryRequest): number {
  // Rough estimation: ~4 chars per token
  const json = JSON.stringify(request);
  return Math.ceil(json.length / 4);
}
```

## Testing Strategy

### Unit Tests

- Prompt building: various inputs → correct prompts
- Schema validation: valid/invalid LLM outputs
- Budget tracking: concurrent access, edge cases
- Token estimation: accuracy within 20%

### Integration Tests

- Full request cycle with mock LLM
- Budget enforcement across multiple requests
- Postgres persistence verification

### Acceptance Tests

- Real LLM test: generate brief, verify citations exist
- Budget exhaustion: verify graceful degradation
- 3-day soak: verify budget stays within limits

## Monitoring

### Metrics

- `ri_brief_generation_total{status=success|failure|skipped}`
- `ri_brief_llm_cost_usd_total`
- `ri_brief_generation_duration_seconds`
- `ri_brief_llm_tokens_total{direction=input|output}`
- `ri_brief_budget_exceeded_total`
- `ri_brief_errors_total{error_type=...}`

### Alerts

- Daily spend > 80% of budget
- Failure rate > 20%
- P99 latency > 120s
