# Feature Specification: Brief Service (LLM Summarizer)

**Service**: `@rising-intelligence/brief`
**Created**: 2026-02-05
**Updated**: 2026-02-05
**Status**: Planned

## Overview

The Brief Service consumes summary requests and produces evidence-grounded briefs:

- **Daily brief**: scheduled summary of top trends with citations and suggested actions.
- **Flash brief** (optional): short summary when a topic spikes.

The service is intentionally isolated so LLM latency/failures do not impact ingestion or trend computation.

## User Scenarios & Testing

### User Story 1 — Daily brief (Priority: P1)

As an operator, I receive a daily brief that explains what happened and what I should do next.

**Independent Test**: Trigger a daily brief request and verify the resulting brief contains citations for each trend.

**Acceptance Scenarios**:

1. **Given** Top N trends and evidence, **When** a daily request arrives, **Then** a success `BriefResult` is published to `summary.results`.
2. **Given** the LLM call fails, **When** retried within budget, **Then** the service recovers and emits a failure record if it ultimately cannot produce a brief.
3. **Given** a configured daily budget, **When** multiple requests arrive, **Then** the service enforces the budget (drops/degrades gracefully).

### User Story 2 — Evidence grounding (Priority: P1)

As an operator, I want every claim in the brief to be backed by evidence, so I can verify information.

**Independent Test**: Generate a brief and verify each highlight has at least one citation URL.

**Acceptance Scenarios**:

1. **Given** evidence items in the request, **When** brief is generated, **Then** each highlight includes at least one citation.
2. **Given** a topic with no evidence, **When** brief is generated, **Then** the topic is either skipped or clearly marked as "no sources available".

### Edge Cases

- Evidence set contains duplicates or near-duplicates.
- Evidence is missing (no curated sources for a topic).
- Model returns hallucinated facts → require grounding and "say uncertain" behavior.
- Context window exceeded → truncate evidence intelligently.

## Constitution Requirements

- **Grounding**: every highlight MUST include citations.
- **Budgeting**: enforce daily cost/token budgets.
- **Safety**: do not emit secrets; redact sensitive content if configured.
- **Honesty**: if uncertain, say so; never fabricate sources.

## Requirements

### Functional Requirements

- **FR-001**: Service MUST consume `summary.requests` from Kafka.
- **FR-002**: Service MUST produce `BriefResult` messages to `summary.results` (success or failure).
- **FR-003**: Service MUST include citations for each highlight.
- **FR-004**: Service SHOULD store the prompt inputs/metadata for audit (without leaking secrets).
- **FR-005 (Read model)**: Service MUST persist each produced result to Postgres (`brief_results`).
- **FR-006**: Service MUST enforce daily cost budget and degrade gracefully if exceeded.
- **FR-007**: Service MUST use structured output (JSON mode or function calling) for reliable parsing.

### Non-Functional Requirements

- **NFR-001**: Daily brief SHOULD complete within 2 minutes (LLM-dependent).
- **NFR-002**: Failures MUST be observable (metrics + logs + traces).
- **NFR-003**: Service MUST handle context window limits gracefully.

## LLM Prompt Design

### System Prompt

```
You are a technical intelligence analyst for a cloud/AI engineer. Your job is to produce concise, actionable briefings about trending technology topics.

RULES:
1. ONLY use information from the provided evidence. Do not make up facts.
2. Every claim MUST be backed by at least one citation [1], [2], etc.
3. If evidence is insufficient, say "Limited coverage" - do not speculate.
4. Focus on WHY something matters to a practicing engineer, not just WHAT happened.
5. Suggested actions should be specific and practical (e.g., "Read the AWS blog post", "Test the new API", "Update your dependencies").
6. Keep each section concise: 2-3 sentences for what_happened, 1-2 for why_it_matters, 1 for suggested_action.
7. Use technical language appropriate for a senior engineer audience.

OUTPUT FORMAT:
You MUST respond with valid JSON matching this schema:
{
  "title": "Brief title summarizing the day",
  "highlights": [
    {
      "topic": "topic.key",
      "what_happened": "Summary of events [1][2]",
      "why_it_matters": "Impact on engineers [1]",
      "suggested_action": "Concrete next step",
      "citations": ["https://...", "https://..."]
    }
  ],
  "notes": "Any caveats about coverage gaps or limitations"
}
```

### User Prompt Template

```
Generate a {brief_type} briefing for {date}.

TOP TRENDING TOPICS (by score):
{topics_summary}

EVIDENCE BY TOPIC:

## {topic.display_name} (score: {score}, volume: {volume}, acceleration: {acceleration}%)

Evidence items:
{for each evidence_item}
[{index}] {title}
    Source: {source} | Published: {published_at}
    URL: {url}
    Excerpt: {text_excerpt}
    Engagement: {engagement_score}
{end for}

---

{repeat for each topic}

Generate the briefing now. Remember: cite sources, be concise, focus on actionable insights.
```

### Few-Shot Example (included in system prompt for consistency)

```json
{
  "title": "Tech Brief: Feb 5, 2026 - Bedrock Updates & RAG Patterns",
  "highlights": [
    {
      "topic": "aws.bedrock",
      "what_happened": "AWS announced new Bedrock model availability including Claude 3 Opus and expanded region support [1]. The community noted improved latency in production workloads [2][3].",
      "why_it_matters": "If you're using Bedrock in production, the new regions may reduce latency for non-US workloads. Claude 3 Opus offers stronger reasoning for complex tasks [1].",
      "suggested_action": "Review the region availability table and consider migrating latency-sensitive workloads.",
      "citations": [
        "https://aws.amazon.com/blogs/aws/bedrock-update",
        "https://reddit.com/r/aws/comments/...",
        "https://news.ycombinator.com/item?id=..."
      ]
    }
  ],
  "notes": "Coverage is weighted toward Reddit and HN discussions today; RSS ingestion was delayed."
}
```

## Context Window Management

### Token Budget Allocation

For a 128K context model (e.g., GPT-4 Turbo, Claude 3):

| Component | Token Budget | Notes |
|-----------|--------------|-------|
| System prompt | ~500 | Fixed |
| Few-shot example | ~400 | Fixed |
| Evidence per topic | ~1000 | Variable, truncate if needed |
| Topics (max 10) | ~10000 | Total evidence budget |
| Output buffer | ~2000 | Reserved for response |
| **Total** | ~13000 | Well under 128K |

### Evidence Truncation Strategy

When evidence exceeds budget:

1. **Priority 1**: Keep highest-engagement items
2. **Priority 2**: Ensure source diversity (at least 1 curated, 1 discussion)
3. **Priority 3**: Truncate `text_excerpt` to 200 chars
4. **Priority 4**: Reduce evidence items per topic (min 2)

```typescript
function truncateEvidence(
  topics: TopicBriefInput[],
  maxTokens: number
): TopicBriefInput[] {
  let totalTokens = estimateTokens(topics);

  while (totalTokens > maxTokens) {
    // Find topic with most evidence
    const largest = topics.reduce((a, b) =>
      a.evidence.length > b.evidence.length ? a : b
    );

    if (largest.evidence.length <= 2) {
      // Can't reduce further, truncate excerpts
      for (const topic of topics) {
        for (const item of topic.evidence) {
          item.text_excerpt = item.text_excerpt.slice(0, 200);
        }
      }
      break;
    }

    // Remove lowest-engagement item
    largest.evidence.sort((a, b) =>
      (b.engagement?.score ?? 0) - (a.engagement?.score ?? 0)
    );
    largest.evidence.pop();

    totalTokens = estimateTokens(topics);
  }

  return topics;
}
```

## Budget Enforcement

### Daily Budget Tracking

```typescript
interface DailyBudget {
  date: string; // YYYY-MM-DD
  budgetUsd: number;
  spentUsd: number;
  requestsCount: number;
}

async function checkBudget(estimatedCost: number): Promise<boolean> {
  const today = new Date().toISOString().slice(0, 10);
  const budget = await redis.hgetall(`budget:${today}`);

  const spent = parseFloat(budget.spentUsd ?? '0');
  const limit = parseFloat(budget.budgetUsd ?? config.LLM_DAILY_BUDGET_USD);

  if (spent + estimatedCost > limit) {
    log.warn({ spent, limit, estimatedCost }, 'Daily budget exceeded');
    metrics.increment('brief_budget_exceeded_total');
    return false;
  }

  return true;
}

async function recordSpend(actualCost: number): Promise<void> {
  const today = new Date().toISOString().slice(0, 10);
  await redis.hincrbyfloat(`budget:${today}`, 'spentUsd', actualCost);
  await redis.hincrby(`budget:${today}`, 'requestsCount', 1);
  await redis.expire(`budget:${today}`, 7 * 24 * 60 * 60); // 7 days
}
```

### Cost Estimation

```typescript
function estimateCost(inputTokens: number, outputTokens: number): number {
  // GPT-4 Turbo pricing (example)
  const inputCostPer1K = 0.01;
  const outputCostPer1K = 0.03;

  return (
    (inputTokens / 1000) * inputCostPer1K +
    (outputTokens / 1000) * outputCostPer1K
  );
}
```

## Output Parsing

### Structured Output with Validation

```typescript
import { z } from 'zod';

const BriefHighlightSchema = z.object({
  topic: z.string(),
  what_happened: z.string().min(10).max(500),
  why_it_matters: z.string().min(10).max(300),
  suggested_action: z.string().min(5).max(200),
  citations: z.array(z.string().url()).min(1),
});

const BriefOutputSchema = z.object({
  title: z.string().min(5).max(100),
  highlights: z.array(BriefHighlightSchema).min(1),
  notes: z.string().optional(),
});

async function generateBrief(request: SummaryRequest): Promise<Brief> {
  const prompt = buildPrompt(request);

  // Use JSON mode for structured output
  const response = await openai.chat.completions.create({
    model: config.LLM_MODEL,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user', content: prompt },
    ],
    response_format: { type: 'json_object' },
    max_tokens: config.LLM_MAX_OUTPUT_TOKENS,
  });

  const content = response.choices[0].message.content;
  const parsed = JSON.parse(content);

  // Validate against schema
  const validated = BriefOutputSchema.parse(parsed);

  return {
    brief_id: generateId(),
    generated_at: new Date().toISOString(),
    window: request.type === 'DAILY' ? 'DAILY' : 'THRESHOLD',
    title: validated.title,
    highlights: validated.highlights.map(h => ({
      topic: h.topic,
      why_it_matters: h.why_it_matters,
      what_happened: h.what_happened,
      suggested_action: h.suggested_action,
      citations: h.citations,
    })),
    notes: validated.notes,
    meta: {
      provider: 'openai',
      model: config.LLM_MODEL,
      input_tokens: response.usage.prompt_tokens,
      output_tokens: response.usage.completion_tokens,
      estimated_cost_usd: estimateCost(
        response.usage.prompt_tokens,
        response.usage.completion_tokens
      ),
    },
  };
}
```

## Success Criteria

- **SC-001**: Briefs are consistently actionable and evidence-grounded.
- **SC-002**: Spend stays within configured budget in 3-day local soak.
- **SC-003**: Every highlight has at least one valid citation URL.
- **SC-004**: Output parsing never fails on valid LLM responses.

## Configuration

```
# Required
KAFKA_BROKERS=localhost:9092
DATABASE_URL=postgresql://user:pass@localhost:5432/rising_intelligence
REDIS_URL=redis://localhost:6379

# LLM Configuration
LLM_PROVIDER=openai
LLM_MODEL=gpt-4-turbo-preview
OPENAI_API_KEY=sk-...

# Budget
LLM_DAILY_BUDGET_USD=1.00
LLM_MAX_TOPICS_PER_BRIEF=10
LLM_MAX_EVIDENCE_PER_TOPIC=5
LLM_MAX_OUTPUT_TOKENS=2000

# Retry
LLM_MAX_RETRIES=3
LLM_RETRY_DELAY_MS=1000
```
