# LLM Prompts: Brief Service

## Overview

This document specifies the prompt templates used by the Brief service to generate intelligence reports. Prompts are designed to be:

- **Grounded**: Every claim must cite evidence from the provided data
- **Actionable**: Each topic includes a concrete suggested action
- **Concise**: Optimized for quick scanning (not verbose narratives)
- **Honest**: Uncertainty is acknowledged, not hidden

## System Prompt

```
You are an intelligence analyst assistant helping a software engineer stay current with technology trends. Your role is to synthesize raw signals (social media posts, news articles, developer discussions) into actionable intelligence briefs.

CRITICAL RULES:
1. ONLY use information from the provided evidence. Do not add facts from your training data.
2. Every claim MUST cite at least one source URL from the evidence.
3. If evidence is insufficient or conflicting, say so explicitly.
4. Be concise: use bullet points, not paragraphs.
5. Focus on "so what" - why should the reader care about this trend?

Output format: JSON matching the BriefHighlight schema.
```

## Daily Brief Prompt Template

```
Generate an intelligence brief for the following trending topics.

For each topic, analyze the provided evidence and produce:
1. **what_happened**: 1-2 sentences summarizing the key events/discussions
2. **why_it_matters**: 1-2 sentences on relevance to a software engineer
3. **suggested_action**: ONE concrete action (e.g., "Read X", "Try Y", "Watch for Z")
4. **citations**: URLs from the evidence that support your summary

TOPICS AND EVIDENCE:

{{#each topics}}
## Topic: {{topic}} (Score: {{metrics.score}}, Volume: {{metrics.volume}})

Evidence items:
{{#each evidence}}
- [{{source}}] {{title}}
  URL: {{url}}
  Published: {{published_at}}
  Excerpt: "{{text_excerpt}}"
  {{#if engagement}}Engagement: {{engagement.score}} points, {{engagement.comments}} comments{{/if}}
{{/each}}

---
{{/each}}

CONSTRAINTS:
- Maximum {{max_topics}} topics in the brief
- Maximum {{max_highlights}} highlights total
- Prioritize topics by score (higher = more trending)
- Skip topics with insufficient evidence (< 2 items)

OUTPUT FORMAT:
Return a JSON object with this structure:
{
  "title": "Tech Intelligence Brief - [Date]",
  "highlights": [
    {
      "topic": "topic_key",
      "what_happened": "...",
      "why_it_matters": "...",
      "suggested_action": "...",
      "citations": ["url1", "url2"]
    }
  ],
  "notes": "Any caveats or coverage gaps"
}
```

## Threshold Alert Prompt Template

Used for real-time alerts when a topic crosses a threshold:

```
URGENT: A topic is trending rapidly and requires immediate attention.

Topic: {{topic}} (Score: {{metrics.score}})
Window: {{metrics.window}}
Volume: {{metrics.volume}} ({{metrics.acceleration}}% acceleration)

Recent evidence:
{{#each evidence}}
- [{{source}}] {{title}} ({{url}})
  "{{text_excerpt}}"
{{/each}}

Generate a SHORT flash alert (2-3 sentences max) explaining:
1. What triggered this spike
2. Why it matters right now

Keep it brief - this is for quick notification, not detailed analysis.

OUTPUT FORMAT:
{
  "topic": "topic_key",
  "summary": "...",
  "citations": ["url1"]
}
```

## Prompt Engineering Guidelines

### Evidence Grounding

Always include enough context in the prompt for the LLM to work without external knowledge:

```
GOOD: "Based on the 5 evidence items above, summarize..."
BAD:  "Summarize the latest news about AWS Bedrock..."
```

### Citation Requirements

Enforce citations by structuring the output schema:

```typescript
interface BriefHighlight {
  topic: string;
  what_happened: string;
  why_it_matters: string;
  suggested_action: string;
  citations: string[]; // REQUIRED, minimum 1
}
```

Validation rule: Reject any highlight where `citations` is empty or contains URLs not in the evidence.

### Handling Edge Cases

**Insufficient evidence**:
```
If fewer than 2 evidence items are available for a topic, respond with:
{
  "topic": "...",
  "what_happened": "Insufficient evidence to summarize this topic.",
  "why_it_matters": "N/A",
  "suggested_action": "Monitor for more coverage.",
  "citations": []
}
```

**Conflicting evidence**:
```
If evidence contains contradictory claims, acknowledge both:
"Sources disagree: [Source A] reports X, while [Source B] claims Y. 
Suggested action: Wait for clarification before acting."
```

**Promotional/spam content**:
```
Skip evidence items that appear promotional or low-quality.
Mention in notes: "Some evidence items were filtered as promotional."
```

### Token Budget Management

| Model | Max Input | Max Output | Target Brief Size |
|-------|-----------|------------|-------------------|
| GPT-4 Turbo | 128K | 4K | ~2K tokens |
| GPT-4o | 128K | 4K | ~2K tokens |
| Claude 3 Opus | 200K | 4K | ~2K tokens |
| GPT-3.5 Turbo | 16K | 4K | ~1.5K tokens |

**Token estimation**:
- Evidence excerpt: ~100-150 tokens each
- Max 8 evidence items per topic × 5 topics = 40 items
- Total input: ~6-8K tokens + system prompt
- Target output: ~1.5K tokens

### Response Parsing & Validation

The Brief service MUST validate LLM output using **citation validation** as the primary safeguard:

```typescript
function validateBriefResponse(response: unknown, evidenceUrls: Set<string>): Brief {
  // 1. Parse JSON (with error handling)
  const parsed = JSON.parse(response);

  // 2. Validate schema
  assertValidBriefSchema(parsed);

  // 3. CRITICAL: Validate all citations reference actual evidence
  for (const highlight of parsed.highlights) {
    if (highlight.citations.length === 0) {
      throw new ValidationError(`Highlight for ${highlight.topic} has no citations`);
    }
    for (const citation of highlight.citations) {
      if (!evidenceUrls.has(citation)) {
        throw new ValidationError(`Citation not in evidence: ${citation}`);
      }
    }
  }

  return parsed;
}
```

### Citation Validation (Primary Safeguard)

**Why citation validation instead of hallucination detection?**

Detecting hallucinations reliably is an unsolved research problem. Pattern-based detection (dates, version numbers, hedging words) produces:
- **False positives**: Legitimate content flagged (e.g., dates in evidence, valid version numbers)
- **False negatives**: Hallucinations that don't match patterns
- **Maintenance burden**: Patterns need constant tuning

**Citation validation is concrete and verifiable**:
- Every URL in `citations` MUST exist in the evidence provided
- If an LLM fabricates a URL, validation fails immediately
- If an LLM makes claims without citations, the prompt instructs it to skip

**Validation rules**:
1. Every highlight MUST have at least 1 citation
2. Every citation URL MUST be in the evidence set
3. URLs are compared after normalization (lowercase, no trailing slash)

**When validation fails**:
1. Log the violation with details
2. Retry once with simplified prompt (fewer topics)
3. If retry fails, generate minimal brief with error note
4. Emit metric `ri_brief_citation_validation_failed_total`

### Handling Uncertainty

Instead of detecting hedging words, instruct the LLM to handle uncertainty explicitly:

**In the system prompt**:
```
If evidence is insufficient or conflicting:
- DO NOT guess or speculate
- State explicitly: "Evidence is limited" or "Sources disagree"
- Provide a conservative suggested_action: "Monitor for more coverage"
- Include fewer citations (only the ones that are clearly relevant)
```

**This shifts the burden**:
- LLM is instructed to be conservative
- Validation catches fabricated citations
- User sees "Evidence is limited" rather than false confidence

## Fallback Behavior

If LLM fails or returns invalid response:

1. **Retry once** with simplified prompt (fewer topics)
2. **If retry fails**, generate minimal brief:
   ```json
   {
     "title": "Brief Generation Failed",
     "highlights": [],
     "notes": "LLM generation failed after 2 attempts. Error: [error_code]"
   }
   ```
3. **Publish failure** to `summary.results` with `failure` status
4. **Alert** via `ri_brief_errors_total{error_type="llm_error"}` metric

## Model Selection

The Brief service should support multiple LLM providers:

| Provider | Model | Use Case |
|----------|-------|----------|
| OpenAI | gpt-4o | Primary (best quality) |
| OpenAI | gpt-3.5-turbo | Fallback (cheaper, faster) |
| Anthropic | claude-3-opus | Alternative primary |
| Anthropic | claude-3-haiku | Alternative fallback |

Selection logic:
```typescript
if (budgetRemaining > 0.50) {
  return 'gpt-4o';
} else if (budgetRemaining > 0.10) {
  return 'gpt-3.5-turbo';
} else {
  throw new BudgetExceededError();
}
```
