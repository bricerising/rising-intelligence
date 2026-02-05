/**
 * Reusable test fixtures for Rising Intelligence tests.
 *
 * These fixtures provide consistent, well-formed test data
 * that matches the expected schemas.
 */

// Note: Import actual types once they exist in contracts
// For now, define inline for bootstrapping

export const fixtures = {
  events: {
    /**
     * AWS Bedrock announcement from RSS feed.
     */
    awsBedrock: {
      event_id: 'test-event-rss-bedrock-001',
      source: 'rss' as const,
      fetched_at: '2026-02-05T10:00:00Z',
      published_at: '2026-02-05T09:30:00Z',
      url: 'https://aws.amazon.com/blogs/aws/bedrock-update',
      title: 'AWS Bedrock Gets New Foundation Models',
      text: 'Today we are excited to announce new foundation models available in Amazon Bedrock, including Claude 3 Opus and Gemini 1.5 Pro. These models offer improved performance for enterprise workloads.',
      tags: ['aws.bedrock', 'ai.llm'],
      engagement: { score: 0, comments: 0, likes: 0, shares: 0 },
    },

    /**
     * Reddit discussion about Bedrock.
     */
    redditBedrock: {
      event_id: 'test-event-reddit-bedrock-001',
      source: 'reddit' as const,
      fetched_at: '2026-02-05T10:05:00Z',
      published_at: '2026-02-05T09:45:00Z',
      url: 'https://reddit.com/r/aws/comments/abc123/bedrock_pricing_changes',
      title: 'Bedrock pricing changes - anyone else notice?',
      text: 'Has anyone noticed the new Bedrock pricing after the model update? Seems like Claude 3 Opus is more expensive but the latency improvements might be worth it for our use case.',
      tags: ['aws.bedrock'],
      engagement: { score: 42, comments: 15, likes: 42, shares: 0 },
      author: { handle: 'cloud_engineer_123' },
    },

    /**
     * HN discussion linking to the same AWS blog post (for dedup testing).
     */
    hnBedrockSameUrl: {
      event_id: 'test-event-hn-bedrock-001',
      source: 'hackernews' as const,
      fetched_at: '2026-02-05T10:10:00Z',
      published_at: '2026-02-05T09:50:00Z',
      url: 'https://aws.amazon.com/blogs/aws/bedrock-update', // Same URL as RSS
      title: 'AWS Bedrock Gets New Foundation Models',
      text: 'Interesting timing with Anthropic releasing Claude 3 Opus directly. Wonder how this affects their partnership.',
      tags: ['aws.bedrock', 'ai.llm'],
      engagement: { score: 156, comments: 89, likes: 0, shares: 0 },
    },

    /**
     * Bluesky post about AI (different topic).
     */
    blueskyAI: {
      event_id: 'test-event-bsky-ai-001',
      source: 'bluesky' as const,
      fetched_at: '2026-02-05T10:15:00Z',
      published_at: '2026-02-05T10:12:00Z',
      url: '',
      title: '',
      text: 'Just tried the new GPT-5 API and wow, the reasoning capabilities are next level. #AI #LLM',
      tags: ['ai.llm', 'ai.openai'],
      engagement: { score: 12, comments: 3, likes: 12, shares: 5 },
      author: { handle: 'ml_researcher.bsky.social' },
    },

    /**
     * GitHub release event.
     */
    githubRelease: {
      event_id: 'test-event-gh-release-001',
      source: 'github' as const,
      fetched_at: '2026-02-05T10:20:00Z',
      published_at: '2026-02-05T08:00:00Z',
      url: 'https://github.com/langchain-ai/langchain/releases/tag/v0.2.0',
      title: 'LangChain v0.2.0 Released',
      text: 'Major release with improved streaming support, better error handling, and new integrations for Claude 3 and Gemini.',
      tags: ['ai.llm', 'lang.python'],
      engagement: { score: 0, comments: 0, likes: 0, shares: 0 },
    },
  },

  /**
   * Pre-built trend snapshots for testing.
   */
  snapshots: {
    trending60m: {
      generated_at: '2026-02-05T11:00:00Z',
      window: 'TREND_WINDOW_60M',
      topics: [
        {
          topic: 'aws.bedrock',
          window: 'TREND_WINDOW_60M',
          window_end: '2026-02-05T11:00:00Z',
          volume: 15,
          prev_volume: 6,
          acceleration: 1.5,
          baseline_volume: 8,
          baseline_delta: 0.875,
          score: 8.5,
          evidence: {
            top_urls: ['https://aws.amazon.com/blogs/aws/bedrock-update'],
            top_event_ids: ['test-event-rss-bedrock-001', 'test-event-reddit-bedrock-001'],
          },
        },
        {
          topic: 'ai.llm',
          window: 'TREND_WINDOW_60M',
          window_end: '2026-02-05T11:00:00Z',
          volume: 10,
          prev_volume: 8,
          acceleration: 0.25,
          baseline_volume: 12,
          baseline_delta: -0.167,
          score: 6.0,
          evidence: {
            top_urls: [],
            top_event_ids: ['test-event-bsky-ai-001', 'test-event-gh-release-001'],
          },
        },
      ],
    },
  },

  /**
   * Pre-built briefs for testing.
   */
  briefs: {
    daily: {
      brief_id: 'test-brief-001',
      generated_at: '2026-02-05T01:00:00Z',
      window: 'BRIEF_WINDOW_DAILY',
      title: 'Tech Brief: Feb 5, 2026 - Bedrock Updates Lead the Day',
      highlights: [
        {
          topic: 'aws.bedrock',
          what_happened: 'AWS announced new foundation models in Bedrock including Claude 3 Opus [1]. Community discussion focused on pricing changes [2].',
          why_it_matters: 'If you use Bedrock in production, the new models offer better performance but may affect costs.',
          suggested_action: 'Review the pricing documentation and test Claude 3 Opus for your workloads.',
          citations: [
            'https://aws.amazon.com/blogs/aws/bedrock-update',
            'https://reddit.com/r/aws/comments/abc123/bedrock_pricing_changes',
          ],
        },
      ],
      notes: 'Coverage weighted toward Reddit today due to HN indexing delay.',
      meta: {
        provider: 'openai',
        model: 'gpt-4-turbo',
        input_tokens: 1500,
        output_tokens: 400,
        estimated_cost_usd: 0.027,
      },
    },
  },

  /**
   * Summary request for testing brief generation.
   */
  summaryRequests: {
    daily: {
      request_id: 'test-request-001',
      requested_at: '2026-02-05T01:00:00Z',
      type: 'SUMMARY_REQUEST_TYPE_DAILY',
      windows: ['TREND_WINDOW_60M', 'TREND_WINDOW_24H'],
      topics: [
        {
          topic: 'aws.bedrock',
          metrics: [
            {
              topic: 'aws.bedrock',
              window: 'TREND_WINDOW_60M',
              volume: 15,
              acceleration: 1.5,
              score: 8.5,
            },
          ],
          evidence: [
            {
              event_id: 'test-event-rss-bedrock-001',
              source: 'rss',
              url: 'https://aws.amazon.com/blogs/aws/bedrock-update',
              title: 'AWS Bedrock Gets New Foundation Models',
              published_at: '2026-02-05T09:30:00Z',
              fetched_at: '2026-02-05T10:00:00Z',
              text_excerpt: 'Today we are excited to announce new foundation models available in Amazon Bedrock, including Claude 3 Opus and Gemini 1.5 Pro.',
              engagement: { score: 0 },
            },
            {
              event_id: 'test-event-reddit-bedrock-001',
              source: 'reddit',
              url: 'https://reddit.com/r/aws/comments/abc123/bedrock_pricing_changes',
              title: 'Bedrock pricing changes - anyone else notice?',
              published_at: '2026-02-05T09:45:00Z',
              fetched_at: '2026-02-05T10:05:00Z',
              text_excerpt: 'Has anyone noticed the new Bedrock pricing after the model update? Seems like Claude 3 Opus is more expensive but the latency improvements might be worth it.',
              engagement: { score: 42 },
            },
          ],
        },
      ],
      budget: {
        daily_budget_usd: 1.0,
        max_topics: 10,
        max_evidence_per_topic: 5,
        max_output_tokens: 2000,
      },
    },
  },
};
