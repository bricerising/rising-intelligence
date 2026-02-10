import type { Logger } from "pino";
import type { SourceAdapter, RawEvent, FetchResult, Source } from "../types.js";
import type { CheckpointStore } from "../checkpoint.js";
import { extractUrls, extractHashtags } from "../topics/extractor.js";
import type { ContentFetcherConfig } from "../content-fetcher.js";
import {
  createTextEnrichmentStrategy,
  type TextEnrichmentStrategy,
} from "./text-enrichment-strategy.js";

const HN_API_BASE = "https://hacker-news.firebaseio.com/v0";
const MIN_HN_TEXT_LENGTH = 1;

type HNMode = "top" | "new" | "best";

interface HNItem {
  id: number;
  type: string;
  by?: string;
  time?: number;
  title?: string;
  text?: string;
  url?: string;
  score?: number;
  descendants?: number;
}

/**
 * Fetch JSON from HN API with timeout
 */
async function fetchJson<T>(url: string): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        "User-Agent": "RisingIntelligence/1.0",
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return (await response.json()) as T;
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Hacker News adapter.
 * Polls HN Firebase API for top/new/best stories.
 */
export class HackerNewsAdapter implements SourceAdapter {
  readonly name = "hackernews";
  readonly source: Source = "hackernews";
  readonly pollIntervalMs: number;

  private mode: HNMode;
  private maxItems: number;
  private checkpoints: CheckpointStore;
  private logger: Logger;
  private textEnrichmentStrategy: TextEnrichmentStrategy;

  constructor(
    mode: HNMode,
    pollIntervalMs: number,
    maxItems: number,
    checkpoints: CheckpointStore,
    logger: Logger,
    contentFetcherConfig?: ContentFetcherConfig
  ) {
    this.mode = mode;
    this.pollIntervalMs = pollIntervalMs;
    this.maxItems = maxItems;
    this.checkpoints = checkpoints;
    this.logger = logger;
    this.textEnrichmentStrategy = createTextEnrichmentStrategy(
      contentFetcherConfig,
      logger,
      "Fetched article content for HN story"
    );
  }

  async initialize(): Promise<void> {
    this.logger.info(
      { mode: this.mode, maxItems: this.maxItems },
      "Hacker News adapter initialized"
    );
  }

  async *fetch(): AsyncIterable<FetchResult> {
    const checkpointKey = `last_max_id_${this.mode}`;
    const lastMaxIdStr = this.checkpoints.getCheckpoint(this.name, checkpointKey);
    const lastMaxId = lastMaxIdStr ? parseInt(lastMaxIdStr, 10) : 0;

    // Get story IDs based on mode
    const endpoint = `${HN_API_BASE}/${this.mode}stories.json`;
    this.logger.debug({ endpoint, lastMaxId }, "Fetching HN story IDs");

    let storyIds: number[];
    try {
      storyIds = await fetchJson<number[]>(endpoint);
    } catch (error) {
      this.logger.error({ error }, "Failed to fetch HN story IDs");
      throw error;
    }

    // Take top N stories that are newer than checkpoint
    const newStoryIds = storyIds
      .filter((id) => id > lastMaxId)
      .slice(0, this.maxItems);

    if (newStoryIds.length === 0) {
      this.logger.debug("No new HN stories");
      return;
    }

    this.logger.debug(
      { totalStories: storyIds.length, newStories: newStoryIds.length },
      "Processing new HN stories"
    );

    // Fetch each story (could parallelize but respecting rate limits)
    let maxProcessedId = lastMaxId;

    for (const storyId of newStoryIds) {
      try {
        const item = await fetchJson<HNItem | null>(
          `${HN_API_BASE}/item/${storyId}.json`
        );

        if (!item || item.type !== "story") {
          maxProcessedId = Math.max(maxProcessedId, storyId);
          continue;
        }

        const event = await this.itemToRawEvent(item);
        if (event) {
          maxProcessedId = Math.max(maxProcessedId, storyId);
          yield {
            event,
            checkpointKey,
            checkpointValue: maxProcessedId.toString(),
          };
        }
      } catch (error) {
        this.logger.warn(
          { storyId, error },
          "Failed to fetch HN story"
        );
        // Continue with other stories
      }

      // Small delay between requests to be nice to the API
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  private async itemToRawEvent(item: HNItem): Promise<RawEvent | null> {
    if (!item.id) return null;

    const title = item.title ?? "";
    let text = item.text ?? "";

    text = await this.textEnrichmentStrategy.enrich({
      text,
      url: item.url,
      minLength: MIN_HN_TEXT_LENGTH,
      logContext: { hnId: item.id },
    });

    // Fall back to URL when we could not extract text for link posts.
    if (!text && item.url) {
      text = item.url;
    }

    const combinedText = `${title} ${text}`;

    const event: RawEvent = {
      event_id: `hn:${item.id}`,
      source: "hackernews",
      fetched_at: new Date().toISOString(),
      published_at: item.time
        ? new Date(item.time * 1000).toISOString()
        : undefined,
      url: item.url ?? `https://news.ycombinator.com/item?id=${item.id}`,
      title,
      text,
      author: item.by
        ? {
            handle: item.by,
            display_name: item.by,
          }
        : undefined,
      engagement: {
        score: item.score ?? 0,
        comments: item.descendants ?? 0,
      },
      extracted: {
        urls: extractUrls(combinedText),
        hashtags: extractHashtags(combinedText),
      },
      source_meta: {
        hn_id: item.id,
        hn_type: item.type,
        mode: this.mode,
      },
    };

    return event;
  }

  async shutdown(): Promise<void> {
    this.logger.info("Hacker News adapter shut down");
  }
}

/**
 * Factory function to create HN adapter
 */
export function createHackerNewsAdapter(
  mode: string,
  pollIntervalMs: number,
  maxItems: number,
  checkpoints: CheckpointStore,
  logger: Logger,
  contentFetcherConfig?: ContentFetcherConfig
): SourceAdapter {
  const validMode = (["top", "new", "best"].includes(mode)
    ? mode
    : "top") as HNMode;

  return new HackerNewsAdapter(
    validMode,
    pollIntervalMs,
    maxItems,
    checkpoints,
    logger,
    contentFetcherConfig
  );
}
