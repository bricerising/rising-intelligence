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
const HN_REQUEST_TIMEOUT_MS = 30_000;
const HN_REQUEST_DELAY_MS = 100;
const HN_RANKED_SCAN_MULTIPLIER = 4;
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

type FetchJsonFn = <T>(url: string) => Promise<T>;

interface HackerNewsApi {
  fetchStoryIds(mode: HNMode): Promise<number[]>;
  fetchItem(storyId: number): Promise<HNItem | null>;
}

/**
 * Proxy around HN HTTP calls so adapter logic stays focused on event normalization.
 */
async function fetchJsonWithTimeout<T>(url: string): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), HN_REQUEST_TIMEOUT_MS);

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

function createHackerNewsApi(fetchJson: FetchJsonFn = fetchJsonWithTimeout): HackerNewsApi {
  return {
    async fetchStoryIds(mode: HNMode): Promise<number[]> {
      return fetchJson<number[]>(`${HN_API_BASE}/${mode}stories.json`);
    },
    async fetchItem(storyId: number): Promise<HNItem | null> {
      return fetchJson<HNItem | null>(`${HN_API_BASE}/item/${storyId}.json`);
    },
  };
}

function getCheckpointKey(mode: HNMode): string {
  return `last_max_id_${mode}`;
}

function parseCheckpointValue(value: string | undefined): number {
  if (!value) {
    return 0;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return parsed;
}

function dedupeStoryIds(storyIds: readonly number[]): number[] {
  return [...new Set(storyIds)];
}

function toEventId(storyId: number): string {
  return `hn:${storyId}`;
}

interface StorySelectionInput {
  mode: HNMode;
  storyIds: readonly number[];
  maxItems: number;
  lastMaxId: number;
  hasSeenStory(storyId: number): boolean;
}

function selectStoryIdsForPolling(input: StorySelectionInput): number[] {
  const dedupedStoryIds = dedupeStoryIds(input.storyIds);
  if (input.mode === "new") {
    // New stories are naturally append-only by ID, so process oldest unseen first.
    return dedupedStoryIds
      .filter((storyId) => storyId > input.lastMaxId)
      .sort((a, b) => a - b)
      .slice(0, input.maxItems);
  }

  // Ranked feeds can reorder; scan more than maxItems so previously skipped unseen IDs can recover.
  const scanWindow = dedupedStoryIds.slice(0, input.maxItems * HN_RANKED_SCAN_MULTIPLIER);
  return scanWindow
    .filter((storyId) => storyId > input.lastMaxId || !input.hasSeenStory(storyId))
    .slice(0, input.maxItems);
}

async function delayBetweenStoryRequests(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, HN_REQUEST_DELAY_MS));
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
  private api: HackerNewsApi;

  constructor(
    mode: HNMode,
    pollIntervalMs: number,
    maxItems: number,
    checkpoints: CheckpointStore,
    logger: Logger,
    contentFetcherConfig?: ContentFetcherConfig,
    api: HackerNewsApi = createHackerNewsApi()
  ) {
    this.mode = mode;
    this.pollIntervalMs = pollIntervalMs;
    this.maxItems = maxItems;
    this.checkpoints = checkpoints;
    this.logger = logger;
    this.api = api;
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
    const checkpointKey = getCheckpointKey(this.mode);
    const lastMaxId = parseCheckpointValue(
      this.checkpoints.getCheckpoint(this.name, checkpointKey)
    );

    this.logger.debug({ mode: this.mode, lastMaxId }, "Fetching HN story IDs");

    let storyIds: number[];
    try {
      storyIds = await this.api.fetchStoryIds(this.mode);
    } catch (error) {
      this.logger.error({ error }, "Failed to fetch HN story IDs");
      throw error;
    }

    const candidateStoryIds = selectStoryIdsForPolling(
      {
        mode: this.mode,
        storyIds,
        maxItems: this.maxItems,
        lastMaxId,
        hasSeenStory: (storyId) =>
          this.checkpoints.hasSeen(this.source, toEventId(storyId)),
      }
    );

    if (candidateStoryIds.length === 0) {
      this.logger.debug("No new HN stories");
      return;
    }

    this.logger.debug(
      {
        mode: this.mode,
        totalStories: storyIds.length,
        candidateStories: candidateStoryIds.length,
      },
      "Processing HN stories"
    );

    let checkpointCursor = lastMaxId;
    let checkpointBlocked = false;

    for (const storyId of candidateStoryIds) {
      try {
        const item = await this.api.fetchItem(storyId);

        if (!item || item.type !== "story") {
          if (this.mode === "new" && !checkpointBlocked) {
            checkpointCursor = storyId;
          } else if (this.mode !== "new") {
            checkpointCursor = Math.max(checkpointCursor, storyId);
          }
          continue;
        }

        const event = await this.itemToRawEvent(item);
        if (this.mode === "new" && !checkpointBlocked) {
          checkpointCursor = storyId;
        } else if (this.mode !== "new") {
          checkpointCursor = Math.max(checkpointCursor, storyId);
        }

        if (event !== null) {
          yield {
            event,
            checkpointKey,
            checkpointValue: checkpointCursor.toString(),
          };
        }
      } catch (error) {
        this.logger.warn(
          { storyId, error },
          "Failed to fetch HN story"
        );
        if (this.mode === "new") {
          checkpointBlocked = true;
        }
      }

      await delayBetweenStoryRequests();
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
export interface CreateHackerNewsAdapterInput {
  mode: string;
  pollIntervalMs: number;
  maxItems: number;
  checkpoints: CheckpointStore;
  logger: Logger;
  contentFetcherConfig?: ContentFetcherConfig;
  api?: HackerNewsApi;
}

export function createHackerNewsAdapter(
  input: CreateHackerNewsAdapterInput
): SourceAdapter {
  const validMode = (["top", "new", "best"].includes(input.mode)
    ? input.mode
    : "top") as HNMode;

  return new HackerNewsAdapter(
    validMode,
    input.pollIntervalMs,
    input.maxItems,
    input.checkpoints,
    input.logger,
    input.contentFetcherConfig,
    input.api
  );
}
