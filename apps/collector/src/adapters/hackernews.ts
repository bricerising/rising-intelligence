import type { Logger } from "pino";
import {
  createCollectedContent,
  createCollectorSourceRecord,
  type CollectorIngestionEvent,
  type CollectorSourceAdapter,
  type FetchResult,
  type Source,
} from "../types.js";
import type { CheckpointStore } from "../checkpoint.js";
import { extractUrls, extractHashtags } from "@rising-intelligence/pipeline";
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
  storyIds: readonly number[];
  maxItems: number;
  lastMaxId: number;
  hasSeenStory(storyId: number): boolean;
}

interface CheckpointProgressState {
  cursor: number;
  blocked: boolean;
}

interface PollModeBehavior {
  readonly mode: HNMode;
  selectStoryIds(input: StorySelectionInput): number[];
  createCheckpointState(lastMaxId: number): CheckpointProgressState;
  onStoryProcessed(state: CheckpointProgressState, storyId: number): void;
  onStoryFetchFailure(state: CheckpointProgressState): void;
}

function selectStoryIdsForNewMode(input: StorySelectionInput): number[] {
  // New stories are naturally append-only by ID, so process oldest unseen first.
  return dedupeStoryIds(input.storyIds)
    .filter((storyId) => storyId > input.lastMaxId)
    .sort((a, b) => a - b)
    .slice(0, input.maxItems);
}

function selectStoryIdsForRankedMode(input: StorySelectionInput): number[] {
  const dedupedStoryIds = dedupeStoryIds(input.storyIds);
  // Ranked feeds can reorder; scan more than maxItems so previously skipped unseen IDs can recover.
  const scanWindow = dedupedStoryIds.slice(0, input.maxItems * HN_RANKED_SCAN_MULTIPLIER);
  return scanWindow
    .filter((storyId) => storyId > input.lastMaxId || !input.hasSeenStory(storyId))
    .slice(0, input.maxItems);
}

function createNewModeBehavior(): PollModeBehavior {
  return {
    mode: "new",
    selectStoryIds: selectStoryIdsForNewMode,
    createCheckpointState(lastMaxId: number): CheckpointProgressState {
      return {
        cursor: lastMaxId,
        blocked: false,
      };
    },
    onStoryProcessed(state: CheckpointProgressState, storyId: number): void {
      if (!state.blocked) {
        state.cursor = storyId;
      }
    },
    onStoryFetchFailure(state: CheckpointProgressState): void {
      state.blocked = true;
    },
  };
}

function createRankedModeBehavior(mode: Extract<HNMode, "top" | "best">): PollModeBehavior {
  return {
    mode,
    selectStoryIds: selectStoryIdsForRankedMode,
    createCheckpointState(lastMaxId: number): CheckpointProgressState {
      return {
        cursor: lastMaxId,
        blocked: false,
      };
    },
    onStoryProcessed(state: CheckpointProgressState, storyId: number): void {
      state.cursor = Math.max(state.cursor, storyId);
    },
    onStoryFetchFailure(): void {
      // Ranked mode keeps the checkpoint cursor unchanged for failed items so they can retry later.
    },
  };
}

const POLL_MODE_BEHAVIORS: Readonly<Record<HNMode, PollModeBehavior>> = {
  top: createRankedModeBehavior("top"),
  new: createNewModeBehavior(),
  best: createRankedModeBehavior("best"),
};

function isHnMode(value: string): value is HNMode {
  return value === "top" || value === "new" || value === "best";
}

function parseHnMode(value: string): HNMode {
  return isHnMode(value) ? value : "top";
}

async function delayBetweenStoryRequests(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, HN_REQUEST_DELAY_MS));
}

/**
 * Hacker News adapter.
 * Polls HN Firebase API for top/new/best stories.
 */
export class HackerNewsAdapter implements CollectorSourceAdapter {
  readonly name = "hackernews";
  readonly source: Source = "hackernews";
  readonly pollIntervalMs: number;

  private readonly mode: HNMode;
  private readonly maxItems: number;
  private readonly checkpoints: CheckpointStore;
  private readonly logger: Logger;
  private readonly textEnrichmentStrategy: TextEnrichmentStrategy;
  private readonly api: HackerNewsApi;
  private readonly pollModeBehavior: PollModeBehavior;

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
    this.pollModeBehavior = POLL_MODE_BEHAVIORS[mode];
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

    const candidateStoryIds = this.pollModeBehavior.selectStoryIds({
      storyIds,
      maxItems: this.maxItems,
      lastMaxId,
      hasSeenStory: (storyId) => this.checkpoints.hasSeen(this.source, toEventId(storyId)),
    });

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

    const checkpointState = this.pollModeBehavior.createCheckpointState(lastMaxId);

    for (const storyId of candidateStoryIds) {
      try {
        const item = await this.api.fetchItem(storyId);

        if (!item || item.type !== "story") {
          this.pollModeBehavior.onStoryProcessed(checkpointState, storyId);
          continue;
        }

        const content = await this.itemToCollectedContent(item);
        this.pollModeBehavior.onStoryProcessed(checkpointState, storyId);

        if (content !== null) {
          yield createCollectorSourceRecord({
            content,
            checkpointKey,
            checkpointValue: checkpointState.cursor.toString(),
          });
        }
      } catch (error) {
        this.logger.warn(
          { storyId, error },
          "Failed to fetch HN story"
        );
        this.pollModeBehavior.onStoryFetchFailure(checkpointState);
      }

      await delayBetweenStoryRequests();
    }
  }

  private async itemToCollectedContent(
    item: HNItem
  ): Promise<CollectorIngestionEvent | null> {
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

    return createCollectedContent({
      eventId: `hn:${item.id}`,
      source: "hackernews",
      fetchedAt: new Date().toISOString(),
      publishedAt: item.time
        ? new Date(item.time * 1000).toISOString()
        : undefined,
      url: item.url ?? `https://news.ycombinator.com/item?id=${item.id}`,
      title,
      text,
      author: item.by
        ? {
            handle: item.by,
            displayName: item.by,
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
      sourceMeta: {
        hn_id: item.id,
        hn_type: item.type,
        mode: this.mode,
      },
    });
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
): CollectorSourceAdapter {
  return new HackerNewsAdapter(
    parseHnMode(input.mode),
    input.pollIntervalMs,
    input.maxItems,
    input.checkpoints,
    input.logger,
    input.contentFetcherConfig,
    input.api
  );
}
