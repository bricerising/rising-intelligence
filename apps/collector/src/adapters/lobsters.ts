import Parser from "rss-parser";
import { createHash } from "node:crypto";
import type { Logger } from "pino";
import type { SourceAdapter, RawEvent, FetchResult, Source } from "../types.js";
import type { CheckpointStore } from "../checkpoint.js";
import { extractUrls, extractHashtags } from "../topics/extractor.js";
import type { ContentFetcherConfig } from "../content-fetcher.js";
import {
  createTextEnrichmentStrategy,
  type TextEnrichmentStrategy,
} from "./text-enrichment-strategy.js";

const LOBSTERS_RSS_URL = "https://lobste.rs/rss";
const MIN_RSS_CONTENT_LENGTH = 300;

/**
 * Create hash of string for stable IDs
 */
function hashString(str: string): string {
  return createHash("sha256").update(str).digest("hex").substring(0, 16);
}

/**
 * Parse date string to ISO8601
 */
function parseDate(dateStr: string | undefined): string | undefined {
  if (!dateStr) return undefined;
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return undefined;
    return date.toISOString();
  } catch {
    return undefined;
  }
}

/**
 * Lobsters adapter.
 * Polls Lobsters RSS feed for new stories.
 * Lobsters is a high-signal, computing-focused community.
 */
export class LobstersAdapter implements SourceAdapter {
  readonly name = "lobsters";
  readonly source: Source = "lobsters";
  readonly pollIntervalMs: number;

  private maxItems: number;
  private checkpoints: CheckpointStore;
  private logger: Logger;
  private parser: Parser;
  private textEnrichmentStrategy: TextEnrichmentStrategy;

  constructor(
    pollIntervalMs: number,
    maxItems: number,
    checkpoints: CheckpointStore,
    logger: Logger,
    contentFetcherConfig?: ContentFetcherConfig
  ) {
    this.pollIntervalMs = pollIntervalMs;
    this.maxItems = maxItems;
    this.checkpoints = checkpoints;
    this.logger = logger;
    this.textEnrichmentStrategy = createTextEnrichmentStrategy(
      contentFetcherConfig,
      logger,
      "Fetched article content for Lobsters item"
    );
    this.parser = new Parser({
      timeout: 30000,
      headers: {
        "User-Agent": "RisingIntelligence/1.0 (https://github.com/rising-intelligence)",
      },
    });
  }

  async initialize(): Promise<void> {
    this.logger.info(
      { maxItems: this.maxItems },
      "Lobsters adapter initialized"
    );
  }

  async *fetch(): AsyncIterable<FetchResult> {
    const checkpointKey = "last_guid";
    const lastGuid = this.checkpoints.getCheckpoint(this.name, checkpointKey);

    this.logger.debug({ lastGuid }, "Fetching Lobsters RSS");

    let feed;
    try {
      feed = await this.parser.parseURL(LOBSTERS_RSS_URL);
    } catch (error) {
      this.logger.error({ error }, "Failed to fetch Lobsters RSS");
      throw error;
    }

    const items = feed.items ?? [];
    if (items.length === 0) {
      this.logger.debug("No items in Lobsters feed");
      return;
    }

    // Find items newer than checkpoint
    let foundCheckpoint = !lastGuid;
    const newItems: typeof items = [];

    for (const item of items) {
      const guid = item.guid ?? item.link;
      if (!guid) continue;

      if (guid === lastGuid) {
        foundCheckpoint = true;
        break;
      }
      newItems.push(item);
    }

    // Limit items if checkpoint not found
    const itemsToProcess = foundCheckpoint
      ? newItems.slice(0, this.maxItems)
      : newItems.slice(0, Math.min(this.maxItems, 10));

    this.logger.debug(
      {
        totalItems: items.length,
        newItems: itemsToProcess.length,
        foundCheckpoint,
      },
      "Processing Lobsters items"
    );

    // Process items in reverse order (oldest first)
    for (const item of itemsToProcess.reverse()) {
      const guid = item.guid ?? item.link;
      if (!guid) continue;

      try {
        const event = await this.itemToRawEvent(item, guid);
        if (event) {
          yield {
            event,
            checkpointKey,
            checkpointValue: guid,
          };
        }
      } catch (error) {
        this.logger.warn({ guid, error }, "Failed to normalize Lobsters item");
      }
    }
  }

  private async itemToRawEvent(
    item: Parser.Item,
    guid: string
  ): Promise<RawEvent | null> {
    const title = item.title ?? "";
    let text = item.contentSnippet ?? item.content ?? "";

    text = await this.textEnrichmentStrategy.enrich({
      text,
      url: item.link,
      minLength: MIN_RSS_CONTENT_LENGTH,
    });

    const combinedText = `${title} ${text}`;

    // Extract lobsters-specific metadata
    // Lobsters items often have tags in categories
    const tags = (item.categories ?? []).map((c) =>
      typeof c === "string" ? c : String(c)
    ).filter(Boolean);

    const event: RawEvent = {
      event_id: `lobsters:${hashString(guid)}`,
      source: "lobsters",
      fetched_at: new Date().toISOString(),
      published_at: parseDate(item.pubDate ?? item.isoDate),
      url: item.link,
      title,
      text,
      author: item.creator
        ? {
            handle: item.creator,
            display_name: item.creator,
          }
        : undefined,
      extracted: {
        urls: extractUrls(combinedText),
        hashtags: extractHashtags(combinedText),
      },
      source_meta: {
        guid,
        tags,
        comments_url: (item as Record<string, unknown>).comments as string | undefined,
      },
    };

    return event;
  }

  async shutdown(): Promise<void> {
    this.logger.info("Lobsters adapter shut down");
  }
}

/**
 * Factory function to create Lobsters adapter
 */
export interface CreateLobstersAdapterInput {
  pollIntervalMs: number;
  maxItems: number;
  checkpoints: CheckpointStore;
  logger: Logger;
  contentFetcherConfig?: ContentFetcherConfig;
}

export function createLobstersAdapter(
  input: CreateLobstersAdapterInput
): SourceAdapter {
  return new LobstersAdapter(
    input.pollIntervalMs,
    input.maxItems,
    input.checkpoints,
    input.logger,
    input.contentFetcherConfig
  );
}
