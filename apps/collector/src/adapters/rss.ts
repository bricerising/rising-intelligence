import Parser from "rss-parser";
import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";
import { createHash } from "node:crypto";
import type { Logger } from "pino";
import type { SourceAdapter, RawEvent, FetchResult, Source } from "../types.js";
import type { CheckpointStore } from "../checkpoint.js";
import { extractUrls, extractHashtags } from "../topics/extractor.js";
import { fetchArticleContent, type ContentFetcherConfig } from "../content-fetcher.js";

/**
 * Feed configuration from feeds.yaml
 */
interface FeedConfig {
  name: string;
  url: string;
  poll_interval_seconds?: number;
  priority?: number;
  enabled?: boolean;
  category?: string;
  topics?: string[];
  notes?: string;
}

/**
 * Parsed feeds.yaml structure
 */
interface FeedsYaml {
  defaults?: {
    poll_interval_seconds?: number;
    priority?: number;
    enabled?: boolean;
  };
  official_blogs?: FeedConfig[];
  ai_research?: FeedConfig[];
  aggregators?: FeedConfig[];
  opensource?: FeedConfig[];
}

/**
 * Create hash of URL for stable event IDs
 */
function hashUrl(url: string): string {
  return createHash("sha256").update(url).digest("hex").substring(0, 16);
}

/**
 * Parse RSS date string to ISO8601
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
 * Load feed configurations from YAML file
 */
function loadFeedsConfig(path: string): FeedConfig[] {
  const content = readFileSync(path, "utf-8");
  const yaml = parseYaml(content) as FeedsYaml;

  const defaults = yaml.defaults ?? {};
  const feeds: FeedConfig[] = [];

  // Collect feeds from all sections
  const sections = [
    yaml.official_blogs,
    yaml.ai_research,
    yaml.aggregators,
    yaml.opensource,
  ];

  for (const section of sections) {
    if (!section) continue;
    for (const feed of section) {
      // Apply defaults
      const config: FeedConfig = {
        ...feed,
        poll_interval_seconds:
          feed.poll_interval_seconds ?? defaults.poll_interval_seconds ?? 900,
        priority: feed.priority ?? defaults.priority ?? 50,
        enabled: feed.enabled ?? defaults.enabled ?? true,
      };
      if (config.enabled) {
        feeds.push(config);
      }
    }
  }

  return feeds;
}

/**
 * RSS/Atom feed adapter.
 * Polls configured feeds and yields RawEvents for new items.
 */
export class RSSAdapter implements SourceAdapter {
  readonly name = "rss";
  readonly source: Source = "rss";
  readonly pollIntervalMs: number;

  private parser: Parser;
  private feeds: FeedConfig[];
  private checkpoints: CheckpointStore;
  private logger: Logger;
  private contentFetcherConfig: ContentFetcherConfig;

  constructor(
    feedsConfigPath: string,
    pollIntervalMs: number,
    checkpoints: CheckpointStore,
    logger: Logger,
    contentFetcherConfig: ContentFetcherConfig
  ) {
    this.pollIntervalMs = pollIntervalMs;
    this.checkpoints = checkpoints;
    this.logger = logger;
    this.contentFetcherConfig = contentFetcherConfig;
    this.parser = new Parser({
      timeout: 30000,
      headers: {
        "User-Agent": "RisingIntelligence/1.0 (https://github.com/rising-intelligence)",
        // Some feeds (for example InfoQ) reject strict RSS-only Accept headers.
        Accept: "*/*",
      },
    });
    this.feeds = loadFeedsConfig(feedsConfigPath);
  }

  async initialize(): Promise<void> {
    this.logger.info(
      { feedCount: this.feeds.length },
      "RSS adapter initialized"
    );
  }

  async *fetch(): AsyncIterable<FetchResult> {
    let attemptedFeeds = 0;
    let failedFeeds = 0;

    for (const feed of this.feeds) {
      attemptedFeeds += 1;
      try {
        yield* this.fetchFeed(feed);
      } catch (error) {
        failedFeeds += 1;
        this.logger.error(
          { feed: feed.name, url: feed.url, error },
          "Failed to fetch feed"
        );
        // Continue with other feeds
      }
    }

    if (attemptedFeeds > 0 && failedFeeds === attemptedFeeds) {
      throw new Error("All configured RSS feeds failed during poll cycle");
    }
  }

  private async *fetchFeed(feed: FeedConfig): AsyncIterable<FetchResult> {
    const feedId = hashUrl(feed.url);
    const checkpointKey = `last_guid_${feedId}`;
    const lastGuid = this.checkpoints.getCheckpoint(this.name, checkpointKey);

    this.logger.debug(
      { feed: feed.name, url: feed.url, lastGuid },
      "Fetching RSS feed"
    );

    let parsedFeed;
    try {
      parsedFeed = await this.parser.parseURL(feed.url);
    } catch (error) {
      this.logger.warn(
        { feed: feed.name, url: feed.url, error },
        "Failed to parse RSS feed"
      );
      throw error;
    }

    const items = parsedFeed.items ?? [];
    if (items.length === 0) {
      this.logger.debug({ feed: feed.name }, "No items in feed");
      return;
    }

    // Find items newer than checkpoint
    let foundCheckpoint = !lastGuid; // If no checkpoint, process all
    const newItems: typeof items = [];

    for (const item of items) {
      const guid = item.guid ?? item.link ?? item.title;
      if (!guid) continue;

      if (guid === lastGuid) {
        foundCheckpoint = true;
        break;
      }
      newItems.push(item);
    }

    // If checkpoint not found, limit to recent items to avoid flooding
    const itemsToProcess = foundCheckpoint ? newItems : newItems.slice(0, 10);

    this.logger.debug(
      {
        feed: feed.name,
        totalItems: items.length,
        newItems: itemsToProcess.length,
        foundCheckpoint,
      },
      "Processing feed items"
    );

    // Process items in reverse order (oldest first) for proper checkpoint tracking
    for (const item of itemsToProcess.reverse()) {
      const guid = item.guid ?? item.link ?? item.title;
      if (!guid) continue;

      const eventId = `rss:${hashUrl(feed.url)}:${hashUrl(guid)}`;
      let text = item.contentSnippet ?? item.content ?? item.summary ?? "";
      const title = item.title ?? "";

      // If text is too short and we have a link, try to fetch article content
      const MIN_RSS_CONTENT_LENGTH = 300;
      if (text.length < MIN_RSS_CONTENT_LENGTH && item.link) {
        const articleContent = await fetchArticleContent(
          item.link,
          this.contentFetcherConfig,
          this.logger
        );

        if (articleContent && articleContent.success) {
          text = articleContent.text;
          this.logger.debug(
            { feed: feed.name, url: item.link, textLength: text.length },
            "Fetched article content for RSS item"
          );
        }
        // If fetch failed, keep the short RSS text
      }

      const event: RawEvent = {
        event_id: eventId,
        source: "rss",
        fetched_at: new Date().toISOString(),
        published_at: parseDate(item.pubDate ?? item.isoDate),
        url: item.link,
        title,
        text,
        author: item.creator
          ? { display_name: item.creator }
          : undefined,
        extracted: {
          urls: extractUrls(`${title} ${text}`),
          hashtags: extractHashtags(`${title} ${text}`),
        },
        source_meta: {
          feed_name: feed.name,
          feed_url: feed.url,
          category: feed.category,
          guid,
        },
      };

      yield {
        event,
        checkpointKey,
        checkpointValue: guid,
      };
    }
  }

  async shutdown(): Promise<void> {
    this.logger.info("RSS adapter shut down");
  }
}

/**
 * Factory function to create RSS adapter
 */
export function createRSSAdapter(
  feedsConfigPath: string,
  pollIntervalMs: number,
  checkpoints: CheckpointStore,
  logger: Logger,
  contentFetcherConfig: ContentFetcherConfig
): SourceAdapter {
  return new RSSAdapter(feedsConfigPath, pollIntervalMs, checkpoints, logger, contentFetcherConfig);
}
