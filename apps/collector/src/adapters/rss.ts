import Parser from "rss-parser";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { parse as parseYaml } from "yaml";
import type { Logger } from "pino";
import {
  createCollectedContent,
  toRawEvent,
  type SourceAdapter,
  type RawEvent,
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
import {
  evaluateMarketFilters,
  matchEntityTerms,
  type MarketFilterProfile,
} from "../market-filters.js";

const MIN_RSS_CONTENT_LENGTH = 300;
const DEFAULT_FEED_POLL_INTERVAL_SECONDS = 900;
const DEFAULT_FEED_PRIORITY = 50;
const DEFAULT_EDGAR_FORMS_ALLOWLIST = ["8-K", "6-K", "10-Q", "10-K", "20-F", "40-F"];
const DEFAULT_SEC_USER_AGENT = "Rising Intelligence contact@example.com";

interface FeedConfig {
  id: string;
  name: string;
  url: string;
  poll_interval_seconds: number;
  priority: number;
  enabled: boolean;
  category?: string;
  topics?: string[];
  notes?: string;
  source_type?: string;
  signal_tier?: "high_volume" | "low_volume";
  market_gate?: boolean;
  entity_terms?: string[];
  cik?: string;
}

interface FeedDefaults {
  poll_interval_seconds?: number;
  priority?: number;
  enabled?: boolean;
  market_gate?: boolean;
  signal_tier?: "high_volume" | "low_volume";
}

interface EdgarDetailMetadata {
  filedDate?: string;
  acceptedAt?: string;
  primaryDocumentName?: string;
}

type EdgarDetailMetadataFetcher = (filingDetailUrl: string) => Promise<EdgarDetailMetadata | null>;

export interface RSSFeedErrorReport {
  feed: string;
  feedUrl: string;
  errorType: "parse_error";
}

export interface RSSAdapterOptions {
  edgarEnabled?: boolean;
  marketFilterProfiles?: readonly MarketFilterProfile[];
  edgarFormsAllowlist?: readonly string[];
  edgarFetchDetailMetadata?: boolean;
  edgarDownloadPrimaryDocs?: boolean;
  edgarPollIntervalSeconds?: number;
  edgarPollJitterRatio?: number;
  secUserAgent?: string;
  random?: () => number;
  fetchEdgarDetailMetadata?: EdgarDetailMetadataFetcher;
}

function hashUrl(url: string): string {
  return createHash("sha256").update(url).digest("hex").substring(0, 16);
}

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

function parseNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function parseBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") {
      return true;
    }
    if (normalized === "false") {
      return false;
    }
  }
  return undefined;
}

function normalizeStringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  return value
    .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
    .filter((entry) => entry.length > 0);
}

function toSignalTier(value: unknown): "high_volume" | "low_volume" | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "high_volume") {
    return "high_volume";
  }
  if (normalized === "low_volume") {
    return "low_volume";
  }
  return undefined;
}

function parseConfigPaths(paths: string): string[] {
  return paths
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function collectFeedEntries(value: unknown, result: Record<string, unknown>[]): void {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectFeedEntries(entry, result);
    }
    return;
  }

  if (!isRecord(value)) {
    return;
  }

  if (typeof value.name === "string" && typeof value.url === "string") {
    result.push(value);
    return;
  }

  for (const [key, nested] of Object.entries(value)) {
    if (key === "defaults") {
      continue;
    }
    collectFeedEntries(nested, result);
  }
}

function inferSignalTier(feed: { name: string; signal_tier?: "high_volume" | "low_volume" }): "high_volume" | "low_volume" {
  if (feed.signal_tier) {
    return feed.signal_tier;
  }
  return /pr\s*newswire/i.test(feed.name) ? "high_volume" : "low_volume";
}

function dedupeEntityTerms(entityTerms: readonly string[]): string[] {
  const deduped: string[] = [];
  const seen = new Set<string>();

  for (const rawTerm of entityTerms) {
    const term = rawTerm.trim();
    if (!term) {
      continue;
    }
    const normalized = term.toLowerCase();
    if (seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    deduped.push(term);
  }

  return deduped;
}

function normalizeFeedConfig(
  rawFeed: Record<string, unknown>,
  defaults: FeedDefaults
): FeedConfig | null {
  const name = typeof rawFeed.name === "string" ? rawFeed.name.trim() : "";
  const url = typeof rawFeed.url === "string" ? rawFeed.url.trim() : "";
  if (!name || !url) {
    return null;
  }

  const pollIntervalSeconds = Math.max(
    1,
    Math.floor(
      parseNumber(rawFeed.poll_interval_seconds)
        ?? defaults.poll_interval_seconds
        ?? DEFAULT_FEED_POLL_INTERVAL_SECONDS
    )
  );

  const priority = Math.max(
    1,
    Math.floor(
      parseNumber(rawFeed.priority)
        ?? defaults.priority
        ?? DEFAULT_FEED_PRIORITY
    )
  );

  const enabled = parseBoolean(rawFeed.enabled) ?? defaults.enabled ?? true;
  const signalTier = toSignalTier(rawFeed.signal_tier) ?? defaults.signal_tier;
  const marketGate = parseBoolean(rawFeed.market_gate) ?? defaults.market_gate ?? false;
  const sourceType = typeof rawFeed.source_type === "string"
    ? rawFeed.source_type.trim()
    : undefined;

  return {
    id: hashUrl(url),
    name,
    url,
    poll_interval_seconds: pollIntervalSeconds,
    priority,
    enabled,
    category: typeof rawFeed.category === "string" ? rawFeed.category : undefined,
    topics: normalizeStringArray(rawFeed.topics),
    notes: typeof rawFeed.notes === "string" ? rawFeed.notes : undefined,
    source_type: sourceType && sourceType.length > 0 ? sourceType : undefined,
    signal_tier: signalTier,
    market_gate: marketGate,
    entity_terms: normalizeStringArray(rawFeed.entity_terms),
    cik: typeof rawFeed.cik === "string" ? rawFeed.cik.trim() : undefined,
  };
}

function loadFeedsConfig(paths: string): FeedConfig[] {
  const configPaths = parseConfigPaths(paths);
  if (configPaths.length === 0) {
    throw new Error("FEEDS_CONFIG_PATH resolved to an empty value");
  }

  const feeds: FeedConfig[] = [];

  for (const configPath of configPaths) {
    const content = readFileSync(configPath, "utf-8");
    const parsed = parseYaml(content);
    const root = isRecord(parsed) ? parsed : {};
    const defaultsRoot = isRecord(root.defaults) ? root.defaults : {};
    const defaults: FeedDefaults = {
      poll_interval_seconds: parseNumber(defaultsRoot.poll_interval_seconds),
      priority: parseNumber(defaultsRoot.priority),
      enabled: parseBoolean(defaultsRoot.enabled),
      market_gate: parseBoolean(defaultsRoot.market_gate),
      signal_tier: toSignalTier(defaultsRoot.signal_tier),
    };

    const feedEntries: Record<string, unknown>[] = [];
    collectFeedEntries(root, feedEntries);

    for (const rawFeed of feedEntries) {
      const normalized = normalizeFeedConfig(rawFeed, defaults);
      if (!normalized || !normalized.enabled) {
        continue;
      }
      feeds.push(normalized);
    }
  }

  return feeds;
}

function toFeedPollIntervalMs(feed: FeedConfig): number {
  return feed.poll_interval_seconds * 1000;
}

function computeJitteredDelayMs(
  baseMs: number,
  jitterRatio: number,
  random: () => number
): number {
  if (jitterRatio <= 0) {
    return baseMs;
  }

  const jitterDelta = baseMs * jitterRatio * ((random() * 2) - 1);
  return Math.max(1_000, Math.round(baseMs + jitterDelta));
}

function normalizeEdgarFormType(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  return value.trim().toUpperCase();
}

function extractEdgarFormType(...values: Array<string | undefined>): string | undefined {
  const combined = values.filter((value): value is string => Boolean(value)).join(" ");
  const match = combined.match(/\b(8-K|6-K|10-Q|10-K|20-F|40-F)\b/i);
  return normalizeEdgarFormType(match?.[1]);
}

function extractAccessionNumber(...values: Array<string | undefined>): string | undefined {
  const combined = values.filter((value): value is string => Boolean(value)).join(" ");
  const match = combined.match(/\b\d{10}-\d{2}-\d{6}\b/);
  return match?.[0];
}

function extractCik(...values: Array<string | undefined>): string | undefined {
  const combined = values.filter((value): value is string => Boolean(value)).join(" ");
  const match = combined.match(/(?:CIK=|cik=|cik\/)(\d{4,10})/);
  return match?.[1];
}

function normalizeDateOrTimestamp(raw: string | undefined): string | undefined {
  if (!raw || raw.trim() === "") {
    return undefined;
  }
  const asDate = new Date(raw);
  if (Number.isNaN(asDate.getTime())) {
    return undefined;
  }
  return asDate.toISOString();
}

function extractSimpleField(html: string, patterns: RegExp[]): string | undefined {
  for (const pattern of patterns) {
    const match = html.match(pattern);
    const value = match?.[1]?.trim();
    if (value) {
      return value;
    }
  }
  return undefined;
}

async function defaultFetchEdgarDetailMetadata(
  filingDetailUrl: string,
  secUserAgent: string
): Promise<EdgarDetailMetadata | null> {
  const response = await fetch(filingDetailUrl, {
    headers: {
      "User-Agent": secUserAgent,
      Accept: "text/html,application/xhtml+xml",
    },
  });
  if (!response.ok) {
    throw new Error(`EDGAR detail page request failed (${response.status})`);
  }

  const html = await response.text();
  const filedDate = extractSimpleField(html, [
    /Filing Date[^0-9]*(\d{4}-\d{2}-\d{2})/i,
    /FILED AS OF DATE[^0-9]*(\d{8})/i,
  ]);
  const acceptedAtRaw = extractSimpleField(html, [
    /Accepted[^0-9]*(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})/i,
    /ACCEPTANCE-DATETIME[^0-9]*(\d{14})/i,
  ]);
  const primaryDocumentName = extractSimpleField(html, [
    /Primary Document[^>]*>\s*([^<\s]+\.(?:htm|html|txt|xml))/i,
    /Document[^>]*>\s*([^<\s]+\.(?:htm|html|txt|xml))/i,
  ]);

  return {
    filedDate: filedDate?.length === 8
      ? `${filedDate.slice(0, 4)}-${filedDate.slice(4, 6)}-${filedDate.slice(6, 8)}`
      : filedDate,
    acceptedAt: acceptedAtRaw?.length === 14
      ? `${acceptedAtRaw.slice(0, 4)}-${acceptedAtRaw.slice(4, 6)}-${acceptedAtRaw.slice(6, 8)}T${acceptedAtRaw.slice(8, 10)}:${acceptedAtRaw.slice(10, 12)}:${acceptedAtRaw.slice(12, 14)}Z`
      : normalizeDateOrTimestamp(acceptedAtRaw),
    primaryDocumentName,
  };
}

interface EdgarMetaResult {
  keep: boolean;
  sourceMeta: Record<string, unknown>;
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
  private textEnrichmentStrategy: TextEnrichmentStrategy;
  private onFeedError?: (report: RSSFeedErrorReport) => void;
  private marketFilterProfiles: readonly MarketFilterProfile[];
  private edgarFormsAllowlist: Set<string>;
  private edgarFetchDetailMetadata: boolean;
  private edgarDownloadPrimaryDocs: boolean;
  private edgarPollIntervalMs: number;
  private edgarPollJitterRatio: number;
  private secUserAgent: string;
  private random: () => number;
  private fetchEdgarDetailMetadata: EdgarDetailMetadataFetcher;
  private nextPollAtByFeed = new Map<string, number>();
  private watchlistEntityTerms: string[];

  constructor(
    feedsConfigPath: string,
    pollIntervalMs: number,
    checkpoints: CheckpointStore,
    logger: Logger,
    contentFetcherConfig?: ContentFetcherConfig,
    onFeedError?: (report: RSSFeedErrorReport) => void,
    options: RSSAdapterOptions = {}
  ) {
    this.pollIntervalMs = pollIntervalMs;
    this.checkpoints = checkpoints;
    this.logger = logger;
    this.onFeedError = onFeedError;
    this.marketFilterProfiles = options.marketFilterProfiles ?? [];
    this.edgarFormsAllowlist = new Set(
      (options.edgarFormsAllowlist ?? DEFAULT_EDGAR_FORMS_ALLOWLIST)
        .map((entry) => normalizeEdgarFormType(entry))
        .filter((entry): entry is string => Boolean(entry))
    );
    this.edgarFetchDetailMetadata = options.edgarFetchDetailMetadata ?? true;
    this.edgarDownloadPrimaryDocs = options.edgarDownloadPrimaryDocs ?? false;
    this.edgarPollIntervalMs = Math.max(
      1_000,
      (options.edgarPollIntervalSeconds ?? 1800) * 1000
    );
    this.edgarPollJitterRatio = options.edgarPollJitterRatio ?? 0.4;
    this.secUserAgent = options.secUserAgent?.trim() || DEFAULT_SEC_USER_AGENT;
    this.random = options.random ?? Math.random;
    this.fetchEdgarDetailMetadata = options.fetchEdgarDetailMetadata
      ?? ((filingDetailUrl) => defaultFetchEdgarDetailMetadata(filingDetailUrl, this.secUserAgent));
    this.textEnrichmentStrategy = createTextEnrichmentStrategy(
      contentFetcherConfig,
      logger,
      "Fetched article content for RSS item"
    );
    this.parser = new Parser({
      timeout: 30000,
      headers: {
        "User-Agent": this.secUserAgent,
        // Some feeds (for example InfoQ) reject strict RSS-only Accept headers.
        Accept: "*/*",
      },
    });
    const configuredFeeds = loadFeedsConfig(feedsConfigPath);
    const edgarEnabled = options.edgarEnabled ?? true;
    this.feeds = edgarEnabled
      ? configuredFeeds
      : configuredFeeds.filter((feed) => feed.source_type !== "edgar");
    if (!edgarEnabled) {
      const disabledFeedCount = configuredFeeds.length - this.feeds.length;
      this.logger.info(
        { disabledFeedCount },
        "EDGAR feeds disabled"
      );
    }
    this.watchlistEntityTerms = dedupeEntityTerms(
      this.feeds
        .filter((feed) => feed.source_type === "edgar")
        .flatMap((feed) => feed.entity_terms ?? [])
    );
    const highVolumeFeedsMissingEntityTerms = this.feeds.filter((feed) => {
      if (!feed.market_gate || inferSignalTier(feed) !== "high_volume") {
        return false;
      }
      const effectiveEntityTerms = dedupeEntityTerms([
        ...(feed.entity_terms ?? []),
        ...this.watchlistEntityTerms,
      ]);
      return effectiveEntityTerms.length === 0;
    });
    if (highVolumeFeedsMissingEntityTerms.length > 0) {
      this.logger.warn(
        {
          feeds: highVolumeFeedsMissingEntityTerms.map((feed) => feed.name),
        },
        "High-volume market-gated feeds are configured without effective entity_terms; strict entity gating may drop all items"
      );
    }
    if (this.edgarDownloadPrimaryDocs) {
      this.logger.warn(
        "EDGAR primary document downloads are disabled in phase 1 despite EDGAR_DOWNLOAD_PRIMARY_DOCS=true"
      );
    }
  }

  async initialize(): Promise<void> {
    const now = Date.now();
    for (const feed of this.feeds) {
      this.nextPollAtByFeed.set(feed.id, now);
    }

    this.logger.info(
      { feedCount: this.feeds.length },
      "RSS adapter initialized"
    );
  }

  private getFeedPollIntervalMs(feed: FeedConfig): number {
    if (feed.source_type === "edgar") {
      return this.edgarPollIntervalMs;
    }
    return toFeedPollIntervalMs(feed);
  }

  private scheduleNextPoll(feed: FeedConfig, nowMs: number): void {
    const baseMs = this.getFeedPollIntervalMs(feed);
    const delayMs = feed.source_type === "edgar"
      ? computeJitteredDelayMs(baseMs, this.edgarPollJitterRatio, this.random)
      : baseMs;
    this.nextPollAtByFeed.set(feed.id, nowMs + delayMs);
  }

  private isFeedDue(feed: FeedConfig, nowMs: number): boolean {
    const nextPollAt = this.nextPollAtByFeed.get(feed.id);
    if (nextPollAt === undefined) {
      return true;
    }
    return nowMs >= nextPollAt;
  }

  async *fetch(): AsyncIterable<FetchResult> {
    let attemptedFeeds = 0;
    let failedFeeds = 0;
    const nowMs = Date.now();

    for (const feed of this.feeds) {
      if (!this.isFeedDue(feed, nowMs)) {
        continue;
      }

      attemptedFeeds += 1;
      try {
        yield* this.fetchFeed(feed);
      } catch (error) {
        failedFeeds += 1;
        this.logger.error(
          { feed: feed.name, url: feed.url, error },
          "Failed to fetch feed"
        );
      } finally {
        this.scheduleNextPoll(feed, Date.now());
      }
    }

    if (attemptedFeeds > 0 && failedFeeds === attemptedFeeds) {
      throw new Error("All configured RSS feeds failed during poll cycle");
    }
  }

  private async buildEdgarMetadata(
    feed: FeedConfig,
    item: Parser.Item,
    title: string,
    text: string
  ): Promise<EdgarMetaResult> {
    const formType = extractEdgarFormType(
      title,
      text,
      item.contentSnippet,
      item.content,
      item.summary
    );
    if (!formType || !this.edgarFormsAllowlist.has(formType)) {
      return {
        keep: false,
        sourceMeta: {},
      };
    }

    const filingDetailUrl = item.link;
    const accessionNumber = extractAccessionNumber(
      item.guid,
      item.link,
      title,
      text
    );
    const cik = feed.cik ?? extractCik(item.guid, item.link, title, text);
    const sourceMeta: Record<string, unknown> = {
      form_type: formType,
      ...(cik && { cik }),
      ...(accessionNumber && { accession_number: accessionNumber }),
      ...(filingDetailUrl && { filing_detail_url: filingDetailUrl }),
      ...(parseDate(item.pubDate ?? item.isoDate) && {
        filed_date: parseDate(item.pubDate ?? item.isoDate)?.slice(0, 10),
      }),
    };

    if (this.edgarFetchDetailMetadata && filingDetailUrl) {
      try {
        const detail = await this.fetchEdgarDetailMetadata(filingDetailUrl);
        if (detail?.filedDate) {
          sourceMeta.filed_date = detail.filedDate;
        }
        if (detail?.acceptedAt) {
          sourceMeta.accepted_at = detail.acceptedAt;
        }
        if (detail?.primaryDocumentName) {
          sourceMeta.primary_document_name = detail.primaryDocumentName;
        }
      } catch (error) {
        this.logger.warn(
          { feed: feed.name, filingDetailUrl, error },
          "Failed to enrich EDGAR detail metadata"
        );
      }
    }

    return {
      keep: true,
      sourceMeta,
    };
  }

  private applyMarketPolicy(
    feed: FeedConfig,
    content: string
  ): {
    keep: boolean;
    marketProfiles: string[];
    matchReasons: string[];
    marketTags: string[];
  } {
    if (!feed.market_gate) {
      return {
        keep: true,
        marketProfiles: [],
        matchReasons: [],
        marketTags: [],
      };
    }

    const marketEvaluation = evaluateMarketFilters(content, this.marketFilterProfiles);
    if (marketEvaluation.marketProfiles.length === 0) {
      return {
        keep: false,
        marketProfiles: [],
        matchReasons: [],
        marketTags: [],
      };
    }

    const signalTier = inferSignalTier(feed);
    const reasons = [...marketEvaluation.matchReasons];
    if (signalTier === "high_volume") {
      const entityMatch = matchEntityTerms(
        content,
        dedupeEntityTerms([...(feed.entity_terms ?? []), ...this.watchlistEntityTerms])
      );
      if (!entityMatch.matched) {
        return {
          keep: false,
          marketProfiles: [],
          matchReasons: [],
          marketTags: [],
        };
      }
      reasons.push(...entityMatch.matchedTerms.map((term) => `entity:${term}`));
    }

    const marketProfiles = marketEvaluation.marketProfiles;
    return {
      keep: true,
      marketProfiles,
      matchReasons: reasons,
      marketTags: marketProfiles.map((profile) => `market.${profile}`),
    };
  }

  private async *fetchFeed(feed: FeedConfig): AsyncIterable<FetchResult> {
    const checkpointKey = `last_guid_${feed.id}`;
    const lastGuid = this.checkpoints.getCheckpoint(this.name, checkpointKey);

    this.logger.debug(
      { feed: feed.name, url: feed.url, lastGuid },
      "Fetching RSS feed"
    );

    let parsedFeed;
    try {
      parsedFeed = await this.parser.parseURL(feed.url);
    } catch (error) {
      try {
        this.onFeedError?.({
          feed: feed.name,
          feedUrl: feed.url,
          errorType: "parse_error",
        });
      } catch (metricError) {
        this.logger.debug(
          { feed: feed.name, url: feed.url, error: metricError },
          "Failed to record RSS feed parse error metric"
        );
      }

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

    let foundCheckpoint = !lastGuid;
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

    for (const item of itemsToProcess.reverse()) {
      const guid = item.guid ?? item.link ?? item.title;
      if (!guid) continue;

      const eventId = `rss:${hashUrl(feed.url)}:${hashUrl(guid)}`;
      let text = item.contentSnippet ?? item.content ?? item.summary ?? "";
      const title = item.title ?? "";

      text = await this.textEnrichmentStrategy.enrich({
        text,
        url: item.link,
        minLength: MIN_RSS_CONTENT_LENGTH,
        logContext: { feed: feed.name },
      });

      const contentForMatching = `${title} ${text}`.trim();
      let sourceMeta: Record<string, unknown> = {
        feed_name: feed.name,
        feed_url: feed.url,
        category: feed.category,
        guid,
        source_type: feed.source_type ?? "rss",
        signal_tier: inferSignalTier(feed),
      };

      if (feed.source_type === "edgar") {
        const edgarMeta = await this.buildEdgarMetadata(feed, item, title, text);
        if (!edgarMeta.keep) {
          continue;
        }
        sourceMeta = {
          ...sourceMeta,
          ...edgarMeta.sourceMeta,
        };
      }

      const marketPolicy = this.applyMarketPolicy(feed, contentForMatching);
      if (!marketPolicy.keep) {
        continue;
      }

      if (marketPolicy.marketProfiles.length > 0) {
        sourceMeta = {
          ...sourceMeta,
          market_profiles: marketPolicy.marketProfiles,
          match_reasons: marketPolicy.matchReasons,
        };
      }

      const event = toRawEvent(createCollectedContent({
        eventId,
        source: "rss",
        fetchedAt: new Date().toISOString(),
        publishedAt: parseDate(item.pubDate ?? item.isoDate),
        url: item.link,
        title,
        text,
        tags: marketPolicy.marketTags.length > 0 ? marketPolicy.marketTags : undefined,
        author: item.creator
          ? { displayName: item.creator }
          : undefined,
        extracted: {
          urls: extractUrls(`${title} ${text}`),
          hashtags: extractHashtags(`${title} ${text}`),
        },
        sourceMeta: sourceMeta,
      }));

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

export interface CreateRSSAdapterInput {
  feedsConfigPath: string;
  pollIntervalMs: number;
  checkpoints: CheckpointStore;
  logger: Logger;
  contentFetcherConfig?: ContentFetcherConfig;
  onFeedError?: (report: RSSFeedErrorReport) => void;
  marketFilterProfiles?: readonly MarketFilterProfile[];
  edgarEnabled?: boolean;
  edgarFormsAllowlist?: readonly string[];
  edgarFetchDetailMetadata?: boolean;
  edgarDownloadPrimaryDocs?: boolean;
  edgarPollIntervalSeconds?: number;
  edgarPollJitterRatio?: number;
  secUserAgent?: string;
}

export function createRSSAdapter(input: CreateRSSAdapterInput): SourceAdapter {
  return new RSSAdapter(
    input.feedsConfigPath,
    input.pollIntervalMs,
    input.checkpoints,
    input.logger,
    input.contentFetcherConfig,
    input.onFeedError,
    {
      marketFilterProfiles: input.marketFilterProfiles,
      edgarEnabled: input.edgarEnabled,
      edgarFormsAllowlist: input.edgarFormsAllowlist,
      edgarFetchDetailMetadata: input.edgarFetchDetailMetadata,
      edgarDownloadPrimaryDocs: input.edgarDownloadPrimaryDocs,
      edgarPollIntervalSeconds: input.edgarPollIntervalSeconds,
      edgarPollJitterRatio: input.edgarPollJitterRatio,
      secUserAgent: input.secUserAgent,
    }
  );
}
