import type { Logger } from "pino";
import {
  buildFunctionDependencies,
  type FunctionDependencyOverrides,
} from "@rising-intelligence/shared/lifecycle";
import type { CheckpointStore } from "../checkpoint.js";
import type { Config } from "../config.js";
import type { ContentFetcherConfig } from "../content-fetcher.js";
import type { MarketFilterProfile } from "../market-filters.js";
import type { SourceAdapter } from "../types.js";
import {
  createHackerNewsAdapter,
  type CreateHackerNewsAdapterInput,
} from "./hackernews.js";
import {
  createLobstersAdapter,
  type CreateLobstersAdapterInput,
} from "./lobsters.js";
import {
  createRSSAdapter,
  type CreateRSSAdapterInput,
  type RSSFeedErrorReport,
} from "./rss.js";

const COLLECTOR_ADAPTER_FACTORY_CONFIG_KEYS = [
  "RSS_ENABLED",
  "RSS_POLL_INTERVAL_SECONDS",
  "EDGAR_ENABLED",
  "FEEDS_CONFIG_PATH",
  "EDGAR_FORMS_ALLOWLIST",
  "EDGAR_FETCH_DETAIL_METADATA",
  "EDGAR_DOWNLOAD_PRIMARY_DOCS",
  "EDGAR_POLL_INTERVAL_SECONDS",
  "EDGAR_POLL_JITTER_RATIO",
  "SEC_USER_AGENT",
  "HN_ENABLED",
  "HN_MODE",
  "HN_POLL_INTERVAL_SECONDS",
  "HN_MAX_ITEMS_PER_POLL",
  "LOBSTERS_ENABLED",
  "LOBSTERS_POLL_INTERVAL_SECONDS",
  "LOBSTERS_MAX_ITEMS_PER_POLL",
  "REDDIT_ENABLED",
  "BLUESKY_ENABLED",
  "MASTODON_ENABLED",
  "GITHUB_ENABLED",
] as const satisfies ReadonlyArray<keyof Config>;

type CollectorAdapterFactoryConfigKey =
  (typeof COLLECTOR_ADAPTER_FACTORY_CONFIG_KEYS)[number];

export type CollectorAdapterFactoryConfig = Pick<Config, CollectorAdapterFactoryConfigKey>;

export interface BuildCollectorAdaptersInput {
  config: CollectorAdapterFactoryConfig;
  checkpointStore: CheckpointStore;
  logger: Logger;
  contentFetcherConfig: ContentFetcherConfig;
  marketFilterProfiles: readonly MarketFilterProfile[];
  onRssFeedError?: (report: RSSFeedErrorReport) => void;
}

type ImplementedAdapterName = "rss" | "hackernews" | "lobsters";
type UnsupportedAdapterName = "reddit" | "bluesky" | "mastodon" | "github";
type AdapterName = ImplementedAdapterName | UnsupportedAdapterName;

export interface CollectorAdapterBuildResult {
  adapters: SourceAdapter[];
  unsupportedEnabledAdapters: UnsupportedAdapterName[];
}

export interface CollectorIngestionAdapterFactory {
  buildIngestionAdapters(input: BuildCollectorAdaptersInput): CollectorAdapterBuildResult;
}

type AdapterEnabledPredicate = (config: CollectorAdapterFactoryConfig) => boolean;

interface AdapterConstructors {
  createRSSAdapter: typeof createRSSAdapter;
  createHackerNewsAdapter: typeof createHackerNewsAdapter;
  createLobstersAdapter: typeof createLobstersAdapter;
}

type AdapterConstructorOverrides = FunctionDependencyOverrides<AdapterConstructors>;

const DEFAULT_ADAPTER_CONSTRUCTORS: AdapterConstructors = {
  createRSSAdapter,
  createHackerNewsAdapter,
  createLobstersAdapter,
};

interface AdapterDefinition<Name extends AdapterName> {
  readonly kind: "implemented" | "unsupported";
  readonly name: Name;
  readonly isEnabled: AdapterEnabledPredicate;
}

interface ImplementedAdapterDefinition
  extends AdapterDefinition<ImplementedAdapterName> {
  readonly kind: "implemented";
  create(input: BuildCollectorAdaptersInput, constructors: AdapterConstructors): SourceAdapter;
}

interface UnsupportedAdapterDefinition
  extends AdapterDefinition<UnsupportedAdapterName> {
  readonly kind: "unsupported";
}

type AdapterDefinitionItem =
  | ImplementedAdapterDefinition
  | UnsupportedAdapterDefinition;

type BuildImplementedAdapter = ImplementedAdapterDefinition["create"];

class CollectorAdapterDefinitionBuilder {
  private readonly definitions: AdapterDefinitionItem[] = [];
  private readonly registeredNames = new Set<AdapterName>();

  implemented(
    name: ImplementedAdapterName,
    isEnabled: AdapterEnabledPredicate,
    create: BuildImplementedAdapter
  ): this {
    this.registerName(name);
    this.definitions.push({
      kind: "implemented",
      name,
      isEnabled,
      create,
    });
    return this;
  }

  unsupported(name: UnsupportedAdapterName, isEnabled: AdapterEnabledPredicate): this {
    this.registerName(name);
    this.definitions.push({
      kind: "unsupported",
      name,
      isEnabled,
    });
    return this;
  }

  build(): ReadonlyArray<AdapterDefinitionItem> {
    return [...this.definitions];
  }

  private registerName(name: AdapterName): void {
    if (this.registeredNames.has(name)) {
      throw new Error(`Collector adapter definition "${name}" is already registered`);
    }
    this.registeredNames.add(name);
  }
}

function parseCsvValues(value: string): string[] {
  const values = new Set<string>();
  for (const entry of value.split(",")) {
    const trimmed = entry.trim();
    if (trimmed.length > 0) {
      values.add(trimmed);
    }
  }
  return [...values];
}

function createAdapterLogger(logger: Logger, adapter: AdapterName): Logger {
  return logger.child({ adapter });
}

function createRssAdapterInput(input: BuildCollectorAdaptersInput): CreateRSSAdapterInput {
  const {
    config,
    checkpointStore,
    logger,
    contentFetcherConfig,
    marketFilterProfiles,
  } = input;

  return {
    feedsConfigPath: config.FEEDS_CONFIG_PATH,
    pollIntervalMs: config.RSS_POLL_INTERVAL_SECONDS * 1000,
    checkpoints: checkpointStore,
    logger: createAdapterLogger(logger, "rss"),
    contentFetcherConfig,
    marketFilterProfiles,
    edgarEnabled: config.EDGAR_ENABLED,
    edgarFormsAllowlist: parseCsvValues(config.EDGAR_FORMS_ALLOWLIST),
    edgarFetchDetailMetadata: config.EDGAR_FETCH_DETAIL_METADATA,
    edgarDownloadPrimaryDocs: config.EDGAR_DOWNLOAD_PRIMARY_DOCS,
    edgarPollIntervalSeconds: config.EDGAR_POLL_INTERVAL_SECONDS,
    edgarPollJitterRatio: config.EDGAR_POLL_JITTER_RATIO,
    secUserAgent: config.SEC_USER_AGENT,
  };
}

function withOptionalRssFeedError(
  input: CreateRSSAdapterInput,
  onRssFeedError?: (report: RSSFeedErrorReport) => void
): CreateRSSAdapterInput {
  if (!onRssFeedError) {
    return input;
  }

  return {
    ...input,
    onFeedError: onRssFeedError,
  };
}

function createHackerNewsAdapterInput(
  input: BuildCollectorAdaptersInput
): CreateHackerNewsAdapterInput {
  const { config, checkpointStore, logger, contentFetcherConfig } = input;

  return {
    mode: config.HN_MODE,
    pollIntervalMs: config.HN_POLL_INTERVAL_SECONDS * 1000,
    maxItems: config.HN_MAX_ITEMS_PER_POLL,
    checkpoints: checkpointStore,
    logger: createAdapterLogger(logger, "hackernews"),
    contentFetcherConfig,
  };
}

function createLobstersAdapterInput(
  input: BuildCollectorAdaptersInput
): CreateLobstersAdapterInput {
  const { config, checkpointStore, logger, contentFetcherConfig } = input;

  return {
    pollIntervalMs: config.LOBSTERS_POLL_INTERVAL_SECONDS * 1000,
    maxItems: config.LOBSTERS_MAX_ITEMS_PER_POLL,
    checkpoints: checkpointStore,
    logger: createAdapterLogger(logger, "lobsters"),
    contentFetcherConfig,
  };
}

function createAdapterDefinitions(): ReadonlyArray<AdapterDefinitionItem> {
  return new CollectorAdapterDefinitionBuilder()
    .implemented("rss", (config) => config.RSS_ENABLED, (input, constructors) =>
      constructors.createRSSAdapter(
        withOptionalRssFeedError(createRssAdapterInput(input), input.onRssFeedError)
      )
    )
    .implemented("hackernews", (config) => config.HN_ENABLED, (input, constructors) =>
      constructors.createHackerNewsAdapter(createHackerNewsAdapterInput(input))
    )
    .implemented("lobsters", (config) => config.LOBSTERS_ENABLED, (input, constructors) =>
      constructors.createLobstersAdapter(createLobstersAdapterInput(input))
    )
    .unsupported("reddit", (config) => config.REDDIT_ENABLED)
    .unsupported("bluesky", (config) => config.BLUESKY_ENABLED)
    .unsupported("mastodon", (config) => config.MASTODON_ENABLED)
    .unsupported("github", (config) => config.GITHUB_ENABLED)
    .build();
}

const DEFAULT_ADAPTER_DEFINITIONS = createAdapterDefinitions();

export class CollectorAdapterFactory implements CollectorIngestionAdapterFactory {
  private readonly constructors: AdapterConstructors;
  private readonly definitions: ReadonlyArray<AdapterDefinitionItem>;

  constructor(constructors: AdapterConstructorOverrides = {}) {
    this.constructors = buildFunctionDependencies(
      "Collector adapter constructor",
      DEFAULT_ADAPTER_CONSTRUCTORS,
      constructors
    );
    this.definitions = DEFAULT_ADAPTER_DEFINITIONS;
  }

  buildIngestionAdapters(
    input: BuildCollectorAdaptersInput
  ): CollectorAdapterBuildResult {
    const adapters: SourceAdapter[] = [];
    const unsupportedEnabledAdapters: UnsupportedAdapterName[] = [];

    for (const definition of this.definitions) {
      if (!definition.isEnabled(input.config)) {
        continue;
      }

      if (definition.kind === "unsupported") {
        unsupportedEnabledAdapters.push(definition.name);
        continue;
      }

      adapters.push(definition.create(input, this.constructors));
    }

    return {
      adapters,
      unsupportedEnabledAdapters,
    };
  }
}

export function createCollectorAdapterFactory(
  constructors: AdapterConstructorOverrides = {}
): CollectorIngestionAdapterFactory {
  return new CollectorAdapterFactory(constructors);
}
