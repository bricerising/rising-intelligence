import type { Logger } from "pino";
import type { CheckpointStore } from "../checkpoint.js";
import type { ContentFetcherConfig } from "../content-fetcher.js";
import type { SourceAdapter } from "../types.js";
import { createHackerNewsAdapter } from "./hackernews.js";
import { createLobstersAdapter } from "./lobsters.js";
import { createRSSAdapter } from "./rss.js";

export interface CollectorAdapterFactoryConfig {
  RSS_ENABLED: boolean;
  RSS_POLL_INTERVAL_SECONDS: number;
  FEEDS_CONFIG_PATH: string;
  HN_ENABLED: boolean;
  HN_MODE: string;
  HN_POLL_INTERVAL_SECONDS: number;
  HN_MAX_ITEMS_PER_POLL: number;
  LOBSTERS_ENABLED: boolean;
  LOBSTERS_POLL_INTERVAL_SECONDS: number;
  LOBSTERS_MAX_ITEMS_PER_POLL: number;
  REDDIT_ENABLED: boolean;
  BLUESKY_ENABLED: boolean;
  MASTODON_ENABLED: boolean;
  GITHUB_ENABLED: boolean;
}

export interface BuildCollectorAdaptersInput {
  config: CollectorAdapterFactoryConfig;
  checkpointStore: CheckpointStore;
  logger: Logger;
  contentFetcherConfig: ContentFetcherConfig;
}

export interface CollectorAdapterBuildResult {
  adapters: SourceAdapter[];
  unsupportedEnabledAdapters: string[];
}

interface AdapterFactoryContext extends BuildCollectorAdaptersInput {}

type AdapterName =
  | "rss"
  | "hackernews"
  | "lobsters"
  | "reddit"
  | "bluesky"
  | "mastodon"
  | "github";

type AdapterEnabledPredicate = (config: CollectorAdapterFactoryConfig) => boolean;
type AdapterBuilder = (ctx: AdapterFactoryContext) => SourceAdapter;

interface AdapterFactory {
  readonly name: AdapterName;
  readonly isEnabled: AdapterEnabledPredicate;
  create(ctx: AdapterFactoryContext): SourceAdapter | null;
}

function createImplementedAdapterFactory(
  name: AdapterName,
  isEnabled: AdapterEnabledPredicate,
  create: AdapterBuilder
): AdapterFactory {
  return {
    name,
    isEnabled,
    create,
  };
}

function createUnsupportedAdapterFactory(
  name: AdapterName,
  isEnabled: AdapterEnabledPredicate
): AdapterFactory {
  return {
    name,
    isEnabled,
    create: () => null,
  };
}

const ADAPTER_FACTORIES: ReadonlyArray<AdapterFactory> = [
  createImplementedAdapterFactory(
    "rss",
    (config) => config.RSS_ENABLED,
    ({ config, checkpointStore, logger, contentFetcherConfig }) =>
      createRSSAdapter(
        config.FEEDS_CONFIG_PATH,
        config.RSS_POLL_INTERVAL_SECONDS * 1000,
        checkpointStore,
        logger.child({ adapter: "rss" }),
        contentFetcherConfig
      )
  ),
  createImplementedAdapterFactory(
    "hackernews",
    (config) => config.HN_ENABLED,
    ({ config, checkpointStore, logger, contentFetcherConfig }) =>
      createHackerNewsAdapter(
        config.HN_MODE,
        config.HN_POLL_INTERVAL_SECONDS * 1000,
        config.HN_MAX_ITEMS_PER_POLL,
        checkpointStore,
        logger.child({ adapter: "hackernews" }),
        contentFetcherConfig
      )
  ),
  createImplementedAdapterFactory(
    "lobsters",
    (config) => config.LOBSTERS_ENABLED,
    ({ config, checkpointStore, logger, contentFetcherConfig }) =>
      createLobstersAdapter(
        config.LOBSTERS_POLL_INTERVAL_SECONDS * 1000,
        config.LOBSTERS_MAX_ITEMS_PER_POLL,
        checkpointStore,
        logger.child({ adapter: "lobsters" }),
        contentFetcherConfig
      )
  ),
  createUnsupportedAdapterFactory("reddit", (config) => config.REDDIT_ENABLED),
  createUnsupportedAdapterFactory("bluesky", (config) => config.BLUESKY_ENABLED),
  createUnsupportedAdapterFactory("mastodon", (config) => config.MASTODON_ENABLED),
  createUnsupportedAdapterFactory("github", (config) => config.GITHUB_ENABLED),
];

export function buildCollectorAdapters(
  input: BuildCollectorAdaptersInput
): CollectorAdapterBuildResult {
  const adapters: SourceAdapter[] = [];
  const unsupportedEnabledAdapters: string[] = [];

  for (const factory of ADAPTER_FACTORIES) {
    if (!factory.isEnabled(input.config)) {
      continue;
    }

    const adapter = factory.create(input);
    if (!adapter) {
      unsupportedEnabledAdapters.push(factory.name);
      continue;
    }

    adapters.push(adapter);
  }

  return {
    adapters,
    unsupportedEnabledAdapters,
  };
}
