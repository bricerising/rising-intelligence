import type { Logger } from "pino";
import type { Config } from "../config.js";
import type { CheckpointStore } from "../checkpoint.js";
import type { ContentFetcherConfig } from "../content-fetcher.js";
import type { SourceAdapter } from "../types.js";
import { createHackerNewsAdapter } from "./hackernews.js";
import { createLobstersAdapter } from "./lobsters.js";
import { createRSSAdapter } from "./rss.js";

const COLLECTOR_ADAPTER_FACTORY_CONFIG_KEYS = [
  "RSS_ENABLED",
  "RSS_POLL_INTERVAL_SECONDS",
  "FEEDS_CONFIG_PATH",
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
}

type ImplementedAdapterName = "rss" | "hackernews" | "lobsters";
type UnsupportedAdapterName = "reddit" | "bluesky" | "mastodon" | "github";
type AdapterName = ImplementedAdapterName | UnsupportedAdapterName;

export interface CollectorAdapterBuildResult {
  adapters: SourceAdapter[];
  unsupportedEnabledAdapters: UnsupportedAdapterName[];
}

interface AdapterFactoryContext extends BuildCollectorAdaptersInput {}

type AdapterEnabledPredicate = (config: CollectorAdapterFactoryConfig) => boolean;

interface AdapterConstructors {
  createRSSAdapter: typeof createRSSAdapter;
  createHackerNewsAdapter: typeof createHackerNewsAdapter;
  createLobstersAdapter: typeof createLobstersAdapter;
}

type AdapterConstructorOverrides = Partial<AdapterConstructors>;

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
  create(
    ctx: AdapterFactoryContext,
    constructors: AdapterConstructors
  ): SourceAdapter;
}

interface UnsupportedAdapterDefinition
  extends AdapterDefinition<UnsupportedAdapterName> {
  readonly kind: "unsupported";
}

type AdapterDefinitionItem =
  | ImplementedAdapterDefinition
  | UnsupportedAdapterDefinition;

function createImplementedAdapterDefinition(
  name: ImplementedAdapterName,
  isEnabled: AdapterEnabledPredicate,
  create: ImplementedAdapterDefinition["create"]
): ImplementedAdapterDefinition {
  return {
    kind: "implemented",
    name,
    isEnabled,
    create,
  };
}

function createUnsupportedAdapterDefinition(
  name: UnsupportedAdapterName,
  isEnabled: AdapterEnabledPredicate
): UnsupportedAdapterDefinition {
  return {
    kind: "unsupported",
    name,
    isEnabled,
  };
}

function createAdapterLogger(logger: Logger, adapter: AdapterName): Logger {
  return logger.child({ adapter });
}

function createAdapterDefinitions(): ReadonlyArray<AdapterDefinitionItem> {
  return [
    createImplementedAdapterDefinition(
      "rss",
      (config) => config.RSS_ENABLED,
      ({ config, checkpointStore, logger, contentFetcherConfig }, constructors) =>
        constructors.createRSSAdapter(
          config.FEEDS_CONFIG_PATH,
          config.RSS_POLL_INTERVAL_SECONDS * 1000,
          checkpointStore,
          createAdapterLogger(logger, "rss"),
          contentFetcherConfig
        )
    ),
    createImplementedAdapterDefinition(
      "hackernews",
      (config) => config.HN_ENABLED,
      ({ config, checkpointStore, logger, contentFetcherConfig }, constructors) =>
        constructors.createHackerNewsAdapter(
          config.HN_MODE,
          config.HN_POLL_INTERVAL_SECONDS * 1000,
          config.HN_MAX_ITEMS_PER_POLL,
          checkpointStore,
          createAdapterLogger(logger, "hackernews"),
          contentFetcherConfig
        )
    ),
    createImplementedAdapterDefinition(
      "lobsters",
      (config) => config.LOBSTERS_ENABLED,
      ({ config, checkpointStore, logger, contentFetcherConfig }, constructors) =>
        constructors.createLobstersAdapter(
          config.LOBSTERS_POLL_INTERVAL_SECONDS * 1000,
          config.LOBSTERS_MAX_ITEMS_PER_POLL,
          checkpointStore,
          createAdapterLogger(logger, "lobsters"),
          contentFetcherConfig
        )
    ),
    createUnsupportedAdapterDefinition("reddit", (config) => config.REDDIT_ENABLED),
    createUnsupportedAdapterDefinition("bluesky", (config) => config.BLUESKY_ENABLED),
    createUnsupportedAdapterDefinition("mastodon", (config) => config.MASTODON_ENABLED),
    createUnsupportedAdapterDefinition("github", (config) => config.GITHUB_ENABLED),
  ];
}

export class CollectorAdapterFactory {
  private readonly constructors: AdapterConstructors;
  private readonly definitions: ReadonlyArray<AdapterDefinitionItem>;

  constructor(constructors: AdapterConstructorOverrides = {}) {
    this.constructors = { ...DEFAULT_ADAPTER_CONSTRUCTORS, ...constructors };
    this.definitions = createAdapterDefinitions();
  }

  build(input: BuildCollectorAdaptersInput): CollectorAdapterBuildResult {
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

const DEFAULT_COLLECTOR_ADAPTER_FACTORY = new CollectorAdapterFactory();

export function createCollectorAdapterFactory(
  constructors: AdapterConstructorOverrides = {}
): CollectorAdapterFactory {
  return new CollectorAdapterFactory(constructors);
}

export function buildCollectorAdapters(
  input: BuildCollectorAdaptersInput
): CollectorAdapterBuildResult {
  return DEFAULT_COLLECTOR_ADAPTER_FACTORY.build(input);
}
