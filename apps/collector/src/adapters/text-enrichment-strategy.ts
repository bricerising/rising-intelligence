import type { Logger } from "pino";
import {
  fetchArticleContent,
  createContentFetcherConfig,
  type ContentFetcherConfig,
} from "../content-fetcher.js";

export interface TextEnrichmentInput {
  text: string;
  url?: string;
  minLength: number;
  logContext?: Record<string, unknown>;
}

export interface TextEnrichmentStrategy {
  enrich(input: TextEnrichmentInput): Promise<string>;
}

export class NoopTextEnrichmentStrategy implements TextEnrichmentStrategy {
  async enrich(input: TextEnrichmentInput): Promise<string> {
    return input.text;
  }
}

export class ArticleFetchTextEnrichmentStrategy
implements TextEnrichmentStrategy {
  constructor(
    private readonly config: ContentFetcherConfig,
    private readonly logger: Logger,
    private readonly successLogMessage: string
  ) {}

  async enrich(input: TextEnrichmentInput): Promise<string> {
    const { text, url, minLength, logContext } = input;
    if (!url || text.length >= minLength) {
      return text;
    }

    const articleContent = await fetchArticleContent(url, this.config, this.logger);
    if (!articleContent?.success) {
      return text;
    }

    this.logger.debug(
      { ...logContext, url, textLength: articleContent.text.length },
      this.successLogMessage
    );
    return articleContent.text;
  }
}

export function resolveContentFetcherConfig(
  config?: ContentFetcherConfig
): ContentFetcherConfig {
  return config ?? createContentFetcherConfig({});
}

export function createTextEnrichmentStrategy(
  config: ContentFetcherConfig | undefined,
  logger: Logger,
  successLogMessage: string
): TextEnrichmentStrategy {
  const resolvedConfig = resolveContentFetcherConfig(config);
  if (!resolvedConfig.enabled) {
    return new NoopTextEnrichmentStrategy();
  }
  return new ArticleFetchTextEnrichmentStrategy(
    resolvedConfig,
    logger,
    successLogMessage
  );
}
