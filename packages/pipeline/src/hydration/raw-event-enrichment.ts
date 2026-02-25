const STALE_EVENT_THRESHOLD_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_INFERRED_TOPICS = 5;
const TRACKING_QUERY_PARAM_KEYS = new Set([
  "fbclid",
  "gclid",
  "dclid",
  "msclkid",
  "igshid",
  "mc_cid",
  "mc_eid",
  "ref_src",
]);

const ENGLISH_STOPWORDS = [
  "the",
  "and",
  "for",
  "with",
  "from",
  "that",
  "this",
  "are",
  "was",
  "were",
  "have",
  "has",
  "will",
  "into",
  "about",
  "over",
  "more",
  "new",
  "than",
];

const TOPIC_INFERENCE_RULES: ReadonlyArray<{
  key: string;
  patterns: ReadonlyArray<RegExp>;
}> = [
  { key: "ai.openai", patterns: [/\bopenai\b/i, /\bchatgpt\b/i, /\bgpt-?\d/i] },
  { key: "ai.anthropic", patterns: [/\banthropic\b/i, /\bclaude\b/i] },
  { key: "ai.google", patterns: [/\bgemini\b/i, /\bgoogle ai\b/i, /\bdeepmind\b/i, /\bvertex ai\b/i] },
  { key: "ai.agents", patterns: [/\bagentic\b/i, /\bai agents?\b/i, /\bagents?\b/i] },
  { key: "ai.llm", patterns: [/\bllms?\b/i, /large language model/i] },
  { key: "aws.bedrock", patterns: [/\bbedrock\b/i] },
  { key: "aws.s3", patterns: [/\bs3\b/i] },
  { key: "aws.ec2", patterns: [/\bec2\b/i] },
  { key: "aws.lambda", patterns: [/\blambda\b/i] },
  { key: "aws.eks", patterns: [/\beks\b/i, /\belastic kubernetes\b/i] },
  { key: "aws.sagemaker", patterns: [/\bsagemaker\b/i] },
  { key: "aws.general", patterns: [/\baws\b/i, /amazon web services/i] },
  { key: "dev.github", patterns: [/\bgithub\b/i, /github\.com/i] },
  { key: "framework.react", patterns: [/\breact\b/i, /\bnext\.js\b/i] },
  { key: "lang.python", patterns: [/\bpython\b/i, /\bpandas\b/i, /\bnumpy\b/i] },
  { key: "lang.rust", patterns: [/\brust\b/i, /\bcargo\b/i, /\bcrates?\b/i] },
  { key: "data.kafka", patterns: [/\bkafka\b/i, /\bredpanda\b/i] },
  { key: "cloud.azure", patterns: [/\bazure\b/i, /\bmicrosoft foundry\b/i] },
  {
    key: "security.general",
    patterns: [/\bcve-\d{4}-\d+\b/i, /\bvulnerability\b/i, /\bzero[- ]day\b/i, /\bexploit\b/i, /\brce\b/i],
  },
];

type RiQualityMetadata = {
  schema_version: 1;
  stale_event: boolean;
  stale_age_hours?: number;
  published_in_future: boolean;
  invalid_url: boolean;
  normalized_url: boolean;
  original_url?: string;
  url_issue_codes?: string[];
  low_information_text: boolean;
  text_issue_codes?: string[];
  inferred_topics: boolean;
  inferred_topic_count: number;
  inferred_lang: boolean;
  lang_inference_method?: "source_meta" | "heuristic_en";
};

type UrlNormalizationResult = {
  normalizedUrl: string | null;
  normalized: boolean;
  invalid: boolean;
  originalUrl?: string;
  issueCodes: string[];
};

export interface RawEventPersistenceInput<TSource = string> {
  eventId: string;
  source: TSource;
  fetchedAt: Date;
  publishedAt: Date | null;
  url: string | null;
  title: string | null;
  text: string;
  authorId: string | null;
  authorHandle: string | null;
  authorDisplayName: string | null;
  engagementScore: number | null;
  engagementComments: number | null;
  engagementLikes: number | null;
  engagementShares: number | null;
  lang: string | null;
  tags: string[];
  extractedHashtags: string[];
  extractedUrls: string[];
  sourceMeta: Record<string, unknown> | null;
}

export type RawEventPersistencePrepared<TSource = string> = RawEventPersistenceInput<TSource> & {
  topics: string[];
};

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function withoutUndefined<T extends Record<string, unknown>>(value: T): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, fieldValue] of Object.entries(value)) {
    if (fieldValue !== undefined) {
      output[key] = fieldValue;
    }
  }

  return output;
}

function normalizeTagList(tags: string[]): string[] {
  const seen = new Set<string>();
  for (const tag of tags) {
    const normalized = tag.trim();
    if (normalized.length > 0) {
      seen.add(normalized);
    }
  }

  return [...seen];
}

function isTrackingParamKey(key: string): boolean {
  const normalized = key.toLowerCase();
  return normalized.startsWith("utm_") || TRACKING_QUERY_PARAM_KEYS.has(normalized);
}

function repairKnownUrlTypos(raw: string): string {
  return raw.replace(/(\.com)(about-[a-z0-9-]+\/)/gi, "$1/$2");
}

function normalizeUrl(rawUrl: string | null): UrlNormalizationResult {
  if (!rawUrl) {
    return {
      normalizedUrl: null,
      normalized: false,
      invalid: false,
      issueCodes: [],
    };
  }

  const issueCodes: string[] = [];
  const trimmed = rawUrl.trim();
  if (trimmed.length === 0) {
    return {
      normalizedUrl: null,
      normalized: false,
      invalid: true,
      originalUrl: rawUrl,
      issueCodes: ["empty_url"],
    };
  }

  const repaired = repairKnownUrlTypos(trimmed);
  if (repaired !== trimmed) {
    issueCodes.push("repaired_known_url_typo");
  }

  let parsed: URL;
  try {
    parsed = new URL(repaired);
  } catch {
    return {
      normalizedUrl: null,
      normalized: false,
      invalid: true,
      originalUrl: rawUrl,
      issueCodes: [...issueCodes, "invalid_url_parse"],
    };
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return {
      normalizedUrl: null,
      normalized: false,
      invalid: true,
      originalUrl: rawUrl,
      issueCodes: [...issueCodes, "unsupported_url_protocol"],
    };
  }

  if (!parsed.hostname) {
    return {
      normalizedUrl: null,
      normalized: false,
      invalid: true,
      originalUrl: rawUrl,
      issueCodes: [...issueCodes, "missing_url_hostname"],
    };
  }

  const originalSerialized = parsed.toString();
  const originalHost = parsed.hostname;
  parsed.hostname = parsed.hostname.toLowerCase();
  if (parsed.hostname.startsWith("www.")) {
    parsed.hostname = parsed.hostname.slice(4);
    issueCodes.push("stripped_www_prefix");
  }

  if (parsed.port === "80" && parsed.protocol === "http:") {
    parsed.port = "";
    issueCodes.push("removed_default_port");
  } else if (parsed.port === "443" && parsed.protocol === "https:") {
    parsed.port = "";
    issueCodes.push("removed_default_port");
  }

  const keys = [...parsed.searchParams.keys()];
  let removedTrackingParam = false;
  for (const key of keys) {
    if (isTrackingParamKey(key)) {
      parsed.searchParams.delete(key);
      removedTrackingParam = true;
    }
  }
  if (removedTrackingParam) {
    issueCodes.push("removed_tracking_query_params");
  }
  parsed.searchParams.sort();

  if (parsed.hash) {
    parsed.hash = "";
    issueCodes.push("removed_fragment");
  }

  if (parsed.pathname !== "/" && parsed.pathname.endsWith("/")) {
    parsed.pathname = parsed.pathname.replace(/\/+$/u, "");
    issueCodes.push("trimmed_trailing_slash");
  }

  if (originalHost !== parsed.hostname) {
    issueCodes.push("normalized_hostname");
  }

  const normalizedUrl = parsed.toString();

  return {
    normalizedUrl,
    normalized: normalizedUrl !== originalSerialized || repaired !== trimmed,
    invalid: false,
    originalUrl: normalizedUrl !== rawUrl ? rawUrl : undefined,
    issueCodes,
  };
}

function normalizeLanguageTag(value: string): string {
  return value.trim().toLowerCase().replaceAll("_", "-");
}

function inferLanguageFromSourceMeta(sourceMeta: Record<string, unknown> | null): string | null {
  if (!sourceMeta) {
    return null;
  }

  const candidates = [sourceMeta.lang, sourceMeta.language, sourceMeta.locale];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return normalizeLanguageTag(candidate);
    }
  }

  return null;
}

function inferEnglishLanguageHeuristic(title: string | null, text: string): string | null {
  const combined = `${title ?? ""} ${text}`.slice(0, 5000).toLowerCase();
  const letterMatches = combined.match(/[a-z]/g);
  const letterCount = letterMatches?.length ?? 0;
  if (letterCount < 20) {
    return null;
  }

  const nonAsciiMatches = combined.match(/[^\x00-\x7F]/g);
  const nonAsciiCount = nonAsciiMatches?.length ?? 0;
  const nonAsciiRatio = combined.length > 0 ? nonAsciiCount / combined.length : 0;

  let stopwordHits = 0;
  for (const stopword of ENGLISH_STOPWORDS) {
    if (combined.includes(` ${stopword} `)) {
      stopwordHits += 1;
    }
    if (stopwordHits >= 2) {
      break;
    }
  }

  if (nonAsciiRatio <= 0.05 && stopwordHits >= 2) {
    return "en";
  }

  return null;
}

function normalizeText(text: string, title: string | null): {
  text: string;
  lowInformation: boolean;
  issueCodes: string[];
} {
  const issueCodes: string[] = [];
  const trimmed = text.trim();
  const commentsOnly = /^comments?$/iu.test(trimmed);
  const urlOnly = /^https?:\/\/\S+$/iu.test(trimmed);
  const veryShort = trimmed.length < 40;

  let normalizedText = text;
  if (commentsOnly && title) {
    normalizedText = title;
    issueCodes.push("comments_placeholder_replaced_with_title");
  } else if (urlOnly && title) {
    normalizedText = `${title}\n${trimmed}`;
    issueCodes.push("url_only_text_padded_with_title");
  } else if (trimmed.length === 0 && title) {
    normalizedText = title;
    issueCodes.push("blank_text_replaced_with_title");
  }

  if (commentsOnly) {
    issueCodes.push("comments_placeholder_text");
  }
  if (urlOnly) {
    issueCodes.push("url_only_text");
  }
  if (veryShort) {
    issueCodes.push("very_short_text");
  }

  return {
    text: normalizedText,
    lowInformation: commentsOnly || urlOnly || veryShort,
    issueCodes,
  };
}

function inferTopics(content: string): string[] {
  const inferred: string[] = [];
  for (const rule of TOPIC_INFERENCE_RULES) {
    if (rule.patterns.some((pattern) => pattern.test(content))) {
      inferred.push(rule.key);
    }

    if (inferred.length >= MAX_INFERRED_TOPICS) {
      break;
    }
  }

  return inferred;
}

function buildMergedSourceMeta(
  sourceMeta: Record<string, unknown> | null,
  quality: RiQualityMetadata
): Record<string, unknown> {
  const base = sourceMeta ? { ...sourceMeta } : {};
  const sanitizedBase = { ...base };
  delete sanitizedBase.ri_quality;

  return {
    ...sanitizedBase,
    ri_quality: withoutUndefined(quality),
  };
}

export function prepareRawEventForPersistence<TSource>(
  event: RawEventPersistenceInput<TSource>
): RawEventPersistencePrepared<TSource> {
  const tags = normalizeTagList(event.tags);
  const normalizedUrl = normalizeUrl(event.url);
  const normalizedText = normalizeText(event.text, event.title);

  const now = event.fetchedAt.getTime();
  const publishedAtMs = event.publishedAt?.getTime() ?? null;
  const publishedInFuture = publishedAtMs !== null && publishedAtMs > now;
  const staleMs = publishedAtMs !== null ? now - publishedAtMs : null;
  const staleEvent = staleMs !== null && staleMs > STALE_EVENT_THRESHOLD_MS;
  const staleAgeHours = staleMs !== null && staleMs > 0 ? Number((staleMs / (60 * 60 * 1000)).toFixed(2)) : undefined;

  let lang = event.lang ? normalizeLanguageTag(event.lang) : null;
  let inferredLang = false;
  let langInferenceMethod: "source_meta" | "heuristic_en" | undefined;
  if (!lang) {
    const sourceMetaLang = inferLanguageFromSourceMeta(event.sourceMeta);
    if (sourceMetaLang) {
      lang = sourceMetaLang;
      inferredLang = true;
      langInferenceMethod = "source_meta";
    } else {
      const heuristicLang = inferEnglishLanguageHeuristic(event.title, normalizedText.text);
      if (heuristicLang) {
        lang = heuristicLang;
        inferredLang = true;
        langInferenceMethod = "heuristic_en";
      }
    }
  }

  let finalTags = tags;
  let inferredTopics = false;
  if (finalTags.length === 0) {
    const sourceMetaTags = Array.isArray(event.sourceMeta?.tags)
      ? event.sourceMeta.tags.filter((value): value is string => typeof value === "string")
      : [];
    const haystack = `${event.title ?? ""}\n${normalizedText.text}\n${normalizedUrl.normalizedUrl ?? ""}\n${sourceMetaTags.join(" ")}`;
    const inferred = inferTopics(haystack);
    if (inferred.length > 0) {
      finalTags = inferred;
      inferredTopics = true;
    }
  }

  const quality: RiQualityMetadata = {
    schema_version: 1,
    stale_event: staleEvent,
    stale_age_hours: staleAgeHours,
    published_in_future: publishedInFuture,
    invalid_url: normalizedUrl.invalid,
    normalized_url: normalizedUrl.normalized,
    original_url: normalizedUrl.originalUrl,
    url_issue_codes: normalizedUrl.issueCodes.length > 0 ? normalizedUrl.issueCodes : undefined,
    low_information_text: normalizedText.lowInformation,
    text_issue_codes: normalizedText.issueCodes.length > 0 ? normalizedText.issueCodes : undefined,
    inferred_topics: inferredTopics,
    inferred_topic_count: inferredTopics ? finalTags.length : 0,
    inferred_lang: inferredLang,
    lang_inference_method: langInferenceMethod,
  };

  return {
    ...event,
    url: normalizedUrl.normalizedUrl,
    text: normalizedText.text,
    lang,
    tags: finalTags,
    topics: finalTags,
    sourceMeta: buildMergedSourceMeta(event.sourceMeta, quality),
  };
}
