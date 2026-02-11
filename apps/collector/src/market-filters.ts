import { readdirSync, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { parse as parseYaml } from "yaml";
import { z } from "zod";

type CompiledMatcher =
  | {
      type: "keyword";
      raw: string;
      keyword: string;
    }
  | {
      type: "regex";
      raw: string;
      pattern: RegExp;
    };

export interface MarketFilterProfile {
  key: string;
  name: string;
  matchers: CompiledMatcher[];
}

export interface MarketFilterMatch {
  profile: string;
  reasons: string[];
}

export interface MarketFilterEvaluation {
  matches: MarketFilterMatch[];
  marketProfiles: string[];
  matchReasons: string[];
}

const matcherSchema = z.union([
  z.object({
    type: z.literal("keyword"),
    value: z.string().min(1),
  }),
  z.object({
    type: z.literal("regex"),
    pattern: z.string().min(1),
    flags: z.string().optional(),
  }),
]);

const profileSchema = z.object({
  profile: z.string().min(1).optional(),
  name: z.string().min(1).optional(),
  keywords: z.array(z.string().min(1)).optional(),
  matchers: z.array(matcherSchema).optional(),
});

function normalizeProfileKey(name: string): string {
  return name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function sanitizeRegexFlags(flags: string | undefined): string {
  if (!flags || flags.trim() === "") {
    return "i";
  }

  const deduped = [...new Set(flags.split(""))];
  return deduped.filter((flag) => flag !== "g" && flag !== "y").join("");
}

function compileProfile(filePath: string, raw: unknown): MarketFilterProfile {
  const parsed = profileSchema.parse(raw);
  const rawName = parsed.profile ?? parsed.name ?? basename(filePath, extname(filePath));
  const key = normalizeProfileKey(rawName);
  if (!key) {
    throw new Error(`Profile name resolved to empty key for ${filePath}`);
  }

  const matchers: CompiledMatcher[] = [];
  for (const keyword of parsed.keywords ?? []) {
    const trimmed = keyword.trim();
    if (!trimmed) {
      continue;
    }
    matchers.push({
      type: "keyword",
      raw: trimmed,
      keyword: trimmed.toLowerCase(),
    });
  }

  for (const matcher of parsed.matchers ?? []) {
    if (matcher.type === "keyword") {
      matchers.push({
        type: "keyword",
        raw: matcher.value.trim(),
        keyword: matcher.value.trim().toLowerCase(),
      });
      continue;
    }

    try {
      matchers.push({
        type: "regex",
        raw: matcher.pattern,
        pattern: new RegExp(matcher.pattern, sanitizeRegexFlags(matcher.flags)),
      });
    } catch (error) {
      throw new Error(
        `Invalid regex matcher "${matcher.pattern}" in ${filePath}: ${(error as Error).message}`
      );
    }
  }

  if (matchers.length === 0) {
    throw new Error(`Market filter profile ${filePath} must include at least one matcher`);
  }

  return {
    key,
    name: rawName.trim(),
    matchers,
  };
}

function getProfileFiles(dirPath: string): string[] {
  const files = readdirSync(dirPath, { withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .filter((name) => {
      const ext = extname(name).toLowerCase();
      return ext === ".yaml" || ext === ".yml";
    })
    .sort();

  return files.map((name) => join(dirPath, name));
}

export function loadMarketFilterProfiles(dirPath: string): MarketFilterProfile[] {
  const profileFiles = getProfileFiles(dirPath);
  return profileFiles.map((filePath) => {
    let parsed: unknown;
    try {
      parsed = parseYaml(readFileSync(filePath, "utf-8"));
    } catch (error) {
      throw new Error(`Failed to parse market filter profile ${filePath}: ${(error as Error).message}`);
    }

    try {
      return compileProfile(filePath, parsed);
    } catch (error) {
      throw new Error(`Invalid market filter profile ${filePath}: ${(error as Error).message}`);
    }
  });
}

function buildReason(profileKey: string, matcher: CompiledMatcher): string {
  if (matcher.type === "keyword") {
    return `${profileKey}:keyword:${matcher.raw}`;
  }
  return `${profileKey}:regex:${matcher.raw}`;
}

export function evaluateMarketFilters(
  content: string,
  profiles: readonly MarketFilterProfile[]
): MarketFilterEvaluation {
  const normalized = content.toLowerCase();
  const matches: MarketFilterMatch[] = [];

  for (const profile of profiles) {
    const reasons: string[] = [];
    for (const matcher of profile.matchers) {
      const matched =
        matcher.type === "keyword"
          ? normalized.includes(matcher.keyword)
          : matcher.pattern.test(content);
      if (matched) {
        reasons.push(buildReason(profile.key, matcher));
      }
    }

    if (reasons.length > 0) {
      matches.push({
        profile: profile.key,
        reasons,
      });
    }
  }

  return {
    matches,
    marketProfiles: matches.map((match) => match.profile),
    matchReasons: matches.flatMap((match) => match.reasons),
  };
}

export interface EntityTermMatch {
  matched: boolean;
  matchedTerms: string[];
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function matchEntityTerms(content: string, entityTerms: readonly string[]): EntityTermMatch {
  const unique = entityTerms
    .map((term) => term.trim())
    .filter((term) => term.length > 0)
    .filter((term, index, allTerms) => allTerms.indexOf(term) === index);

  const matchedTerms = unique.filter((term) => {
    const pattern = new RegExp(`\\b${escapeRegex(term)}\\b`, "i");
    return pattern.test(content);
  });

  return {
    matched: matchedTerms.length > 0,
    matchedTerms,
  };
}
