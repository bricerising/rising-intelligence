const MAX_GLOB_LENGTH = 128;
const TOPIC_GLOB_PATTERN = /^[A-Za-z0-9.*?_-]+$/;

function escapeRegexCharacter(char: string): string {
  return char.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
}

export function validateTopicGlob(glob: string): string {
  const normalized = glob.trim();
  if (normalized.length === 0) {
    throw new Error("Topic glob must not be empty");
  }
  if (normalized.length > MAX_GLOB_LENGTH) {
    throw new Error(`Topic glob exceeds max length (${MAX_GLOB_LENGTH})`);
  }
  if (!TOPIC_GLOB_PATTERN.test(normalized)) {
    throw new Error(`Unsupported topic glob pattern: ${glob}`);
  }
  return normalized;
}

export function normalizeTopicGlobs(globs: string[] | undefined): string[] | undefined {
  if (!globs || globs.length === 0) {
    return undefined;
  }

  const deduped: string[] = [];
  const seen = new Set<string>();
  for (const glob of globs) {
    const normalized = validateTopicGlob(glob);
    if (!seen.has(normalized)) {
      seen.add(normalized);
      deduped.push(normalized);
    }
  }
  return deduped;
}

export function compileTopicGlob(glob: string): RegExp {
  const normalized = validateTopicGlob(glob);
  let pattern = "^";
  for (const char of normalized) {
    if (char === "*") {
      pattern += ".*";
      continue;
    }
    if (char === "?") {
      pattern += ".";
      continue;
    }
    pattern += escapeRegexCharacter(char);
  }
  pattern += "$";
  return new RegExp(pattern, "i");
}

export function compileTopicGlobMatchers(globs: string[] | undefined): RegExp[] {
  const normalized = normalizeTopicGlobs(globs);
  const resolvedGlobs = normalized && normalized.length > 0 ? normalized : ["*"];
  return resolvedGlobs.map((glob) => compileTopicGlob(glob));
}

export function matchesAnyTopicGlob(topic: string, matchers: readonly RegExp[]): boolean {
  return matchers.some((matcher) => matcher.test(topic));
}
