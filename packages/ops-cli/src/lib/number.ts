const INTEGER_PATTERN = /^-?\d+$/;

function parseIntegerStrict(rawValue: string, flagName: string): number {
  const normalized = rawValue.trim();
  if (!INTEGER_PATTERN.test(normalized)) {
    throw new Error(`Invalid integer for ${flagName}: ${rawValue}`);
  }

  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid integer for ${flagName}: ${rawValue}`);
  }

  return parsed;
}

export function parseNonNegativeIntegerStrict(rawValue: string, flagName: string): number {
  const parsed = parseIntegerStrict(rawValue, flagName);
  if (parsed < 0) {
    throw new Error(`Expected ${flagName} to be non-negative, received: ${parsed}`);
  }

  return parsed;
}

export function parsePositiveIntegerStrict(rawValue: string, flagName: string): number {
  const parsed = parseIntegerStrict(rawValue, flagName);
  if (parsed <= 0) {
    throw new Error(`Expected ${flagName} to be a positive integer, received: ${parsed}`);
  }

  return parsed;
}
