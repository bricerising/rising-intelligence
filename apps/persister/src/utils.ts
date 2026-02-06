export function nowSeconds(startMs: number): number {
  return (Date.now() - startMs) / 1000;
}

export function toBigInt(value: string, fallback: bigint): bigint {
  try {
    return BigInt(value);
  } catch {
    return fallback;
  }
}

export function serializeError(error: unknown): { message: string } {
  if (error instanceof Error) {
    return { message: error.message };
  }

  return { message: String(error) };
}
