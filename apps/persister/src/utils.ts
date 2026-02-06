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
