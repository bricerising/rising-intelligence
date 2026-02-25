export class CircuitBreaker {
  private consecutiveFailures = 0;
  private openUntil = 0;

  constructor(
    private readonly failureThreshold: number,
    private readonly openMs: number
  ) {}

  isOpen(now = Date.now()): boolean {
    return now < this.openUntil;
  }

  timeUntilClose(now = Date.now()): number {
    return Math.max(0, this.openUntil - now);
  }

  recordSuccess(): void {
    this.consecutiveFailures = 0;
    this.openUntil = 0;
  }

  recordFailure(now = Date.now()): boolean {
    this.consecutiveFailures += 1;

    if (this.consecutiveFailures >= this.failureThreshold) {
      this.openUntil = now + this.openMs;
      this.consecutiveFailures = 0;
      return true;
    }

    return false;
  }
}
