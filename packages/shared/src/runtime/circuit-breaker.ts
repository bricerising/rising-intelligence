export class CircuitBreaker {
  private consecutiveFailures = 0;
  private openUntil = 0;
  private halfOpen = false;

  constructor(
    private readonly failureThreshold: number,
    private readonly openMs: number
  ) {}

  isOpen(now = Date.now()): boolean {
    if (now >= this.openUntil && this.openUntil > 0) {
      this.halfOpen = true;
      return false;
    }
    return now < this.openUntil;
  }

  isHalfOpen(): boolean {
    return this.halfOpen;
  }

  timeUntilClose(now = Date.now()): number {
    return Math.max(0, this.openUntil - now);
  }

  recordSuccess(): void {
    this.consecutiveFailures = 0;
    this.openUntil = 0;
    this.halfOpen = false;
  }

  recordFailure(now = Date.now()): boolean {
    if (this.halfOpen) {
      this.halfOpen = false;
      this.openUntil = now + this.openMs;
      this.consecutiveFailures = 0;
      return true;
    }

    this.consecutiveFailures += 1;

    if (this.consecutiveFailures >= this.failureThreshold) {
      this.openUntil = now + this.openMs;
      this.consecutiveFailures = 0;
      return true;
    }

    return false;
  }
}
