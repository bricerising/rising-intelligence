/**
 * Test clock for deterministic time-based testing.
 *
 * Usage:
 *   const clock = new TestClock();
 *   clock.freeze('2026-02-05T10:00:00Z');
 *   // ... run test ...
 *   clock.advance(60 * 60 * 1000); // Advance 1 hour
 *   clock.unfreeze();
 */
export class TestClock {
  private frozen: Date | null = null;

  /**
   * Freeze time at the specified instant.
   */
  freeze(time: Date | string): void {
    this.frozen = typeof time === 'string' ? new Date(time) : time;
  }

  /**
   * Unfreeze time, returning to real clock.
   */
  unfreeze(): void {
    this.frozen = null;
  }

  /**
   * Get current time (frozen or real).
   */
  now(): Date {
    return this.frozen ?? new Date();
  }

  /**
   * Get current time as ISO string.
   */
  nowISO(): string {
    return this.now().toISOString();
  }

  /**
   * Advance frozen time by the specified milliseconds.
   * @throws if clock is not frozen
   */
  advance(ms: number): void {
    if (!this.frozen) {
      throw new Error('Cannot advance: clock is not frozen');
    }
    this.frozen = new Date(this.frozen.getTime() + ms);
  }

  /**
   * Advance frozen time by the specified duration.
   */
  advanceMinutes(minutes: number): void {
    this.advance(minutes * 60 * 1000);
  }

  advanceHours(hours: number): void {
    this.advance(hours * 60 * 60 * 1000);
  }

  advanceDays(days: number): void {
    this.advance(days * 24 * 60 * 60 * 1000);
  }

  /**
   * Check if clock is currently frozen.
   */
  isFrozen(): boolean {
    return this.frozen !== null;
  }
}
