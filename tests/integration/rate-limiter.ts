/**
 * Rate limiter to handle backend constraints of 10 queries per minute
 */
export class RateLimiter {
  private lastExecutionTimes: number[] = [];
  private readonly maxRequestsPerMinute: number;
  private readonly windowMs: number;

  constructor(maxRequestsPerMinute: number = 10) {
    this.maxRequestsPerMinute = maxRequestsPerMinute;
    this.windowMs = 60 * 1000; // 1 minute in milliseconds
  }

  /**
   * Wait until it's safe to make another request
   * @returns Promise that resolves when it's safe to proceed
   */
  async waitForNextSlot(): Promise<void> {
    const now = Date.now();
    
    // Remove requests older than 1 minute
    this.lastExecutionTimes = this.lastExecutionTimes.filter(
      time => now - time < this.windowMs
    );

    // If we're at the limit, wait until the oldest request is outside the window
    if (this.lastExecutionTimes.length >= this.maxRequestsPerMinute) {
      const oldestRequest = Math.min(...this.lastExecutionTimes);
      const waitTime = this.windowMs - (now - oldestRequest) + 100; // Add 100ms buffer
      
      if (waitTime > 0) {
        console.log(`Rate limit reached. Waiting ${Math.ceil(waitTime / 1000)}s before next request...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
    }

    // Record this execution time
    this.lastExecutionTimes.push(Date.now());
  }

  /**
   * Get current request count in the current window
   */
  getCurrentRequestCount(): number {
    const now = Date.now();
    return this.lastExecutionTimes.filter(time => now - time < this.windowMs).length;
  }

  /**
   * Reset the rate limiter (useful for testing)
   */
  reset(): void {
    this.lastExecutionTimes = [];
  }
}

// Global rate limiter instance
export const globalRateLimiter = new RateLimiter(10);