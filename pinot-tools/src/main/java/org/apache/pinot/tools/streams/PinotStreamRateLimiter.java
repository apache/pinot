package org.apache.pinot.tools.streams;

/**
 * Represents a very simple rate limiter that is used by Pinot
 */
@FunctionalInterface
public interface PinotStreamRateLimiter {
  /**
   * Blocks current thread until X permits are available
   * @param permits how many permits we wish to acquire
   */
  void acquire(int permits);
}
