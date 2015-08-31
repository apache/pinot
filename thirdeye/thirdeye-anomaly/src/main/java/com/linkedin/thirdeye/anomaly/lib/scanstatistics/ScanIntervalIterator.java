package com.linkedin.thirdeye.anomaly.lib.scanstatistics;

import com.google.common.collect.Range;


/**
 * A O(1) space iterator for ranges.
 */
public class ScanIntervalIterator {

  private final int max;
  private final int min;

  private final int minWidth;
  private final int maxWidth;
  private final int increment;

  // use of longs eliminates possibility of overflow
  private long lowerHead;
  private long upperHead;

  /**
   * @param min
   * @param max
   * @param minWidth
   *  The smallest interval to consider (inclusive)
   * @param maxWidth
   *  The largest interval to consider (inclusive)
   */
  public ScanIntervalIterator(int min, int max, int minWidth, int maxWidth, int increment) {
    this.min = min;
    this.max = max;

    if (min >= max) {
      throw new IllegalStateException("min must be less than max");
    }

    this.minWidth = minWidth;
    if (minWidth <= 0) {
      throw new IllegalStateException("min width must be positive");
    }

    this.maxWidth = maxWidth;
    if (maxWidth < minWidth) {
      throw new IllegalStateException("max width must be greater than min width");
    }

    this.increment = increment;
    if (increment < 1) {
      throw new IllegalStateException("increment must be greater than min width");
    }

    reset();
  }

  /**
   * Start over at the very beginning
   */
  public void reset() {
    lowerHead = min;
    upperHead = min + minWidth;
  }

  /**
   * @return
   *  Whether there is another interval.
   */
  public boolean hasNext() {
    return upperHead <= max && lowerHead <= max - minWidth;
  }

  /**
   * Calls to next() should be preceded by hasNext(). Otherwise, behavior is undefined.
   *
   * @return
   *  The next interval.
   */
  public Range<Integer> next() {
    Range<Integer> interval = Range.closedOpen((int)lowerHead, (int)upperHead);
    update();
    return interval;
  }

  private void update() {
    lowerHead += increment;
    if (lowerHead >= upperHead - minWidth + 1) {
      upperHead++;
      lowerHead = Math.max(upperHead - maxWidth, min);
    }
  }

}
