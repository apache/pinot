package org.apache.pinot.fsa;

/**
 * Minimum/maximum and range.
 */
final class MinMax {
  public final int min;
  public final int max;

  MinMax(int min, int max) {
    this.min = Math.min(min, max);
    this.max = Math.max(min, max);
  }

  public int range() {
    return max - min;
  }
}