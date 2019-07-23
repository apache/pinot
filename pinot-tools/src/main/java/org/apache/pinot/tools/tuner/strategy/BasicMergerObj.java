package org.apache.pinot.tools.tuner.strategy;

/*
 * Accumulator for column stats
 */
public abstract class BasicMergerObj {
  public abstract String toString();

  public long getCount() {
    return _count;
  }

  private long _count = 0;

  public void addCount() {
    this._count += 1;
  }

  public void mergeCount(BasicMergerObj basicMergerObj) {
    this._count += basicMergerObj._count;
  }
}
