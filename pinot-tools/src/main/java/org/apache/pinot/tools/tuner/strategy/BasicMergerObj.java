package org.apache.pinot.tools.tuner.strategy;

/**
 * Accumulator for column stats
 */
public abstract class BasicMergerObj {
  public abstract String toString();

  /**
   * get the default counter for BasicMergerObjs merged to this BasicMergerObj
   * @return
   */
  public long getCount() {
    return _count;
  }

  private long _count = 0;

  /**
   * increase default counter by one
   */
  public void increaseCount() {
    this._count += 1;
  }

  /**
   * merge the default counter of two BasicMergerObjs
   * @param basicMergerObj BasicMergerObj to merge to this BasicMergerObj
   */
  public void mergeCount(BasicMergerObj basicMergerObj) {
    this._count += basicMergerObj._count;
  }
}
