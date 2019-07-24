package org.apache.pinot.tools.tuner.strategy;

/**
 * Accumulator for column stats
 */
public abstract class AbstractMergerObj {
  public abstract String toString();

  /**
   * get the default counter for BasicMergerObjs merged to this AbstractMergerObj
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
   * @param abstractMergerObj AbstractMergerObj to merge to this AbstractMergerObj
   */
  public void mergeCount(AbstractMergerObj abstractMergerObj) {
    this._count += abstractMergerObj._count;
  }
}
