package org.apache.pinot.tools.tuner.strategy;

/**
 *
 */
public class FrequencyMergerObj extends AbstractMergerObj {

  public FrequencyMergerObj() {
    _pureScore = 0;
  }

  private long _pureScore;

  public long getPureScore() {
    return _pureScore;
  }

  public void merge(int pureScore) {
    super.increaseCount();
    this._pureScore += pureScore;
  }

  public void merge(FrequencyMergerObj fobj) {
    super.mergeCount(fobj);
    this._pureScore += fobj._pureScore;
  }

  @Override
  public String toString() {
    return "ParseBasedMergerObj{" + "_pureScore=" + _pureScore + '}';
  }
}
