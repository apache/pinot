package org.apache.pinot.tools.tuner.strategy;

public class FrequencyMergerObj extends BasicMergerObj {

  public FrequencyMergerObj() {
    _pureScore = 0;
  }

  private long _pureScore;

  public long getPureScore() {
    return _pureScore;
  }

  public void merge(int _pureScore) {
    super.addCount();
    this._pureScore += _pureScore;
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
