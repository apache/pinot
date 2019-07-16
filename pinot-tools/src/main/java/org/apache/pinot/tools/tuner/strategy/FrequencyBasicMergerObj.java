package org.apache.pinot.tools.tuner.strategy;

public class FrequencyBasicMergerObj extends BasicMergerObj {

  public FrequencyBasicMergerObj() {
    _pureScore = 0;
  }

  private long _pureScore;

  public long getPureScore() {
      return _pureScore;
  }

  public void merge(int _pureScore) {
    this._pureScore += _pureScore;
  }

  public void merge(FrequencyBasicMergerObj fobj) {
    this._pureScore += fobj._pureScore;
  }

  @Override
  public String toString() {
    return "ParseBasedBasicMergerObj{" + "_pureScore=" + _pureScore + '}';
  }
}
