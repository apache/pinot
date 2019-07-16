package org.apache.pinot.tools.tuner.strategy;

import java.math.BigInteger;


public class ParseBasedBasicMergerObj extends BasicMergerObj {
  private long _pureScore;
  private BigInteger _weigtedScore;

  public long getPureScore() {
    return _pureScore;
  }

  public BigInteger getWeigtedScore() {
    return _weigtedScore;
  }

  public ParseBasedBasicMergerObj() {
    _pureScore = 0;
    _weigtedScore = BigInteger.ZERO;
  }

  public void merge(int _pureScore, BigInteger _weigtedScore) {
    this._pureScore += _pureScore;
    this._weigtedScore = this._weigtedScore.add(_weigtedScore);
  }

  public void merge(ParseBasedBasicMergerObj pb) {
    this._pureScore += pb._pureScore;
    this._weigtedScore = this._weigtedScore.add(pb._weigtedScore);
  }

  @Override
  public String toString() {
    return "ParseBasedBasicMergerObj{" + "_pureScore=" + _pureScore + ", _weigtedScore=" + _weigtedScore + '}';
  }
}
