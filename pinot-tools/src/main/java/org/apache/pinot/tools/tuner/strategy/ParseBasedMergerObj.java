package org.apache.pinot.tools.tuner.strategy;

import java.math.BigInteger;


public class ParseBasedMergerObj extends MergerObj {
  private long _pureScore;
  private BigInteger _weigtedScore;

  public long getPureScore() {
    return _pureScore;
  }

  public BigInteger getWeigtedScore() {
    return _weigtedScore;
  }

  public ParseBasedMergerObj() {
    _pureScore = 0;
    _weigtedScore = BigInteger.ZERO;
  }

  public void merge(int _pureScore, BigInteger _weigtedScore) {
    this._pureScore += _pureScore;
    this._weigtedScore = this._weigtedScore.add(_weigtedScore);
  }

  public void merge(ParseBasedMergerObj pb) {
    this._pureScore += pb._pureScore;
    this._weigtedScore = this._weigtedScore.add(pb._weigtedScore);
  }

  @Override
  public String toString() {
    return "ParseBasedMergerObj{" + "_pureScore=" + _pureScore + ", _weigtedScore=" + _weigtedScore + '}';
  }
}
