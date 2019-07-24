package org.apache.pinot.tools.tuner.strategy;

import java.math.BigInteger;


public class ParseBasedAccumulator extends AbstractAccumulator {
  private long _pureScore;
  private BigInteger _weightedScore;

  public long getPureScore() {
    return _pureScore;
  }

  public BigInteger getWeightedScore() {
    return _weightedScore;
  }

  public ParseBasedAccumulator() {
    _pureScore = 0;
    _weightedScore = BigInteger.ZERO;
  }

  public void merge(int _pureScore, BigInteger _weightedScore) {
    super.increaseCount();
    this._pureScore += _pureScore;
    this._weightedScore = this._weightedScore.add(_weightedScore);
  }

  public void merge(ParseBasedAccumulator pb) {
    super.mergeCount(pb);
    this._pureScore += pb._pureScore;
    this._weightedScore = this._weightedScore.add(pb._weightedScore);
  }

  @Override
  public String toString() {
    return "ParseBasedMergerObj{" + "_pureScore=" + _pureScore + ", _weightedScore=" + _weightedScore + '}';
  }
}
