package org.apache.pinot.tools.tuner.strategy;

import java.math.BigInteger;
import javax.annotation.Nonnull;


public class ParseBasedObj extends ColumnStatsObj{
  private long _pureScore;
  private BigInteger _weigtedScore;

  public ParseBasedObj() {
    _pureScore=0;
    _weigtedScore=BigInteger.ZERO;

  }

  public void merge(int _pureScore, BigInteger _weigtedScore){
    this._pureScore+=_pureScore;
    this._weigtedScore.add(_weigtedScore);
  }

  public void merge(ParseBasedObj pb){
    this._pureScore+=pb._pureScore;
    this._weigtedScore.add(pb._weigtedScore);
  }

  @Override
  public String toString() {
    return "ParseBasedObj{" + "_pureScore=" + _pureScore + ", _weigtedScore=" + _weigtedScore + '}';
  }
}
