package com.linkedin.thirdeye.anomaly.lib.scanstatistics;

import com.google.common.collect.Range;

public class MaxInterval {

  private final double _maxLikelihood;

  private final Range<Integer> _interval;

  public MaxInterval(double maxLikelihood, Range<Integer> interval) {
    _maxLikelihood = maxLikelihood;
    _interval = interval;
  }

  public Range<Integer> getInterval() {
    return _interval;
  }

  public double getMaxLikelihood() {
    return _maxLikelihood;
  }

}


