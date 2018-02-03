package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;


public enum BaselineType {
  SUM(DoubleSeries.SUM),
  PRODUCT(DoubleSeries.PRODUCT),
  MEAN(DoubleSeries.MEAN),
  MEDIAN(DoubleSeries.MEDIAN),
  MIN(DoubleSeries.MIN),
  MAX(DoubleSeries.MAX),
  STD(DoubleSeries.STD);

  final Series.DoubleFunction function;

  BaselineType(Series.DoubleFunction function) {
    this.function = function;
  }

  public Series.DoubleFunction getFunction() {
    return function;
  }
}

