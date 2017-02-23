package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;


public class ThirdEyeMockDataSource implements AnomalyFunctionExDataSource<Object, DataFrame> {

  @Override
  public DataFrame query(Object not_needed, AnomalyFunctionExContext context) {
    DataFrame df = new DataFrame(5);
    df.addSeries("long", 3, 4, 5, 6, 7);
    df.addSeries("double", 1.2, 3.5, 2.8, 6.4, 4.9);
    df.addSeries("stable", 1, 1, 1, 1, 1);
    df.addSeries("string", "aaa", "abb", "bcb", "caa", "ccb");
    return df;
  }
}
