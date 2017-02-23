package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;


public class AlwaysAnomaly extends AnomalyFunctionEx{
  @Override
  public AnomalyFunctionExResult apply() throws Exception {
    AnomalyFunctionExResult result = new AnomalyFunctionExResult();

    long start = getContext().getMonitoringWindowStart();
    long end = getContext().getMonitoringWindowEnd();

    result.setContext(getContext());
    result.addAnomaly(start, end, "always trigger anomaly");

    return result;
  }
}
