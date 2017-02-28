package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;


public class AlwaysAnomalyDummy extends AnomalyFunctionEx {
  @Override
  public AnomalyFunctionExResult apply() throws Exception {
    AnomalyFunctionExResult result = new AnomalyFunctionExResult();

    long start = getContext().getMonitoringWindowStart();
    long end = getContext().getMonitoringWindowEnd();
    long interval = Long.parseLong(getConfig("interval", "3600000"));

    result.setContext(getContext());
    for(long t=start; t<end; t+=interval) {
      long effectiveEnd = Math.min(t + interval, end);
      result.addAnomaly(t, effectiveEnd, "always trigger anomaly");
    }

    return result;
  }
}
