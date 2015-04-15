package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;

public interface ThirdEyeFunction {
  MetricTimeSeries apply(StarTreeConfig config, MetricTimeSeries timeSeries);
}
