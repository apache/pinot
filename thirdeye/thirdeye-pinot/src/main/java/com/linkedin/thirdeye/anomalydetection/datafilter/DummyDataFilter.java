package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public class DummyDataFilter extends BaseDataFilter {
  @Override
  public boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap) {
    return true;
  }
}
