package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.util.Map;

public class DummyDataFilter extends BaseDataFilter {
  @Override
  public void setParameters(Map<String, String> props) {
  }

  @Override
  public boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap) {
    return true;
  }

  @Override
  public boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap, long windowStart,
      long windowEnd) {
    return true;
  }
}
