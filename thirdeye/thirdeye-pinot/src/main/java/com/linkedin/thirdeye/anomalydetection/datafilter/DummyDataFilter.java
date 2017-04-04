package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.api.DimensionMap;

public class DummyDataFilter extends BaseDataFilter {
  @Override
  public boolean isQualified(AnomalyDetectionContext context, DimensionMap dimensionMap) {
    return true;
  }
}
