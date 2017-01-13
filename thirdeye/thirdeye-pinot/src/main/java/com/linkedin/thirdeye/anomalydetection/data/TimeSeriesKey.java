package com.linkedin.thirdeye.anomalydetection.data;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.Objects;
import org.apache.commons.lang.ObjectUtils;

public class TimeSeriesKey {
  String metricName;
  DimensionMap dimensionMap;

  @Override
  public boolean equals(Object o) {
    if (o instanceof TimeSeriesKey) {
      TimeSeriesKey other = (TimeSeriesKey) o;
      return ObjectUtils.equals(this, other);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, dimensionMap);
  }
}
