package com.linkedin.thirdeye.anomalydetection.context;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.Objects;
import org.apache.commons.lang.ObjectUtils;

public class TimeSeriesKey {
  private String metricName = "";
  private DimensionMap dimensionMap = new DimensionMap();

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public DimensionMap getDimensionMap() {
    return dimensionMap;
  }

  public void setDimensionMap(DimensionMap dimensionMap) {
    this.dimensionMap = dimensionMap;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TimeSeriesKey) {
      TimeSeriesKey other = (TimeSeriesKey) o;
      return ObjectUtils.equals(metricName, other.metricName)
          && ObjectUtils.equals(dimensionMap, other.dimensionMap);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, dimensionMap);
  }
}
