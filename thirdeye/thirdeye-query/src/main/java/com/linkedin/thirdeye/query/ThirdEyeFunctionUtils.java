package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;

import java.util.Arrays;
import java.util.List;

public class ThirdEyeFunctionUtils {
  public static MetricTimeSeries copyBlankSeriesDouble(List<String> metrics, MetricSchema metricSchema) {
    MetricType[] metricTypes = new MetricType[metrics.size()];
    Arrays.fill(metricTypes, MetricType.DOUBLE);
    return new MetricTimeSeries(new MetricSchema(metrics, Arrays.asList(metricTypes)));
  }

  public static MetricTimeSeries copyBlankSeriesSame(List<String> metrics, MetricSchema metricSchema) {
    MetricType[] metricTypes = new MetricType[metrics.size()];
    for (int i = 0; i < metrics.size(); i++) {
      metricTypes[i] = metricSchema.getMetricType(metrics.get(i));
    }
    return new MetricTimeSeries(new MetricSchema(metrics, Arrays.asList(metricTypes)));
  }
}
