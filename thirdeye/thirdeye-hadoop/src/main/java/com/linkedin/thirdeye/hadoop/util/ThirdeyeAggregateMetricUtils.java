package com.linkedin.thirdeye.hadoop.util;

import java.util.List;

import com.linkedin.thirdeye.api.MetricType;

public class ThirdeyeAggregateMetricUtils {

  public static void aggregate(List<MetricType> metricTypes, Number[] aggMetricValues, Number[] metricValues) {
    int numMetrics = aggMetricValues.length;
    for (int i = 0; i < numMetrics; i++) {
      MetricType metricType = metricTypes.get(i);
      switch (metricType) {
        case SHORT:
          aggMetricValues[i] = aggMetricValues[i].shortValue() + metricValues[i].shortValue();
          break;
        case INT:
          aggMetricValues[i] = aggMetricValues[i].intValue() + metricValues[i].intValue();
          break;
        case FLOAT:
          aggMetricValues[i] = aggMetricValues[i].floatValue() + metricValues[i].floatValue();
          break;
        case DOUBLE:
          aggMetricValues[i] = aggMetricValues[i].doubleValue() + metricValues[i].doubleValue();
          break;
        case LONG:
        default:
          aggMetricValues[i] = aggMetricValues[i].longValue() + metricValues[i].longValue();
          break;
      }
    }
  }

}
