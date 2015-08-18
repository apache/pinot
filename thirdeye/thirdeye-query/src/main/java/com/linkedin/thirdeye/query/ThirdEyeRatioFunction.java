package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Replace numerator / denominator in series with RATIO(numerator, denominator)
 */
public class ThirdEyeRatioFunction implements ThirdEyeFunction {
  private final String numerator;
  private final String denominator;

  public ThirdEyeRatioFunction(List<String> functionMetrics) {
    if (functionMetrics.size() != 2) {
      throw new IllegalArgumentException("Must provide numerator and denominator function names");
    }
    this.numerator = functionMetrics.get(0);
    this.denominator = functionMetrics.get(1);
  }

  @Override
  public String toString() {
    return String.format("RATIO(%s,%s)", numerator, denominator);
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, ThirdEyeQuery query, MetricTimeSeries timeSeries) {
    List<String> metricNames = new ArrayList<>(timeSeries.getSchema().getNames());
    List<MetricType> metricTypes = new ArrayList<>(timeSeries.getSchema().getTypes());

    // The ratio
    String ratioName = toString();
    metricNames.add(ratioName);
    metricTypes.add(MetricType.DOUBLE);

    // New schema
    MetricSchema newSchema = new MetricSchema(metricNames, metricTypes);
    MetricTimeSeries newTimeSeries = new MetricTimeSeries(newSchema);

    // Compute
    for (Long time : timeSeries.getTimeWindowSet()) {
      // Copy all non-involved metrics
      MetricType denominatorType = null;
      for (int i = 0; i < timeSeries.getSchema().getNumMetrics(); i++) {
        String name = timeSeries.getSchema().getMetricName(i);
        newTimeSeries.increment(time, name, timeSeries.get(time, name));
        if (name.equals(denominator)) {
          denominatorType = timeSeries.getSchema().getMetricType(i);
        }
      }

      // Compute ratio
      Number n = timeSeries.get(time, numerator);
      Number d = timeSeries.get(time, denominator);
      if (!NumberUtils.isZero(d, denominatorType)) {
        double r = n.doubleValue() / d.doubleValue();
        newTimeSeries.increment(time, ratioName, r);
      }
    }

    return newTimeSeries;
  }
}
