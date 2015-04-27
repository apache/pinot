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
  public MetricTimeSeries apply(StarTreeConfig config, ThirdEyeQuery query, MetricTimeSeries timeSeries) {
    MetricSchema oldSchema = timeSeries.getSchema();
    List<String> metricNames = new ArrayList<>(oldSchema.getNumMetrics() - 1);
    List<MetricType> metricTypes = new ArrayList<>(oldSchema.getNumMetrics() - 1);

    // Determine to keep numerator or denominator
    Map<String, Integer> nameCounts = new HashMap<>();
    if (query != null) {
      for (String name : query.getMetricNames()) {
        Integer count = nameCounts.get(name);
        if (count == null) {
          count = 0;
        }
        nameCounts.put(name, count + 1);
      }
    }

    boolean keepNumerator = nameCounts.get(numerator) != null && nameCounts.get(numerator) > 1;
    boolean keepDenominator = nameCounts.get(denominator) != null && nameCounts.get(denominator) > 1;

    // Keep all other original metrics
    MetricType denominatorType = null;
    for (int i = 0; i < oldSchema.getNumMetrics(); i++) {
      String name = oldSchema.getMetricName(i);
      if (!name.equals(numerator) && !name.equals(denominator)) {
        metricNames.add(name);
        metricTypes.add(oldSchema.getMetricType(i));
      }

      if (name.equals(denominator)) {
        denominatorType = oldSchema.getMetricType(name);
      }
    }

    // Add numerator / denominator if necessary
    if (keepNumerator) {
      metricNames.add(numerator);
      metricTypes.add(oldSchema.getMetricType(numerator));
    }
    if (keepDenominator) {
      metricNames.add(denominator);
      metricTypes.add(oldSchema.getMetricType(denominator));
    }

    // Add the ratio metric
    String ratioName = String.format("RATIO(%s,%s)", numerator, denominator);
    metricNames.add(ratioName);
    metricTypes.add(MetricType.DOUBLE);

    MetricSchema newSchema = new MetricSchema(metricNames, metricTypes);
    MetricTimeSeries newTimeSeries = new MetricTimeSeries(newSchema);

    // Compute
    for (Long time : timeSeries.getTimeWindowSet()) {
      // Copy all non-involved metrics
      for (int i = 0; i < oldSchema.getNumMetrics(); i++) {
        String name = oldSchema.getMetricName(i);
        if (!name.equals(numerator) && !name.equals(denominator)) {
          newTimeSeries.increment(time, name, timeSeries.get(time, name));
        }
      }

      // Numerator
      if (keepNumerator) {
        newTimeSeries.increment(time, numerator, timeSeries.get(time, numerator));
      }

      // Denominator
      if (keepDenominator) {
        newTimeSeries.increment(time, denominator, timeSeries.get(time, denominator));
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
